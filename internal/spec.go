package internal

import (
	"context"
	"fmt"
	"strings"

	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
)

type NameWithType struct {
	Name string `json:"name"`
	Type *Type  `json:"type"`
}

type FunctionSpec struct {
	NamePath []string        `json:"name"`
	Language string          `json:"language"`
	Args     []*NameWithType `json:"args"`
	Return   *Type           `json:"return"`
	Body     string          `json:"body"`
	Code     string          `json:"code"`
}

func (s *FunctionSpec) FuncName() string {
	return FormatName(s.NamePath)
}

func (s *FunctionSpec) SQL() string {
	args := []string{}
	for _, arg := range s.Args {
		t, _ := arg.Type.ToZetaSQLType()
		args = append(args, fmt.Sprintf("%s %s", arg.Name, t.Kind()))
	}
	retType, _ := s.Return.ToZetaSQLType()
	return fmt.Sprintf(
		"CREATE FUNCTION `%s`(%s) RETURNS %s AS (%s)",
		s.FuncName(),
		strings.Join(args, ", "),
		retType.Kind(),
		s.Body,
	)
}

type TableSpec struct {
	NamePath   []string       `json:"namePath"`
	Columns    []*ColumnSpec  `json:"columns"`
	CreateMode ast.CreateMode `json:"createMode"`
}

func (s *TableSpec) Column(name string) *ColumnSpec {
	for _, col := range s.Columns {
		if col.Name == name {
			return col
		}
	}
	return nil
}

func (s *TableSpec) TableName() string {
	return FormatName(s.NamePath)
}

func (s *TableSpec) SQLiteSchema() string {
	columns := []string{}
	for _, c := range s.Columns {
		columns = append(columns, c.SQLiteSchema())
	}
	var stmt string
	switch s.CreateMode {
	case ast.CreateDefaultMode:
		stmt = "CREATE TABLE"
	case ast.CreateOrReplaceMode:
		stmt = "CREATE TABLE"
	case ast.CreateIfNotExistsMode:
		stmt = "CREATE TABLE IF NOT EXISTS"
	}
	return fmt.Sprintf("%s `%s` (%s)", stmt, s.TableName(), strings.Join(columns, ","))
}

type ColumnSpec struct {
	Name      string `json:"name"`
	Type      *Type  `json:"type"`
	IsNotNull bool   `json:"isNotNull"`
}

type Type struct {
	Name        string          `json:"name"`
	Kind        int             `json:"kind"`
	ElementType *Type           `json:"elementType"`
	FieldTypes  []*NameWithType `json:"fieldTypes"`
}

func (t *Type) IsArray() bool {
	return t.Kind == types.ARRAY
}

func (t *Type) IsStruct() bool {
	return t.Kind == types.STRUCT
}

func (t *Type) ToZetaSQLType() (types.Type, error) {
	switch types.TypeKind(t.Kind) {
	case types.ARRAY:
		typ, err := t.ElementType.ToZetaSQLType()
		if err != nil {
			return nil, err
		}
		return types.NewArrayType(typ)
	case types.STRUCT:
		var fields []*types.StructField
		for _, field := range t.FieldTypes {
			typ, err := field.Type.ToZetaSQLType()
			if err != nil {
				return nil, err
			}
			fields = append(fields, types.NewStructField(field.Name, typ))
		}
		return types.NewStructType(fields)
	}
	return types.TypeFromKind(types.TypeKind(t.Kind)), nil
}

func (s *ColumnSpec) SQLiteSchema() string {
	var typ string
	switch types.TypeKind(s.Type.Kind) {
	case types.INT32, types.INT64, types.UINT32, types.UINT64:
		typ = "INT"
	case types.ENUM:
		typ = "INT"
	case types.BOOL:
		typ = "BOOLEAN"
	case types.FLOAT:
		typ = "FLOAT"
	case types.BYTES:
		typ = "BLOB"
	case types.DOUBLE:
		typ = "DOUBLE"
	case types.JSON:
		typ = "JSON"
	case types.STRING:
		typ = "TEXT"
	case types.DATE:
		typ = "TEXT"
	case types.TIMESTAMP:
		typ = "TEXT"
	case types.ARRAY:
		typ = "TEXT"
	case types.STRUCT:
		typ = "TEXT"
	case types.PROTO:
		typ = "TEXT"
	case types.TIME:
		typ = "TEXT"
	case types.DATETIME:
		typ = "TEXT"
	case types.GEOGRAPHY:
		typ = "TEXT"
	case types.NUMERIC:
		typ = "TEXT"
	case types.BIG_NUMERIC:
		typ = "TEXT"
	case types.EXTENDED:
		typ = "TEXT"
	case types.INTERVAL:
		typ = "TEXT"
	case types.UNKNOWN:
		fallthrough
	default:
		typ = "UNKNOWN"
	}
	schema := fmt.Sprintf("`%s` %s", s.Name, typ)
	if s.IsNotNull {
		schema += " NOT NULL"
	}
	return schema
}

func newFunctionSpec(ctx context.Context, namePath []string, stmt *ast.CreateFunctionStmtNode) (*FunctionSpec, error) {
	args := []*NameWithType{}
	for _, arg := range stmt.Signature().Arguments() {
		args = append(args, &NameWithType{
			Name: arg.ArgumentName(),
			Type: newType(arg.Type()),
		})
	}
	funcExpr := stmt.FunctionExpression()
	body, err := newNode(funcExpr).FormatSQL(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to format function expression: %w", err)
	}
	return &FunctionSpec{
		NamePath: MergeNamePath(namePath, stmt.NamePath()),
		Args:     args,
		Return:   newType(stmt.ReturnType()),
		Code:     stmt.Code(),
		Body:     body,
		Language: stmt.Language(),
	}, nil
}

func newTableSpec(namePath []string, stmt *ast.CreateTableStmtNode) *TableSpec {
	columns := []*ColumnSpec{}
	for _, columnNode := range stmt.ColumnDefinitionList() {
		annotation := columnNode.Annotations()
		var isNotNull bool
		if annotation != nil {
			params := annotation.TypeParameters()
			if params != nil {
				//TODO: get type param from params
				_ = params
			}
			isNotNull = annotation.NotNull()
		}
		columns = append(columns, &ColumnSpec{
			Name:      columnNode.Name(),
			Type:      newType(columnNode.Type()),
			IsNotNull: isNotNull,
		})
	}
	return &TableSpec{
		NamePath:   MergeNamePath(namePath, stmt.NamePath()),
		Columns:    columns,
		CreateMode: stmt.CreateMode(),
	}
}

func newType(t types.Type) *Type {
	kind := t.Kind()
	var (
		elem       *Type
		fieldTypes []*NameWithType
	)
	switch kind {
	case types.ARRAY:
		elem = newType(t.AsArray().ElementType())
	case types.STRUCT:
		for _, field := range t.AsStruct().Fields() {
			fieldTypes = append(fieldTypes, &NameWithType{
				Name: field.Name(),
				Type: newType(field.Type()),
			})
		}
	}
	return &Type{
		Name:        t.TypeName(types.ProductInternal),
		Kind:        int(kind),
		ElementType: elem,
		FieldTypes:  fieldTypes,
	}
}
