package internal

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/goccy/go-json"
	googlesql "github.com/goccy/go-googlesql"
)

type NameWithType struct {
	Name string `json:"name"`
	Type *Type  `json:"type"`
}

func (t *NameWithType) FunctionArgumentType() (*googlesql.FunctionArgumentType, error) {
	if t.Type.SignatureKind != googlesql.SignatureArgumentKindArgTypeFixed {
		return NewTemplatedFunctionArgumentType(
			t.Type.SignatureKind,
			NewFunctionArgumentTypeOptions(googlesql.FunctionEnums_ArgumentCardinalityRequired),
		), nil
	}
	typ, err := t.Type.ToZetaSQLType()
	if err != nil {
		return nil, err
	}
	opt := NewFunctionArgumentTypeOptions(googlesql.FunctionEnums_ArgumentCardinalityRequired)
	// SetArgumentName isn't exposed on the bridge; argument names flow
	// through the FunctionSignature builder separately in modern API.
	_ = t.Name
	return NewFunctionArgumentType(typ, opt), nil
}

type FunctionSpec struct {
	IsTemp    bool            `json:"isTemp"`
	NamePath  []string        `json:"name"`
	Language  string          `json:"language"`
	Args      []*NameWithType `json:"args"`
	Return    *Type           `json:"return"`
	Body      string          `json:"body"`
	Code      string          `json:"code"`
	UpdatedAt time.Time       `json:"updatedAt"`
	CreatedAt time.Time       `json:"createdAt"`
}

func (s *FunctionSpec) FuncName() string {
	return formatPath(s.NamePath)
}

func (s *FunctionSpec) SQL() string {
	args := []string{}
	for _, arg := range s.Args {
		t, _ := arg.Type.ToZetaSQLType()
		args = append(args, fmt.Sprintf("%s %s", arg.Name, m1(t.Kind())))
	}
	retType, _ := s.Return.ToZetaSQLType()
	return fmt.Sprintf(
		"CREATE FUNCTION `%s`(%s) RETURNS %v AS (%s)",
		s.FuncName(),
		strings.Join(args, ", "),
		m1(retType.Kind()),
		s.Body,
	)
}

func (s *FunctionSpec) CallSQL(ctx context.Context, callNode *ResolvedBaseFunctionCallNode, argValues []string) (string, error) {
	args, _ := callNode.ArgumentList()
	var body string
	if s.Body == "" {
		// templated argument func
		definedArgs := make([]string, 0, len(args))
		for idx, arg := range args {
			typeName := newType(m1(arg.Type())).FormatType()
			definedArgs = append(
				definedArgs,
				fmt.Sprintf("%s %s", s.Args[idx].Name, typeName),
			)
		}
		funcName := strings.Join(s.NamePath, ".")
		runtimeDefinedFunc := fmt.Sprintf(
			"CREATE FUNCTION `%s`(%s) as (%s)",
			funcName,
			strings.Join(definedArgs, ","),
			s.Code,
		)
		analyzer := analyzerFromContext(ctx)
		runtimeSpec, err := analyzer.analyzeTemplatedFunctionWithRuntimeArgument(ctx, runtimeDefinedFunc)
		if err != nil {
			return "", err
		}
		body = runtimeSpec.Body
	} else {
		body = s.Body
	}
	for i := 0; i < len(s.Args); i++ {
		argRef := fmt.Sprintf("@%s", s.Args[i].Name)
		value := argValues[i]
		body = strings.Replace(body, argRef, value, -1)
	}
	return fmt.Sprintf("( %s )", body), nil
}

type TableSpec struct {
	IsTemp     bool           `json:"isTemp"`
	IsView     bool           `json:"isView"`
	NamePath   []string       `json:"namePath"`
	Columns    []*ColumnSpec  `json:"columns"`
	PrimaryKey []string       `json:"primaryKey"`
	CreateMode googlesql.ResolvedCreateStatementEnums_CreateMode `json:"createMode"`
	Query      string         `json:"query"`
	UpdatedAt  time.Time      `json:"updatedAt"`
	CreatedAt  time.Time      `json:"createdAt"`
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
	return formatPath(s.NamePath)
}

func (s *TableSpec) SQLiteSchema() string {
	if s.IsView {
		return viewSQLiteSchema(s)
	}
	if s.Query != "" {
		return fmt.Sprintf("CREATE TABLE `%s` AS %s", s.TableName(), s.Query)
	}
	columns := []string{}
	for _, c := range s.Columns {
		columns = append(columns, c.SQLiteSchema())
	}
	if len(s.PrimaryKey) != 0 {
		columns = append(
			columns,
			fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(s.PrimaryKey, ",")),
		)
	}
	var stmt string
	switch s.CreateMode {
	case googlesql.ResolvedCreateStatementEnums_CreateModeCreateDefault:
		stmt = "CREATE TABLE"
	case googlesql.ResolvedCreateStatementEnums_CreateModeCreateOrReplace:
		stmt = "CREATE TABLE"
	case googlesql.ResolvedCreateStatementEnums_CreateModeCreateIfNotExists:
		stmt = "CREATE TABLE IF NOT EXISTS"
	}
	return fmt.Sprintf("%s `%s` (%s)", stmt, s.TableName(), strings.Join(columns, ","))
}

func viewSQLiteSchema(s *TableSpec) string {
	var stmt string
	switch s.CreateMode {
	case googlesql.ResolvedCreateStatementEnums_CreateModeCreateDefault:
		stmt = "CREATE VIEW"
	case googlesql.ResolvedCreateStatementEnums_CreateModeCreateOrReplace:
		stmt = "CREATE VIEW"
	case googlesql.ResolvedCreateStatementEnums_CreateModeCreateIfNotExists:
		stmt = "CREATE VIEW IF NOT EXISTS"
	}
	return fmt.Sprintf("%s `%s` AS %s", stmt, s.TableName(), s.Query)
}

type ColumnSpec struct {
	Name      string `json:"name"`
	Type      *Type  `json:"type"`
	IsNotNull bool   `json:"isNotNull"`
}

type Type struct {
	Name          string                      `json:"name"`
	Kind          int                         `json:"kind"`
	SignatureKind googlesql.SignatureArgumentKind `json:"signatureKind"`
	ElementType   *Type                       `json:"elementType"`
	FieldTypes    []*NameWithType             `json:"fieldTypes"`
}

func (t *Type) FunctionArgumentType() (*googlesql.FunctionArgumentType, error) {
	if t.SignatureKind != googlesql.SignatureArgumentKindArgTypeFixed {
		return NewTemplatedFunctionArgumentType(
			t.SignatureKind,
			NewFunctionArgumentTypeOptions(googlesql.FunctionEnums_ArgumentCardinalityRequired),
		), nil
	}
	typ, err := t.ToZetaSQLType()
	if err != nil {
		return nil, err
	}
	opt := NewFunctionArgumentTypeOptions(googlesql.FunctionEnums_ArgumentCardinalityRequired)
	return NewFunctionArgumentType(typ, opt), nil
}

// kindAs returns t.Kind as a googlesql.TypeKind for comparison with
// the enum constants the rest of the code uses.
func (t *Type) kindAs() googlesql.TypeKind { return googlesql.TypeKind(t.Kind) }

func (t *Type) IsArray() bool {
	return t.kindAs() == googlesql.TypeKindTypeArray
}

func (t *Type) IsStruct() bool {
	return t.kindAs() == googlesql.TypeKindTypeStruct
}

func (t *Type) AvailableAutoIndex() bool {
	switch t.kindAs() {
	case googlesql.TypeKindTypeBytes, googlesql.TypeKindTypeJson, googlesql.TypeKindTypeArray, googlesql.TypeKindTypeStruct,
		googlesql.TypeKindTypeGeography, googlesql.TypeKindTypeProto, googlesql.TypeKindTypeExtended:
		return false
	}
	return true
}

func (t *Type) GoReflectType() (reflect.Type, error) {
	switch t.kindAs() {
	case googlesql.TypeKindTypeInt32, googlesql.TypeKindTypeInt64, googlesql.TypeKindTypeUint32, googlesql.TypeKindTypeUint64:
		return reflect.TypeOf(int64(0)), nil
	case googlesql.TypeKindTypeBool:
		return reflect.TypeOf(false), nil
	case googlesql.TypeKindTypeFloat, googlesql.TypeKindTypeDouble:
		return reflect.TypeOf(float64(0)), nil
	case googlesql.TypeKindTypeBytes, googlesql.TypeKindTypeString, googlesql.TypeKindTypeNumeric, googlesql.TypeKindTypeBignumeric,
		googlesql.TypeKindTypeDate, googlesql.TypeKindTypeDatetime, googlesql.TypeKindTypeTime, googlesql.TypeKindTypeTimestamp, googlesql.TypeKindTypeInterval, googlesql.TypeKindTypeJson:
		return reflect.TypeOf(""), nil
	case googlesql.TypeKindTypeArray:
		elem, err := t.ElementType.GoReflectType()
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elem), nil
	case googlesql.TypeKindTypeStruct:
		return reflect.TypeOf(map[string]interface{}{}), nil
	}
	return nil, fmt.Errorf("cannot convert %s to reflect.Type", t.Name)
}

func (t *Type) ToZetaSQLType() (googlesql.Googlesql_TypeNode, error) {
	if t == nil {
		return nil, fmt.Errorf("nil Type cannot be converted to googlesql type")
	}
	switch t.kindAs() {
	case googlesql.TypeKindTypeArray:
		if t.ElementType == nil {
			return nil, fmt.Errorf("ArrayType.ElementType is nil")
		}
		typ, err := t.ElementType.ToZetaSQLType()
		if err != nil {
			return nil, err
		}
		return NewArrayType(typ)
	case googlesql.TypeKindTypeStruct:
		var fields []*StructField
		for _, field := range t.FieldTypes {
			typ, err := field.Type.ToZetaSQLType()
			if err != nil {
				return nil, err
			}
			fields = append(fields, NewStructField(field.Name, typ))
		}
		return NewStructType(fields)
	}
	return TypeFromKind(t.kindAs()), nil
}

func (t *Type) FormatType() string {
	switch t.kindAs() {
	case googlesql.TypeKindTypeStruct:
		formatTypes := make([]string, 0, len(t.FieldTypes))
		for _, field := range t.FieldTypes {
			formatTypes = append(formatTypes, fmt.Sprintf("`%s` %s", field.Name, field.Type.FormatType()))
		}
		return fmt.Sprintf("STRUCT<%s>", strings.Join(formatTypes, ","))
	case googlesql.TypeKindTypeArray:
		return fmt.Sprintf("ARRAY<%s>", t.ElementType.FormatType())
	}
	return fmt.Sprintf("%v", t.kindAs())
}

func (s *ColumnSpec) SQLiteSchema() string {
	var typ string
	switch googlesql.TypeKind(s.Type.Kind) {
	case googlesql.TypeKindTypeInt32, googlesql.TypeKindTypeInt64, googlesql.TypeKindTypeUint32, googlesql.TypeKindTypeUint64:
		typ = "INT"
	case googlesql.TypeKindTypeEnum:
		typ = "INT"
	case googlesql.TypeKindTypeBool:
		typ = "BOOLEAN"
	case googlesql.TypeKindTypeFloat:
		typ = "FLOAT"
	case googlesql.TypeKindTypeBytes:
		typ = "BLOB"
	case googlesql.TypeKindTypeDouble:
		typ = "DOUBLE"
	case googlesql.TypeKindTypeJson:
		typ = "JSON"
	case googlesql.TypeKindTypeString:
		typ = "TEXT"
	case googlesql.TypeKindTypeDate:
		typ = "TEXT"
	case googlesql.TypeKindTypeTimestamp:
		typ = "TEXT"
	case googlesql.TypeKindTypeArray:
		typ = "TEXT"
	case googlesql.TypeKindTypeStruct:
		typ = "TEXT"
	case googlesql.TypeKindTypeProto:
		typ = "TEXT"
	case googlesql.TypeKindTypeTime:
		typ = "TEXT"
	case googlesql.TypeKindTypeDatetime:
		typ = "TEXT"
	case googlesql.TypeKindTypeGeography:
		typ = "TEXT"
	case googlesql.TypeKindTypeNumeric:
		typ = "TEXT"
	case googlesql.TypeKindTypeBignumeric:
		typ = "TEXT"
	case googlesql.TypeKindTypeExtended:
		typ = "TEXT"
	case googlesql.TypeKindTypeInterval:
		typ = "TEXT"
	default:
		typ = "UNKNOWN"
	}
	schema := fmt.Sprintf("`%s` %s", s.Name, typ)
	if s.IsNotNull {
		schema += " NOT NULL"
	}
	return schema
}

func newTypeFromFunctionArgumentType(t *googlesql.FunctionArgumentType) *Type {
	if m1(t.IsTemplated()) {
		return &Type{SignatureKind: m1(t.Kind())}
	}
	return newType(m1(t.Type()))
}

func newFunctionSpec(ctx context.Context, namePath *NamePath, stmt googlesql.ResolvedCreateFunctionStmtNode) (*FunctionSpec, error) {
	args := []*NameWithType{}
	signature, _ := stmt.Signature()
	for _, arg := range compatSignatureArguments(signature) {
		args = append(args, &NameWithType{
			Name: m1(arg.ArgumentName()),
			Type: newTypeFromFunctionArgumentType(arg),
		})
	}

	var body string
	language, _ := stmt.Language()
	switch language {
	case "js":
		code, err := EncodeGoValue(StringType(), m1(stmt.Code()))
		if err != nil {
			return nil, err
		}
		encodedType, err := json.Marshal(newType(m1(stmt.ReturnType())))
		if err != nil {
			return nil, err
		}
		retType, err := EncodeGoValue(StringType(), string(encodedType))
		if err != nil {
			return nil, err
		}
		argParams := make([]string, 0, len(args))
		argNames := make([]string, 0, len(args))
		for _, arg := range args {
			argParams = append(argParams, fmt.Sprintf("@%s", arg.Name))
			argNames = append(argNames, arg.Name)
		}
		if len(argParams) == 0 {
			body = fmt.Sprintf("zetasqlite_eval_javascript('%s', '%s')", code, retType)
		} else {
			arr, err := EncodeGoValue(StringArrayType(), argNames)
			if err != nil {
				return nil, err
			}
			body = fmt.Sprintf(
				"zetasqlite_eval_javascript('%s', '%s', '%s', %s)",
				code, retType, arr,
				strings.Join(argParams, ","),
			)
		}
	default:
		funcExpr, _ := stmt.FunctionExpression()
		if funcExpr != nil {
			bodyQuery, err := newNode(funcExpr).FormatSQL(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to format function expression: %w", err)
			}
			body = bodyQuery
		}
	}
	now := time.Now()
	return &FunctionSpec{
		IsTemp:    ResolvedCreateStatementCreateScope(stmt) == googlesql.ResolvedCreateStatementEnums_CreateScopeCreateTemp,
		NamePath:  namePath.mergePath(m1(stmt.NamePath())),
		Args:      args,
		Return:    newType(m1(stmt.ReturnType())),
		Code:      m1(stmt.Code()),
		Body:      body,
		Language:  language,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func newTypeFromFunctionArgumentTypeByRealType(t *googlesql.FunctionArgumentType, realType googlesql.Googlesql_TypeNode) *Type {
	if m1(t.IsTemplated()) {
		if m1(realType.IsArray()) {
			return &Type{SignatureKind: googlesql.SignatureArgumentKindArgArrayTypeAny1}
		}
		return &Type{SignatureKind: googlesql.SignatureArgumentKindArgTypeAny1}
	}
	return newType(m1(t.Type()))
}

func newTemplatedFunctionSpec(ctx context.Context, namePath *NamePath, stmt googlesql.ResolvedCreateFunctionStmtNode, realStmts []googlesql.ResolvedCreateFunctionStmtNode) (*FunctionSpec, error) {
	signature, _ := stmt.Signature()
	arguments := compatSignatureArguments(signature)
	realStmt := realStmts[0]
	realSignature, _ := realStmt.Signature()
	realArguments := compatSignatureArguments(realSignature)
	resultType := newType(m1(m1(realSignature.ResultType()).Type()))
	resultTypeName := resultType.FormatType()

	allSameResultType := true
	for _, stmt := range realStmts {
		if newType(m1(m1(m1(stmt.Signature()).ResultType()).Type())).FormatType() != resultTypeName {
			allSameResultType = false
			break
		}
	}
	var retType *Type
	if allSameResultType {
		retType = resultType
	} else {
		retType = newTypeFromFunctionArgumentTypeByRealType(
			m1(signature.ResultType()),
			m1(m1(realSignature.ResultType()).Type()),
		)
	}
	args := []*NameWithType{}
	for i := 0; i < len(arguments); i++ {
		args = append(args, &NameWithType{
			Name: m1(arguments[i].ArgumentName()),
			Type: newTypeFromFunctionArgumentTypeByRealType(
				arguments[i],
				m1(realArguments[i].Type()),
			),
		})
	}
	funcExpr, _ := stmt.FunctionExpression()
	var body string
	if funcExpr != nil {
		bodyQuery, err := newNode(funcExpr).FormatSQL(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to format function expression: %w", err)
		}
		body = bodyQuery
	}
	now := time.Now()
	return &FunctionSpec{
		IsTemp:    ResolvedCreateStatementCreateScope(stmt) == googlesql.ResolvedCreateStatementEnums_CreateScopeCreateTemp,
		NamePath:  namePath.mergePath(m1(stmt.NamePath())),
		Args:      args,
		Return:    retType,
		Code:      m1(stmt.Code()),
		Body:      body,
		Language:  m1(stmt.Language()),
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func newColumnsFromDef(def []googlesql.ResolvedColumnDefinitionNode) []*ColumnSpec {
	columns := []*ColumnSpec{}
	for _, columnNode := range def {
		annotation, _ := columnNode.Annotations()
		var isNotNull bool
		if annotation != nil {
			// annotation.TypeParameters isn't exposed on the bridge
			// yet; keep the hook but skip type-param extraction.
			isNotNull, _ = annotation.NotNull()
		}
		columns = append(columns, &ColumnSpec{
			Name:      m1(columnNode.Name()),
			Type:      newType(m1(columnNode.Type())),
			IsNotNull: isNotNull,
		})
	}
	return columns
}

func newColumnsFromOutputColumns(def []googlesql.ResolvedOutputColumnNode) []*ColumnSpec {
	columns := []*ColumnSpec{}
	for _, columnNode := range def {
		column, _ := columnNode.Column()

		columns = append(columns, &ColumnSpec{
			Name: m1(columnNode.Name()),
			Type: newType(m1(column.Type())),
		})
	}
	return columns
}

func newPrimaryKey(key googlesql.ResolvedPrimaryKeyNode) []string {
	if key == nil {
		return nil
	}
	names, _ := key.ColumnNameList()
	return names
}

func newTableSpec(namePath *NamePath, stmt googlesql.ResolvedCreateTableStmtNode) *TableSpec {
	return newTableSpecWithQuery(namePath, "", stmt)
}

func newTableSpecWithQuery(namePath *NamePath, query string, stmt googlesql.ResolvedCreateTableStmtNode) *TableSpec {
	now := time.Now()
	return &TableSpec{
		IsTemp:     isTempCreate(query, stmt),
		NamePath:   namePath.mergePath(m1(stmt.NamePath())),
		Columns:    newColumnsFromDef(m1(stmt.ColumnDefinitionList())),
		PrimaryKey: newPrimaryKey(m1(stmt.PrimaryKey())),
		CreateMode: ResolvedCreateStatementCreateMode(stmt),
		UpdatedAt:  now,
		CreatedAt:  now,
	}
}

// isTempCreate prefers the resolved-tree CreateScope accessor when it
// returns a non-default value, and falls back to scanning the original
// query text while that accessor is still stubbed out.
func isTempCreate(query string, stmt any) bool {
	scope := ResolvedCreateStatementCreateScope(stmt)
	if scope == googlesql.ResolvedCreateStatementEnums_CreateScopeCreateTemp {
		return true
	}
	if query != "" && ResolvedCreateStatementIsTempFromQuery(query) {
		return true
	}
	return false
}

func newTableAsViewSpec(namePath *NamePath, query string, stmt googlesql.ResolvedCreateViewStmtNode) *TableSpec {
	var outputColumns []string
	outList, _ := stmt.OutputColumnList()
	for _, column := range outList {
		colName, _ := column.Name()
		col, _ := column.Column()
		refColumnName, _ := col.Name()
		colID, _ := col.ColumnId()
		outputColumns = append(
			outputColumns,
			fmt.Sprintf("`%s#%d` AS `%s`", refColumnName, colID, colName),
		)
	}
	now := time.Now()
	return &TableSpec{
		IsTemp:     ResolvedCreateStatementCreateScope(stmt) == googlesql.ResolvedCreateStatementEnums_CreateScopeCreateTemp,
		IsView:     true,
		NamePath:   namePath.mergePath(m1(stmt.NamePath())),
		Columns:    newColumnsFromOutputColumns(m1(stmt.OutputColumnList())),
		CreateMode: ResolvedCreateStatementCreateMode(stmt),
		Query:      fmt.Sprintf("SELECT %s FROM (%s)", strings.Join(outputColumns, ","), query),
		UpdatedAt:  now,
		CreatedAt:  now,
	}
}

func newTableAsSelectSpec(namePath *NamePath, query string, stmt googlesql.ResolvedCreateTableAsSelectStmtNode) *TableSpec {
	var outputColumns []string
	for _, column := range m1(stmt.OutputColumnList()) {
		colName, _ := column.Name()
		refColumnName, _ := m1(column.Column()).Name()
		colID, _ := m1(column.Column()).ColumnId()
		outputColumns = append(
			outputColumns,
			fmt.Sprintf("`%s#%d` AS `%s`", refColumnName, colID, colName),
		)
	}
	now := time.Now()
	return &TableSpec{
		IsTemp:     ResolvedCreateStatementCreateScope(stmt) == googlesql.ResolvedCreateStatementEnums_CreateScopeCreateTemp,
		NamePath:   namePath.mergePath(m1(stmt.NamePath())),
		Columns:    newColumnsFromDef(m1(stmt.ColumnDefinitionList())),
		PrimaryKey: newPrimaryKey(m1(stmt.PrimaryKey())),
		CreateMode: ResolvedCreateStatementCreateMode(stmt),
		Query:      fmt.Sprintf("SELECT %s FROM (%s)", strings.Join(outputColumns, ","), query),
		UpdatedAt:  now,
		CreatedAt:  now,
	}
}

func newType(t googlesql.Googlesql_TypeNode) *Type {
	// Googlesql_TypeNode exposes the base-class accessors directly.
	kind := m1(t.Kind())
	var (
		elem       *Type
		fieldTypes []*NameWithType
	)
	// Composite types need the nested element information so
	// CAST(... AS ARRAY<T>) and friends can round-trip through the
	// formatter. ArrayType.ElementType and StructType.Fields are now
	// exposed on the bridge; recurse into them when the dynamic type
	// matches.
	switch kind {
	case googlesql.TypeKindTypeArray:
		if at, ok := t.(*googlesql.ArrayType); ok {
			if e, err := at.ElementType(); err == nil && e != nil {
				elem = newType(e)
			}
		}
	case googlesql.TypeKindTypeStruct:
		if st, ok := t.(*googlesql.StructType); ok {
			fields, _ := st.Fields()
			for _, field := range fields {
				if field == nil || field.Type_ == nil {
					continue
				}
				fieldTypes = append(fieldTypes, &NameWithType{
					Name: field.Name,
					Type: newType(field.Type_),
				})
			}
		}
	}
	// Googlesql_TypeNode interface does not expose TypeName(mode); use
	// DebugString instead which gives an equivalent printable form.
	name, _ := t.DebugString(false)
	return &Type{
		Name:        name,
		Kind:        int(kind),
		ElementType: elem,
		FieldTypes:  fieldTypes,
	}
}
