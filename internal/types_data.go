package internal

import (
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
)

// Data structures for pure transformation inputs

// ExpressionData represents the pure data extracted from an expression node
type ExpressionData struct {
	Type      ExpressionType        `json:"type,omitempty"`
	Parameter *ParameterData        `json:"parameter,omitempty"`
	Literal   *LiteralData          `json:"literal,omitempty"`
	Function  *FunctionCallData     `json:"function,omitempty"`
	Cast      *CastData             `json:"cast,omitempty"`
	Column    *ColumnRefData        `json:"column,omitempty"`
	Binary    *BinaryExpressionData `json:"binary,omitempty"`
	Case      *CaseExpressionData   `json:"case,omitempty"`
	Subquery  *SubqueryData         `json:"subquery,omitempty"`
}

func (e *ExpressionData) Value() interface{} {
	switch e.Type {
	case ExpressionTypeLiteral:
		return e.Literal
	case ExpressionTypeCase:
		return e.Case
	case ExpressionTypeFunction:
		return e.Function
	case ExpressionTypeCast:
		return e.Cast
	case ExpressionTypeColumn:
		return e.Column
	case ExpressionTypeBinary:
		return e.Binary
	case ExpressionTypeSubquery:
		return e.Subquery
	default:
		return nil
	}
}

// LiteralData represents literal value data
type LiteralData struct {
	Value    Value          `json:"value,omitempty"`     // Use zetasqlite Value which handles both Go literals and ZetaSQL values
	TypeName string         `json:"type_name,omitempty"` // String representation of type for reference
	Location *ParseLocation `json:"location,omitempty"`
}

// ParameterData represents a parameter binding value
type ParameterData struct {
	Identifier string `json:"identifier,omitempty"`
}

type WindowSpecificationData struct {
	PartitionBy []*ExpressionData  `json:"partition_by,omitempty"`
	OrderBy     []*OrderByItemData `json:"order_by,omitempty"`
	FrameClause *FrameClauseData   `json:"frame_clause,omitempty"`
}

// FunctionCallData represents function call data
type FunctionCallData struct {
	Name       string                   `json:"name,omitempty"`
	Arguments  []ExpressionData         `json:"arguments,omitempty"`
	WindowSpec *WindowSpecificationData `json:"window_spec,omitempty"`
	Signature  *FunctionSignature       `json:"signature,omitempty"`
}

func NewFunctionCallExpressionData(name string, arguments ...ExpressionData) ExpressionData {
	return ExpressionData{
		Type: ExpressionTypeFunction,
		Function: &FunctionCallData{
			Name:      name,
			Arguments: arguments,
		},
	}
}

// FrameClause represents window frame specifications
type FrameClauseData struct {
	Unit  string // ROWS, RANGE, GROUPS
	Start *FrameBoundData
	End   *FrameBoundData
}

// FrameBoundData represents frame boundary specifications
type FrameBoundData struct {
	Type   string // UNBOUNDED, CURRENT, PRECEDING, FOLLOWING
	Offset ExpressionData
}

// FunctionSignature represents function signature information
type FunctionSignature struct {
	Arguments []*ArgumentInfo `json:"arguments,omitempty"`
}

// ArgumentInfo represents function argument metadata
type ArgumentInfo struct {
	Name string     `json:"name,omitempty"`
	Type types.Type `json:"type,omitempty"`
}

// CastData represents type casting data
type CastData struct {
	Expression      ExpressionData `json:"expression,omitempty"`
	FromType        types.Type     `json:"from_type,omitempty"`
	ToType          types.Type     `json:"to_type,omitempty"`
	SafeCast        bool           `json:"safe_cast,omitempty"`
	ReturnNullOnErr bool           `json:"return_null_on_err,omitempty"`
}

// ColumnRefData represents column reference data
type ColumnRefData struct {
	Column     *ast.Column `json:"column,omitempty"`
	Type       types.Type  `json:"type,omitempty"`
	TableAlias string      `json:"table_alias,omitempty"`
	ColumnName string      `json:"column_name,omitempty"`
	ColumnID   int         `json:"column_id,omitempty"`
	TableName  string      `json:"table_name,omitempty"` // Original table name from AST
}

func NewColumnExpressionData(column *ast.Column) ExpressionData {
	return ExpressionData{
		Type: ExpressionTypeColumn,
		Column: &ColumnRefData{
			Column:     column,
			ColumnName: column.Name(),
			ColumnID:   column.ColumnID(),
			TableName:  column.TableName(),
			Type:       column.Type(),
		},
	}
}

// ColumnData represents extracted column information for JSON serialization
type ColumnData struct {
	ID        int    `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	Type      string `json:"type,omitempty"`
	TableName string `json:"table_name,omitempty"`
}

// BinaryExpressionData represents binary operation data
type BinaryExpressionData struct {
	Left     ExpressionData `json:"left,omitempty"`
	Operator string         `json:"operator,omitempty"`
	Right    ExpressionData `json:"right,omitempty"`
}

// CaseExpressionData represents CASE expression data
type CaseExpressionData struct {
	CaseExpr    *ExpressionData   `json:"case_expr,omitempty"` // Optional - for CASE expr WHEN...
	WhenClauses []*WhenClauseData `json:"when_clauses,omitempty"`
	ElseClause  *ExpressionData   `json:"else_clause,omitempty"`
}

// WhenClauseData represents a WHEN clause in CASE expressions
type WhenClauseData struct {
	Condition ExpressionData `json:"condition,omitempty"`
	Result    ExpressionData `json:"result,omitempty"`
}

// SubqueryData represents subquery expression data
type SubqueryData struct {
	Query        ScanData         `json:"query,omitempty"`
	SubqueryType ast.SubqueryType `json:"subquery_type,omitempty"`
	InExpr       *ExpressionData  `json:"in_expr,omitempty"`
}

// StatementData represents statement-level data
type StatementData struct {
	Type   StatementType `json:"type,omitempty"`
	Select *SelectData   `json:"select,omitempty"`
	Insert *InsertData   `json:"insert,omitempty"`
	Update *UpdateData   `json:"update,omitempty"`
	Delete *DeleteData   `json:"delete,omitempty"`
	Create *CreateData   `json:"create,omitempty"`
	Drop   *DropData     `json:"drop,omitempty"`
	Merge  *MergeData    `json:"merge,omitempty"`
}

// StatementType identifies the type of statement
type StatementType int

const (
	StatementTypeSelect StatementType = iota
	StatementTypeInsert
	StatementTypeUpdate
	StatementTypeDelete
	StatementTypeCreate
	StatementTypeDrop
	StatementTypeMerge
)

// SelectData represents SELECT statement data
type SelectData struct {
	SelectList   []*SelectItemData  `json:"select_list,omitempty"`
	FromClause   *ScanData          `json:"from_clause,omitempty"`
	WhereClause  *ExpressionData    `json:"where_clause,omitempty"`
	GroupBy      []*ExpressionData  `json:"group_by,omitempty"`
	Having       *ExpressionData    `json:"having,omitempty"`
	OrderBy      []*OrderByItemData `json:"order_by,omitempty"`
	Limit        *LimitData         `json:"limit,omitempty"`
	SetOperation *SetOperationData  `json:"set_operation,omitempty"`
}

// SelectItemData represents a SELECT list item
type SelectItemData struct {
	Expression ExpressionData `json:"expression,omitempty"`
	Alias      string         `json:"alias,omitempty"`
}

// ScanData represents scan operation data
type ScanData struct {
	Type       ScanType      `json:"type,omitempty"`
	ColumnList []*ColumnData `json:"column_list,omitempty"` // Output columns from this scan

	TableScan        *TableScanData     `json:"table_scan,omitempty"`
	JoinScan         *JoinScanData      `json:"join_scan,omitempty"`
	FilterScan       *FilterScanData    `json:"filter_scan,omitempty"`
	ProjectScan      *ProjectScanData   `json:"project_scan,omitempty"`
	AggregateScan    *AggregateScanData `json:"aggregate_scan,omitempty"`
	OrderByScan      *OrderByScanData   `json:"order_by_scan,omitempty"`
	LimitScan        *LimitScanData     `json:"limit_scan,omitempty"`
	SetOperationScan *SetOperationData  `json:"set_operation_scan,omitempty"`
	WithScan         *WithScanData      `json:"with_scan,omitempty"`
	WithRefScan      *WithRefScanData   `json:"with_ref_scan,omitempty"`
	WithEntryScan    *WithEntryData     `json:"with_entry_scan,omitempty"`
	ArrayScan        *ArrayScanData     `json:"array_scan,omitempty"`
	AnalyticScan     *AnalyticScanData  `json:"analytic_scan,omitempty"`
}

func (s *ScanData) FindColumnByID(id int) *ColumnData {
	for _, col := range s.ColumnList {
		if col.ID == id {
			return col
		}
	}
	return nil
}

// ScanType identifies the type of scan operation
type ScanType int

const (
	ScanTypeTable ScanType = iota
	ScanTypeJoin
	ScanTypeFilter
	ScanTypeProject
	ScanTypeAggregate
	ScanTypeOrderBy
	ScanTypeLimit
	ScanTypeSetOp
	ScanTypeSingleRow
	ScanTypeWith
	ScanTypeWithRef
	ScanTypeWithEntry
	ScanTypeArray
	ScanTypeAnalytic
)

// TableScanData represents table scan data
type TableScanData struct {
	TableName        string            `json:"table_name,omitempty"`
	Alias            string            `json:"alias,omitempty"`
	Columns          []*ColumnData     `json:"columns,omitempty"`
	SyntheticColumns []*SelectItemData `json:"synthetic_columns,omitempty"`
}

// JoinScanData represents join operation data
type JoinScanData struct {
	JoinType      ast.JoinType    `json:"join_type,omitempty"`
	LeftScan      ScanData        `json:"left_scan,omitempty"`
	RightScan     ScanData        `json:"right_scan,omitempty"`
	JoinCondition *ExpressionData `json:"join_condition,omitempty"`
	UsingColumns  []string        `json:"using_columns,omitempty"`
}

// FilterScanData represents filter operation data
type FilterScanData struct {
	InputScan  ScanData       `json:"input_scan,omitempty"`
	FilterExpr ExpressionData `json:"filter_expr,omitempty"`
}

// ProjectScanData represents projection operation data
type ProjectScanData struct {
	InputScan ScanData              `json:"input_scan,omitempty"`
	ExprList  []*ComputedColumnData `json:"expr_list,omitempty"`
}

// ComputedColumnData represents computed column data
type ComputedColumnData struct {
	Column     *ast.Column    `json:"column,omitempty"`
	Expression ExpressionData `json:"expression,omitempty"`
}

// AggregateScanData represents aggregate operation data
type AggregateScanData struct {
	InputScan     ScanData              `json:"input_scan,omitempty"`
	GroupByList   []*ComputedColumnData `json:"group_by_list,omitempty"`
	AggregateList []*ComputedColumnData `json:"aggregate_list,omitempty"`
	GroupingSets  []*GroupingSetData    `json:"grouping_sets,omitempty"`
}

// OrderByScanData represents ORDER BY operation data
type OrderByScanData struct {
	InputScan      ScanData           `json:"input_scan,omitempty"`
	OrderByColumns []*OrderByItemData `json:"order_by_columns,omitempty"`
}

// GroupingSetData represents a grouping set
type GroupingSetData struct {
	GroupByColumns []*ComputedColumnData `json:"group_by_columns,omitempty"`
}

// OrderByItemData represents ORDER BY item data
type OrderByItemData struct {
	Expression   ExpressionData    `json:"expression,omitempty"`
	IsDescending bool              `json:"is_descending,omitempty"`
	NullOrder    ast.NullOrderMode `json:"null_order,omitempty"`
}

// LimitData represents LIMIT clause data
type LimitData struct {
	Count  ExpressionData  `json:"count,omitempty"`
	Offset *ExpressionData `json:"offset,omitempty"`
}

// LimitScanData represents LIMIT/OFFSET scan operation data
type LimitScanData struct {
	InputScan ScanData       `json:"input_scan,omitempty"` // The nested scan being limited
	Count     ExpressionData `json:"count,omitempty"`      // LIMIT expression
	Offset    ExpressionData `json:"offset,omitempty"`     // OFFSET expression (optional)
}

// ArrayScanData represents array scan (UNNEST) operation data
type ArrayScanData struct {
	InputScan         *ScanData       `json:"input_scan,omitempty"`          // Optional input scan for correlated arrays
	ArrayExpr         ExpressionData  `json:"array_expr,omitempty"`          // Array expression to UNNEST
	ElementColumn     *ColumnData     `json:"element_column,omitempty"`      // Column for array elements
	ArrayOffsetColumn *ColumnData     `json:"array_offset_column,omitempty"` // Optional column for array indices
	IsOuter           bool            `json:"is_outer,omitempty"`            // Whether to use LEFT JOIN (true) or INNER JOIN (false)
	JoinExpr          *ExpressionData `json:"join_expr,omitempty"`           // Optional join condition
}

// AnalyticScanData represents analytic (window function) scan operation data
type AnalyticScanData struct {
	InputScan    ScanData              `json:"input_scan,omitempty"`    // The nested scan providing input
	FunctionList []*ComputedColumnData `json:"function_list,omitempty"` // List of analytic function calls
}

// SetOperationData represents set operation data
type SetOperationData struct {
	Type     string          `json:"type,omitempty"`     // UNION, INTERSECT, EXCEPT
	Modifier string          `json:"modifier,omitempty"` // ALL, DISTINCT
	Items    []StatementData `json:"items,omitempty"`    // List of statements to combine
}

// ParseLocation represents source location information
type ParseLocation struct {
	StartLine   int    `json:"start_line,omitempty"`
	StartColumn int    `json:"start_column,omitempty"`
	EndLine     int    `json:"end_line,omitempty"`
	EndColumn   int    `json:"end_column,omitempty"`
	Filename    string `json:"filename,omitempty"`
}

// Additional data types for other statements...

// InsertData represents INSERT statement data
type InsertData struct {
	TableName string             `json:"table_name,omitempty"`
	Columns   []string           `json:"columns,omitempty"`
	Values    [][]ExpressionData `json:"values,omitempty"`
	Query     *SelectData        `json:"query,omitempty"`
}

// UpdateData represents UPDATE statement data
type UpdateData struct {
	TableName   string          `json:"table_name,omitempty"`
	TableScan   *ScanData       `json:"table_scan,omitempty"`
	SetItems    []*SetItemData  `json:"set_items,omitempty"`
	FromClause  *ScanData       `json:"from_clause,omitempty"`
	WhereClause *ExpressionData `json:"where_clause,omitempty"`
}

// SetItemData represents SET item in UPDATE
type SetItemData struct {
	Column string         `json:"column,omitempty"`
	Value  ExpressionData `json:"value,omitempty"`
}

// DeleteData represents DELETE statement data
type DeleteData struct {
	TableName   string          `json:"table_name,omitempty"`
	TableScan   *ScanData       `json:"table_scan,omitempty"`
	WhereClause *ExpressionData `json:"where_clause,omitempty"`
}

// CreateData represents CREATE statement data
type CreateData struct {
	Type     CreateType          `json:"type,omitempty"`
	Table    *CreateTableData    `json:"table,omitempty"`
	View     *CreateViewData     `json:"view,omitempty"`
	Function *CreateFunctionData `json:"function,omitempty"`
}

// CreateType identifies the type of CREATE statement
type CreateType int

const (
	CreateTypeTable CreateType = iota
	CreateTypeView
)

// CreateTableData represents CREATE TABLE data
type CreateTableData struct {
	TableName   string                  `json:"table_name,omitempty"`
	Columns     []*ColumnDefinitionData `json:"columns,omitempty"`
	AsSelect    *SelectData             `json:"as_select,omitempty"`
	IfNotExists bool                    `json:"if_not_exists,omitempty"`
}

// ColumnDefinitionData represents column definition data
type ColumnDefinitionData struct {
	Name         string          `json:"name,omitempty"`
	Type         string          `json:"type,omitempty"`
	NotNull      bool            `json:"not_null,omitempty"`
	IsPrimaryKey bool            `json:"is_primary_key,omitempty"`
	DefaultValue *ExpressionData `json:"default_value,omitempty"`
}

// CreateViewData represents CREATE VIEW data
type CreateViewData struct {
	ViewName string     `json:"view_name,omitempty"`
	Query    SelectData `json:"query,omitempty"`
}

// CreateFunctionData represents CREATE FUNCTION data
type CreateFunctionData struct {
	FunctionName string                     `json:"function_name,omitempty"`
	Parameters   []*ParameterDefinitionData `json:"parameters,omitempty"`
	ReturnType   string                     `json:"return_type,omitempty"`
	Language     string                     `json:"language,omitempty"`
	Code         string                     `json:"code,omitempty"`
	Options      map[string]ExpressionData  `json:"options,omitempty"`
}

// ParameterDefinitionData represents function parameter data
type ParameterDefinitionData struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
}

// DropData represents DROP statement data
type DropData struct {
	IfExists   bool   `json:"if_exists,omitempty"`
	ObjectType string `json:"object_type,omitempty"` // TABLE, VIEW, INDEX, SCHEMA, FUNCTION
	ObjectName string `json:"object_name,omitempty"`
}

// WithScanData represents WITH scan data (complete WITH statements)
type WithScanData struct {
	WithEntryList []*WithEntryData `json:"with_entry_list,omitempty"`
	Query         ScanData         `json:"query,omitempty"`
	ColumnList    []*ColumnData    `json:"column_list,omitempty"`
}

// WithRefScanData represents WITH reference scan data (references to CTEs)
type WithRefScanData struct {
	WithQueryName string        `json:"with_query_name,omitempty"`
	ColumnList    []*ColumnData `json:"column_list,omitempty"`
}

// WithEntryData represents individual WITH entry data (CTE definitions)
type WithEntryData struct {
	WithQueryName string        `json:"with_query_name,omitempty"`
	WithSubquery  ScanData      `json:"with_subquery,omitempty"`
	ColumnList    []*ColumnData `json:"column_list,omitempty"`
}

// MergeData represents MERGE statement data
type MergeData struct {
	TargetTable string                 `json:"target_table,omitempty"`
	TargetScan  *ScanData              `json:"target_scan,omitempty"`
	SourceScan  *ScanData              `json:"source_scan,omitempty"`
	MergeExpr   ExpressionData         `json:"merge_expr,omitempty"`
	WhenClauses []*MergeWhenClauseData `json:"when_clauses,omitempty"`
}

// MergeWhenClauseData represents a WHEN clause in MERGE statements
type MergeWhenClauseData struct {
	MatchType     ast.MatchType    `json:"match_type,omitempty"`     // MATCHED, NOT_MATCHED_BY_SOURCE, NOT_MATCHED_BY_TARGET
	Condition     *ExpressionData  `json:"condition,omitempty"`      // Optional condition
	ActionType    ast.ActionType   `json:"action_type,omitempty"`    // INSERT, UPDATE, DELETE
	InsertColumns []*ColumnData    `json:"insert_columns,omitempty"` // For INSERT actions
	InsertValues  []ExpressionData `json:"insert_values,omitempty"`  // For INSERT actions
	SetItems      []*SetItemData   `json:"set_items,omitempty"`      // For UPDATE actions
}
