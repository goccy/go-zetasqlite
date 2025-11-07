package internal

import (
	"fmt"
	"github.com/goccy/go-zetasql/types"
	"strings"
)

// SQLFragment represents any component that can generate SQL
type SQLFragment interface {
	WriteSql(writer *SQLWriter)
	String() string
}

// SQLWriter handles SQL string generation with proper formatting
type SQLWriter struct {
	builder     strings.Builder
	indentLevel int
	useNewlines bool
}

func NewSQLWriter() *SQLWriter {
	return &SQLWriter{
		useNewlines: true,
	}
}

func (w *SQLWriter) Write(s string) {
	w.builder.WriteString(s)
}

func (w *SQLWriter) WriteLine(s string) {
	if w.useNewlines {
		w.builder.WriteString(strings.Repeat("  ", w.indentLevel))
	}
	w.builder.WriteString(s)
	if w.useNewlines {
		w.builder.WriteString("\n")
	} else {
		w.builder.WriteString(" ")
	}
}

func (w *SQLWriter) Indent() {
	w.indentLevel++
}

func (w *SQLWriter) Dedent() {
	if w.indentLevel > 0 {
		w.indentLevel--
	}
}

func (w *SQLWriter) String() string {
	return w.builder.String()
}

// SelectType represents different SELECT variants
type SelectType int

const (
	SelectTypeStandard SelectType = iota
	SelectTypeDistinct
	SelectTypeAll
	SelectTypeAsStruct
	SelectTypeAsValue
)

// ExpressionType represents different types of SQL expressions
type ExpressionType int

const (
	ExpressionTypeColumn ExpressionType = iota
	ExpressionTypeLiteral
	ExpressionTypeParameter
	ExpressionTypeFunction
	ExpressionTypeList
	ExpressionTypeUnary
	ExpressionTypeBinary
	ExpressionTypeSubquery
	ExpressionTypeStar
	ExpressionTypeCase
	ExpressionTypeExists
	ExpressionTypeCast
)

// CaseExpression represents SQL CASE expressions
type CaseExpression struct {
	CaseExpr    *SQLExpression // Optional expression after CASE (for CASE expr WHEN...)
	WhenClauses []*WhenClause  // WHEN condition THEN result pairs
	ElseExpr    *SQLExpression // Optional ELSE expression
}

// WhenClause represents a WHEN-THEN clause in a CASE expression
type WhenClause struct {
	Condition *SQLExpression
	Result    *SQLExpression
}

// ExistsExpression represents SQL EXISTS expressions
type ExistsExpression struct {
	Subquery *SelectStatement
}

// ListExpression represents SQL list expressions
type ListExpression struct {
	Expressions []*SQLExpression
}

func (e *ListExpression) WriteSql(writer *SQLWriter) {
	writer.Write("(")
	for i, expr := range e.Expressions {
		writer.Write(expr.String())
		if i != len(e.Expressions)-1 {
			writer.Write(",")
		}
	}
	writer.Write(")")
}

// UnaryExpression represents SQL unary expressions
type UnaryExpression struct {
	Operator   string
	Expression *SQLExpression
}

func (e *UnaryExpression) WriteSql(writer *SQLWriter) {
	writer.Write(e.Operator)
	writer.Write(" (")
	e.Expression.WriteSql(writer)
	writer.Write(")")
}

type BinaryExpression struct {
	Left     *SQLExpression
	Right    *SQLExpression
	Operator string
}

func (e *BinaryExpression) WriteSql(writer *SQLWriter) {
	if e.Left != nil {
		e.Left.WriteSql(writer)
	}
	writer.Write(fmt.Sprintf(" %s ", e.Operator))
	if e.Right != nil {
		e.Right.WriteSql(writer)
	}
}

func (e *BinaryExpression) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	e.WriteSql(writer)
	return writer.String()
}

// SQLExpression represents any SQL expression
type SQLExpression struct {
	Type             ExpressionType
	Value            string
	ListExpression   *ListExpression
	UnaryExpression  *UnaryExpression
	BinaryExpression *BinaryExpression
	FunctionCall     *FunctionCall
	Subquery         *SelectStatement
	CaseExpression   *CaseExpression
	ExistsExpr       *ExistsExpression
	Alias            string
	TableAlias       string
	Collation        string
}

func (e *SQLExpression) WriteSql(writer *SQLWriter) {
	switch e.Type {
	case ExpressionTypeColumn:
		if e.TableAlias != "" {
			writer.Write(fmt.Sprintf("`%s`.`%s`", e.TableAlias, e.Value))
		} else {
			writer.Write("`" + e.Value + "`")
		}
	case ExpressionTypeLiteral:
		writer.Write(e.Value)
	case ExpressionTypeList:
		e.ListExpression.WriteSql(writer)
	case ExpressionTypeUnary:
		e.UnaryExpression.WriteSql(writer)
	case ExpressionTypeBinary:
		e.BinaryExpression.WriteSql(writer)
	case ExpressionTypeFunction:
		if e.FunctionCall != nil {
			e.FunctionCall.WriteSql(writer)
		}
	case ExpressionTypeSubquery:
		writer.Write("(")
		if e.Subquery != nil {
			e.Subquery.WriteSql(writer)
		}
		writer.Write(")")
	case ExpressionTypeStar:
		if e.TableAlias != "" {
			writer.Write(fmt.Sprintf("%s.*", e.TableAlias))
		} else {
			writer.Write("*")
		}
	case ExpressionTypeCase:
		if e.CaseExpression != nil {
			e.CaseExpression.WriteSql(writer)
		}
	case ExpressionTypeExists:
		if e.ExistsExpr != nil {
			e.ExistsExpr.WriteSql(writer)
		}
	case ExpressionTypeParameter:
		writer.Write(e.Value)
	}

	// Add collation if specified
	if e.Collation != "" {
		writer.Write(fmt.Sprintf(" COLLATE %s", e.Collation))
	}

	if e.Alias != "" && !(e.Type == ExpressionTypeColumn && e.Alias == e.Value) {
		writer.Write(" AS ")
		writer.Write("`" + e.Alias + "`")
	}

}

func (e *SQLExpression) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = e.Subquery != nil
	e.WriteSql(writer)
	return writer.String()
}

// WriteSql method for CaseExpression
func (c *CaseExpression) WriteSql(writer *SQLWriter) {
	writer.Write("CASE")

	// Optional CASE expression (for CASE expr WHEN value THEN...)
	if c.CaseExpr != nil {
		writer.Write(" ")
		c.CaseExpr.WriteSql(writer)
	}

	// WHEN clauses
	for _, whenClause := range c.WhenClauses {
		writer.Write(" WHEN ")
		whenClause.Condition.WriteSql(writer)
		writer.Write(" THEN ")
		whenClause.Result.WriteSql(writer)
	}

	// Optional ELSE clause
	if c.ElseExpr != nil {
		writer.Write(" ELSE ")
		c.ElseExpr.WriteSql(writer)
	}

	writer.Write(" END")
}

func (c *CaseExpression) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	c.WriteSql(writer)
	return writer.String()
}

// WriteSql method for ExistsExpression
func (e *ExistsExpression) WriteSql(writer *SQLWriter) {
	writer.Write("EXISTS (")
	if e.Subquery != nil {
		e.Subquery.WriteSql(writer)
	}
	writer.Write(")")
}

func (e *ExistsExpression) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	e.WriteSql(writer)
	return writer.String()
}

// FunctionCall represents SQL function calls
type FunctionCall struct {
	Name       string
	Arguments  []*SQLExpression
	IsDistinct bool
	WindowSpec *WindowSpecification
}

func (f *FunctionCall) WriteSql(writer *SQLWriter) {
	writer.Write(f.Name)
	writer.Write("(")
	if f.IsDistinct {
		writer.Write("DISTINCT ")
	}
	for i, arg := range f.Arguments {
		if i > 0 {
			writer.Write(", ")
		}
		arg.WriteSql(writer)
	}
	writer.Write(")")
	if f.WindowSpec != nil {
		writer.Write(" OVER (")
		f.WindowSpec.WriteSql(writer)
		writer.Write(")")
	}
}

func (f *FunctionCall) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	f.WriteSql(writer)
	return writer.String()
}

// WindowSpecification represents OVER clause specifications
type WindowSpecification struct {
	PartitionBy []*SQLExpression
	OrderBy     []*OrderByItem
	FrameClause *FrameClause
}

func (w *WindowSpecification) WriteSql(writer *SQLWriter) {
	if len(w.PartitionBy) > 0 {
		writer.Write("PARTITION BY ")
		for i, expr := range w.PartitionBy {
			if i > 0 {
				writer.Write(", ")
			}
			expr.WriteSql(writer)
		}
	}

	if len(w.OrderBy) > 0 {
		if len(w.PartitionBy) > 0 {
			writer.Write(" ")
		}
		writer.Write("ORDER BY ")
		for i, item := range w.OrderBy {
			if i > 0 {
				writer.Write(", ")
			}
			item.WriteSql(writer)
		}
	}

	if w.FrameClause != nil {
		if len(w.PartitionBy) > 0 || len(w.OrderBy) > 0 {
			writer.Write(" ")
		}
		w.FrameClause.WriteSql(writer)
	}

}

// FrameClause represents window frame specifications
type FrameClause struct {
	Unit  string // ROWS, RANGE, GROUPS
	Start *FrameBound
	End   *FrameBound
}

func (f *FrameClause) WriteSql(writer *SQLWriter) {
	writer.Write(f.Unit)
	if f.End != nil {
		writer.Write(" BETWEEN ")
		f.Start.WriteSql(writer)
		writer.Write(" AND ")
		f.End.WriteSql(writer)
	} else {
		writer.Write(" ")
		f.Start.WriteSql(writer)
	}
}

// FrameBound represents frame boundary specifications
type FrameBound struct {
	Type   string // UNBOUNDED, CURRENT, PRECEDING, FOLLOWING
	Offset *SQLExpression
}

func (f *FrameBound) WriteSql(writer *SQLWriter) {
	if f.Offset != nil {
		f.Offset.WriteSql(writer)
		writer.Write(" ")
	}
	writer.Write(f.Type)
}

// SelectListItem represents an item in the SELECT clause
type SelectListItem struct {
	Expression      *SQLExpression
	Alias           string
	IsStarExpansion bool
	ExceptColumns   []string                  // For SELECT * EXCEPT
	ReplaceColumns  map[string]*SQLExpression // For SELECT * REPLACE
}

func (s *SelectListItem) WriteSql(writer *SQLWriter) {
	if s.IsStarExpansion {
		s.Expression.WriteSql(writer)
		if len(s.ExceptColumns) > 0 {
			writer.Write(" EXCEPT (")
			for i, col := range s.ExceptColumns {
				if i > 0 {
					writer.Write(", ")
				}
				writer.Write("`" + col + "`")
			}
			writer.Write(")")
		}
		if len(s.ReplaceColumns) > 0 {
			writer.Write(" REPLACE (")
			i := 0
			for col, expr := range s.ReplaceColumns {
				if i > 0 {
					writer.Write(", ")
				}
				expr.WriteSql(writer)
				writer.Write(" AS `" + col + "`")
				i++
			}
			writer.Write(")")
		}
	} else {
		s.Expression.WriteSql(writer)
		if s.Alias != "" && !(s.Expression.Type == ExpressionTypeColumn && s.Alias == s.Expression.Value) {
			writer.Write(" AS `" + s.Alias + "`")
		}
	}
}

func (s *SelectListItem) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	s.WriteSql(writer)
	return writer.String()
}

// TableReference represents a table reference in SQL
type TableReference struct {
	TableName string
	Alias     string
}

// FromItemType represents different types of FROM clause items
type FromItemType int

const (
	FromItemTypeTable FromItemType = iota
	FromItemTypeSubquery
	FromItemTypeJoin
	FromItemTypeWithRef
	FromItemTypeTableFunction
	FromItemTypeUnnest
	FromItemTypeSingleRow
)

// JoinType represents different types of JOINs
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
	JoinTypeCross
)

// FromItem represents items in the FROM clause
type FromItem struct {
	Type          FromItemType
	TableName     string
	Alias         string
	Subquery      *SelectStatement
	Join          *JoinClause
	WithRef       string
	TableFunction *TableFunction
	UnnestExpr    *SQLExpression
	Hints         []string
}

func (f *FromItem) WriteSql(writer *SQLWriter) {
	switch f.Type {
	case FromItemTypeTable:
		writer.Write("`" + f.TableName + "`")
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write("`" + f.Alias + "`")
		}
	case FromItemTypeSubquery:
		writer.Write("(\n")
		if f.Subquery != nil {
			writer.Indent()
			f.Subquery.WriteSql(writer)
			writer.Dedent()
		}
		writer.Write("\n)")
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write("`" + f.Alias + "`")
		}
	case FromItemTypeJoin:
		if f.Join != nil {
			f.Join.WriteSql(writer)
		}
	case FromItemTypeWithRef:
		writer.Write(f.WithRef)
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write("`" + f.Alias + "`")
		}
	case FromItemTypeTableFunction:
		if f.TableFunction != nil {
			f.TableFunction.WriteSql(writer)
		}
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write("`" + f.Alias + "`")
		}
	case FromItemTypeUnnest:
		writer.Write("UNNEST(")
		if f.UnnestExpr != nil {
			f.UnnestExpr.WriteSql(writer)
		}
		writer.Write(")")
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write("`" + f.Alias + "`")
		}
	}
}

func (f *FromItem) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = true
	f.WriteSql(writer)
	return writer.String()
}

// JoinClause represents JOIN operations
type JoinClause struct {
	Type      JoinType
	Left      *FromItem
	Right     *FromItem
	Condition *SQLExpression
	Using     []string
}

func (j *JoinClause) WriteSql(writer *SQLWriter) {
	if j.Left != nil {
		j.Left.WriteSql(writer)
	}

	switch j.Type {
	case JoinTypeInner:
		writer.Write(" INNER JOIN ")
	case JoinTypeLeft:
		writer.Write(" LEFT JOIN ")
	case JoinTypeRight:
		writer.Write(" RIGHT JOIN ")
	case JoinTypeFull:
		writer.Write(" FULL OUTER JOIN ")
	case JoinTypeCross:
		writer.Write(" CROSS JOIN ")
	}

	if j.Right != nil {
		j.Right.WriteSql(writer)
	}

	if j.Type != JoinTypeCross {
		if len(j.Using) > 0 {
			writer.Write(" USING (")
			for i, col := range j.Using {
				if i > 0 {
					writer.Write(", ")
				}
				writer.Write(col)
			}
			writer.Write(")")
		} else if j.Condition != nil {
			writer.Write(" ON ")
			j.Condition.WriteSql(writer)
		}
	}

}

// TableFunction represents table-valued functions
type TableFunction struct {
	Name      string
	Arguments []*SQLExpression
}

func (t *TableFunction) WriteSql(writer *SQLWriter) {
	writer.Write(t.Name)
	writer.Write("(")
	for i, arg := range t.Arguments {
		if i > 0 {
			writer.Write(", ")
		}
		arg.WriteSql(writer)
	}
	writer.Write(")")
}

// OrderByItem represents items in ORDER BY clause
type OrderByItem struct {
	Expression *SQLExpression
	Direction  string // ASC, DESC
	NullsOrder string // NULLS FIRST, NULLS LAST
}

func (o *OrderByItem) WriteSql(writer *SQLWriter) {
	o.Expression.WriteSql(writer)
	if o.Direction != "" {
		writer.Write(" ")
		writer.Write(o.Direction)
	}
	if o.NullsOrder != "" {
		writer.Write(" ")
		writer.Write(o.NullsOrder)
	}
}

func (o *OrderByItem) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	o.WriteSql(writer)
	return writer.String()
}

// WithClause represents CTE (Common Table Expression) definitions
type WithClause struct {
	Name         string
	Materialized bool
	Recursive    bool
	Columns      []string
	Query        *SelectStatement
}

func (w *WithClause) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	w.WriteSql(writer)
	return writer.String()
}

func (w *WithClause) WriteSql(writer *SQLWriter) {
	writer.Write("`" + w.Name + "`")
	if len(w.Columns) > 0 {
		writer.Write(" (")
		for i, col := range w.Columns {
			if i > 0 {
				writer.Write(", ")
			}
			writer.Write(col)
		}
		writer.Write(")")
	}
	writer.Write(" AS MATERIALIZED 	(")
	writer.WriteLine("")
	writer.Indent()
	if w.Query != nil {
		w.Query.WriteSql(writer)
	}
	writer.Dedent()
	writer.WriteLine(")")
}

// SetOperation represents UNION, INTERSECT, EXCEPT operations
type SetOperation struct {
	Type     string // UNION, INTERSECT, EXCEPT
	Modifier string // ALL, DISTINCT
	Items    []*SelectStatement
}

func (s *SetOperation) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	s.WriteSql(writer)
	return writer.String()
}

func (s *SetOperation) WriteSql(writer *SQLWriter) {
	for i := 0; i < len(s.Items); i++ {
		s.Items[i].WriteSql(writer)
		if i != len(s.Items)-1 {
			writer.WriteLine("")
			writer.Write(s.Type)
			if s.Modifier != "" {
				writer.Write(" ")
				writer.Write(s.Modifier)
			}
			writer.WriteLine("")
		}
	}
}

// SelectStatement represents the main SELECT statement structure
type SelectStatement struct {
	// WITH clause
	WithClauses []*WithClause

	// SELECT clause
	SelectType   SelectType
	SelectList   []*SelectListItem
	AsStructType string
	AsValueType  string

	// FROM clause
	FromClause *FromItem

	// WHERE clause
	WhereClause *SQLExpression

	// GROUP BY clause
	GroupByList []*SQLExpression

	// HAVING clause
	HavingClause *SQLExpression

	// ORDER BY clause
	OrderByList []*OrderByItem

	// LIMIT OFFSET clause
	LimitClause *LimitClause

	// Set operations
	SetOperation *SetOperation

	// Hints
	Hints []string
}

type LimitClause struct {
	Count  *SQLExpression
	Offset *SQLExpression
}

func (s *SelectStatement) WriteSql(writer *SQLWriter) {
	// WITH clause
	if len(s.WithClauses) > 0 {
		// Check if any WITH clause is recursive
		hasRecursive := false
		for _, withClause := range s.WithClauses {
			if withClause.Recursive {
				hasRecursive = true
				break
			}
		}

		if hasRecursive {
			writer.Write("WITH RECURSIVE ")
		} else {
			writer.Write("WITH ")
		}

		for i, withClause := range s.WithClauses {
			if i > 0 {
				writer.Write(", ")
				writer.WriteLine("")
			}
			withClause.WriteSql(writer)
		}
		writer.WriteLine("")
	}

	// SetOperations implement their own writer for SELECT (but use WithClauses, GroupBy, OrderBy)
	if s.SetOperation != nil {
		s.SetOperation.WriteSql(writer)
	} else {
		// SELECT clause
		switch s.SelectType {
		case SelectTypeDistinct:
			writer.Write("SELECT DISTINCT")
		case SelectTypeAll:
			writer.Write("SELECT ALL")
		case SelectTypeAsStruct:
			writer.Write("SELECT AS STRUCT")
		case SelectTypeAsValue:
			writer.Write("SELECT AS VALUE")
		default:
			writer.Write("SELECT")
		}

		if s.AsStructType != "" {
			writer.Write(" AS ")
			writer.Write(s.AsStructType)
		}

		if len(s.SelectList) > 0 {
			writer.WriteLine("")
			writer.Indent()
			for i, item := range s.SelectList {
				if i > 0 {
					writer.Write(",")
					writer.WriteLine("")
				}
				item.WriteSql(writer)
			}
			writer.Dedent()
		}

		// FROM clause
		if s.FromClause != nil && s.FromClause.Type != FromItemTypeSingleRow {
			writer.WriteLine("")
			writer.Write("FROM ")
			s.FromClause.WriteSql(writer)
		}
	}

	// WHERE clause
	if s.WhereClause != nil {
		writer.WriteLine("")
		writer.Write("WHERE ")
		s.WhereClause.WriteSql(writer)
	}

	// GROUP BY clause
	if len(s.GroupByList) > 0 {
		writer.WriteLine("")
		writer.Write("GROUP BY ")
		for i, expr := range s.GroupByList {
			if i > 0 {
				writer.Write(", ")
			}
			expr.WriteSql(writer)
		}
	}

	// HAVING clause
	if s.HavingClause != nil {
		writer.WriteLine("")
		writer.Write("HAVING ")
		s.HavingClause.WriteSql(writer)
	}

	// ORDER BY clause
	if len(s.OrderByList) > 0 {
		writer.WriteLine("")
		writer.Write("ORDER BY ")
		for i, item := range s.OrderByList {
			if i > 0 {
				writer.Write(", ")
			}
			item.WriteSql(writer)
		}
	}

	// LIMIT clause
	if s.LimitClause != nil {
		writer.WriteLine("")
		writer.Write("LIMIT ")
		s.LimitClause.Count.WriteSql(writer)

		if s.LimitClause.Offset != nil {
			writer.Write(" OFFSET ")
			s.LimitClause.Offset.WriteSql(writer)
		}
	}

}

func (s *SelectStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// CreateTableStatement WriteSql implementation
func (s *CreateTableStatement) WriteSql(writer *SQLWriter) {
	writer.Write("CREATE TABLE")
	if s.IfNotExists {
		writer.Write(" IF NOT EXISTS")
	}
	writer.Write(" ")
	writer.Write("`" + s.TableName + "`")

	if s.AsSelect != nil {
		writer.Write(" AS ")
		s.AsSelect.WriteSql(writer)
		return
	}

	writer.Write(" (")
	writer.WriteLine("")
	writer.Indent()
	for i, col := range s.Columns {
		if i > 0 {
			writer.Write(",")
			writer.WriteLine("")
		}
		col.WriteSql(writer)
	}
	writer.Dedent()
	writer.WriteLine("")
	writer.Write(")")
}

func (s *CreateTableStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// ColumnDefinition WriteSql implementation
func (c *ColumnDefinition) WriteSql(writer *SQLWriter) {
	writer.Write("`" + c.Name + "`")
	writer.Write(" ")
	writer.Write(c.Type)
	if c.NotNull {
		writer.Write(" NOT NULL")
	}
	if c.IsPrimaryKey {
		writer.Write(" PRIMARY KEY")
	}
	if c.DefaultValue != nil {
		writer.Write(" DEFAULT ")
		c.DefaultValue.WriteSql(writer)
	}
}

func (c *ColumnDefinition) String() string {
	writer := NewSQLWriter()
	c.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// CreateViewStatement WriteSql implementation
func (s *CreateViewStatement) WriteSql(writer *SQLWriter) {
	writer.Write("CREATE VIEW")
	if s.IfNotExists {
		writer.Write(" IF NOT EXISTS")
	}
	writer.Write(" `" + s.ViewName + "` AS ")
	s.Query.WriteSql(writer)
}

func (s *CreateViewStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// CreateFunctionStatement WriteSql implementation
func (s *CreateFunctionStatement) WriteSql(writer *SQLWriter) {
	writer.Write("CREATE FUNCTION")
	if s.IfNotExists {
		writer.Write(" IF NOT EXISTS")
	}
	writer.Write(" ")
	writer.Write("`" + s.FunctionName + "`")
	writer.Write("(")
	for i, param := range s.Parameters {
		if i > 0 {
			writer.Write(", ")
		}
		param.WriteSql(writer)
	}
	writer.Write(")")
	if s.ReturnType != "" {
		writer.Write(" RETURNS ")
		writer.Write(s.ReturnType)
	}
	if s.Language != "" {
		writer.Write(" LANGUAGE ")
		writer.Write(s.Language)
	}
	if s.Code != "" {
		writer.Write(" AS ")
		writer.Write(s.Code)
	}
}

func (s *CreateFunctionStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// ParameterDefinition WriteSql implementation
func (p *ParameterDefinition) WriteSql(writer *SQLWriter) {
	writer.Write("`" + p.Name + "`")
	writer.Write(" ")
	writer.Write(p.Type)
}

func (p *ParameterDefinition) String() string {
	writer := NewSQLWriter()
	p.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// DropStatement WriteSql implementation
func (s *DropStatement) WriteSql(writer *SQLWriter) {
	writer.Write("DROP ")
	writer.Write(s.ObjectType)
	if s.IfExists {
		writer.Write(" IF EXISTS")
	}
	writer.Write(" ")
	writer.Write("`" + s.ObjectName + "`")
}

func (s *DropStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// TruncateStatement WriteSql implementation
func (s *TruncateStatement) WriteSql(writer *SQLWriter) {
	writer.Write("TRUNCATE TABLE ")
	writer.Write("`" + s.TableName + "`")
}

func (s *TruncateStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// SetItem WriteSql implementation
func (s *SetItem) WriteSql(writer *SQLWriter) {
	s.Column.WriteSql(writer)
	writer.Write(" = ")
	s.Value.WriteSql(writer)
}

func (s *SetItem) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// Builder helper functions

// NewSelectStatement creates a new SELECT statement
func NewSelectStatement() *SelectStatement {
	return &SelectStatement{
		SelectType: SelectTypeStandard,
	}
}

func NewSelectStarStatement(from *FromItem) *SelectStatement {
	return &SelectStatement{
		SelectType: SelectTypeStandard,
		FromClause: from,
		SelectList: []*SelectListItem{
			{
				Expression: NewStarExpression(),
			},
		},
	}
}

// NewColumnExpression creates a new column reference expression
func NewColumnExpression(column string, tableAlias ...string) *SQLExpression {
	expr := &SQLExpression{
		Type:  ExpressionTypeColumn,
		Value: column,
	}
	if len(tableAlias) > 0 {
		expr.TableAlias = tableAlias[0]
	}
	return expr
}

// NewStarExpression creates a new star (*) expression for SELECT *
func NewStarExpression(tableAlias ...string) *SQLExpression {
	expr := &SQLExpression{
		Type: ExpressionTypeStar,
	}
	if len(tableAlias) > 0 {
		expr.TableAlias = tableAlias[0]
	}
	return expr
}

// NewLiteralExpression creates a new literal expression
func NewLiteralExpression(value string) *SQLExpression {
	return &SQLExpression{
		Type:  ExpressionTypeLiteral,
		Value: value,
	}
}

func NewLiteralExpressionFromGoValue(t types.Type, value interface{}) (*SQLExpression, error) {
	encoded, err := ValueFromGoValue(value)
	if err != nil {
		return nil, err
	}
	literal, err := LiteralFromValue(encoded)
	if err != nil {
		return nil, err
	}
	return NewLiteralExpression(literal), nil
}

// NewFunctionExpression creates a new function call expression
func NewFunctionExpression(name string, args ...*SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeFunction,
		FunctionCall: &FunctionCall{
			Name:      name,
			Arguments: args,
		},
	}
}

// NewBinaryExpression creates a new binary expression
func NewBinaryExpression(left *SQLExpression, operator string, right *SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeBinary,
		BinaryExpression: &BinaryExpression{
			Left:     left,
			Operator: operator,
			Right:    right,
		},
	}
}

// NewCaseExpression creates a new CASE expression (searched case)
func NewCaseExpression(whenClauses []*WhenClause, elseExpr *SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeCase,
		CaseExpression: &CaseExpression{
			WhenClauses: whenClauses,
			ElseExpr:    elseExpr,
		},
	}
}

// NewSimpleCaseExpression creates a new CASE expression with a case expression (simple case)
func NewSimpleCaseExpression(caseExpr *SQLExpression, whenClauses []*WhenClause, elseExpr *SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeCase,
		CaseExpression: &CaseExpression{
			CaseExpr:    caseExpr,
			WhenClauses: whenClauses,
			ElseExpr:    elseExpr,
		},
	}
}

// NewSubqueryFromItem creates a subquery FROM item
func NewSubqueryFromItem(subquery *SelectStatement, alias string) *FromItem {
	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: subquery,
		Alias:    alias,
	}
}

// NewListExpression creates a new list expression
func NewListExpression(expressions []*SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeList,
		ListExpression: &ListExpression{
			Expressions: expressions,
		},
	}
}

// NewNotExpression creates a new NOT expression
func NewNotExpression(expression *SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeUnary,
		UnaryExpression: &UnaryExpression{
			Operator:   "NOT",
			Expression: expression,
		},
	}
}

// NewExistsExpression creates a new EXISTS expression
func NewExistsExpression(subquery *SelectStatement) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeExists,
		ExistsExpr: &ExistsExpression{
			Subquery: subquery,
		},
	}
}

// NewInnerJoin creates an INNER JOIN
func NewInnerJoin(left, right *FromItem, condition *SQLExpression) *FromItem {
	return &FromItem{
		Type: FromItemTypeJoin,
		Join: &JoinClause{
			Type:      JoinTypeInner,
			Left:      left,
			Right:     right,
			Condition: condition,
		},
	}
}

// DDL Statement types

type CreateTableStatement struct {
	IfNotExists bool
	TableName   string
	Columns     []*ColumnDefinition
	AsSelect    *SelectStatement
}

type ColumnDefinition struct {
	Name         string
	Type         string
	NotNull      bool
	DefaultValue *SQLExpression
	IsPrimaryKey bool
}

type CreateViewStatement struct {
	IfNotExists bool
	ViewName    string
	Query       SQLFragment
}

type CreateFunctionStatement struct {
	IfNotExists  bool
	FunctionName string
	Parameters   []*ParameterDefinition
	ReturnType   string
	Language     string
	Code         string
	Options      map[string]*SQLExpression
}

type ParameterDefinition struct {
	Name string
	Type string
}

type DeleteStatement struct {
	Table     SQLFragment
	WhereExpr SQLFragment
}

func (d *DeleteStatement) String() string {
	writer := NewSQLWriter()
	d.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

func (d *DeleteStatement) WriteSql(writer *SQLWriter) {
	writer.Write("DELETE FROM ")
	d.Table.WriteSql(writer)
	if d.WhereExpr != nil {
		writer.Write(" WHERE ")
		writer.Write(d.WhereExpr.String())
	}
}

type InsertStatement struct {
	TableName string
	Columns   []string
	Query     *SelectStatement
	Rows      []SQLFragment
}

func (d *InsertStatement) String() string {
	writer := NewSQLWriter()
	d.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

func (d *InsertStatement) WriteSql(writer *SQLWriter) {
	writer.Write("INSERT INTO ")
	writer.WriteLine("`" + d.TableName + "`")
	writer.WriteLine(" (" + strings.Join(d.Columns, ", ") + ") ")
	if d.Query != nil {
		writer.Write(" ")
		d.Query.WriteSql(writer)
	} else if len(d.Rows) > 0 {
		writer.WriteLine("VALUES ")
		for i, value := range d.Rows {
			writer.Write("(" + value.String() + ")")
			if len(d.Rows) != 1 && i != len(d.Rows)-1 {
				writer.Write(",")
			}
		}
	}
}

type DropStatement struct {
	IfExists   bool
	ObjectType string
	ObjectName string
}

type TruncateStatement struct {
	TableName string
}

type UpdateStatement struct {
	Table       *FromItem
	SetItems    []*SetItem
	FromClause  *FromItem
	WhereClause *SQLExpression
}

func (u *UpdateStatement) WriteSql(writer *SQLWriter) {
	writer.Write("UPDATE ")
	u.Table.WriteSql(writer)
	writer.Write(" SET ")
	for i, item := range u.SetItems {
		if i > 0 {
			writer.Write(", ")
		}
		item.WriteSql(writer)
	}
	if u.FromClause != nil {
		writer.Write(" FROM ")
		u.FromClause.WriteSql(writer)
	}
	if u.WhereClause != nil {
		writer.Write(" WHERE ")
		u.WhereClause.WriteSql(writer)
	}
}

func (u *UpdateStatement) String() string {
	writer := NewSQLWriter()
	u.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

type SetItem struct {
	Column *SQLExpression
	Value  *SQLExpression
}

// CompoundSQLFragment represents multiple SQL statements that should be executed in sequence
type CompoundSQLFragment struct {
	statements []string
}

// NewCompoundSQLFragment creates a new compound SQL fragment
func NewCompoundSQLFragment(statements []string) *CompoundSQLFragment {
	return &CompoundSQLFragment{
		statements: statements,
	}
}

// String returns the compound fragment as a collection of statements
// Note: This is primarily for compatibility - the actual execution will handle each statement separately
func (c *CompoundSQLFragment) String() string {
	return strings.Join(c.statements, ";\n") + ";"
}

// WriteSql writes the compound fragment to a SQL writer
func (c *CompoundSQLFragment) WriteSql(writer *SQLWriter) {
	for i, stmt := range c.statements {
		if i > 0 {
			writer.Write(";\n")
		}
		writer.Write(stmt)
	}
}

// GetStatements returns the individual statements in the compound fragment
func (c *CompoundSQLFragment) GetStatements() []string {
	return c.statements
}
