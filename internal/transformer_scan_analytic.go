package internal

import (
	"fmt"
)

// AnalyticScanTransformer handles analytic scan (window function) transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, analytic scans represent window functions that compute values over
// a set of rows related to the current row. This includes functions like ROW_NUMBER(),
// RANK(), LAG(), LEAD(), SUM() OVER(), etc. with PARTITION BY and ORDER BY clauses.
//
// The transformer converts ZetaSQL AnalyticScan nodes by:
// - Recursively transforming the input scan that provides the base data
// - Pre-transforming all window function expressions before column registration
// - Creating SELECT list with both passthrough columns and computed window functions
// - Extracting ORDER BY clauses from window specifications for proper result ordering
// - Ensuring proper column qualification and fragment context management
//
// Window functions require careful ordering to ensure correct evaluation, which is
// preserved through ORDER BY clauses derived from the PARTITION BY and ORDER BY
// specifications in the window function definitions.
type AnalyticScanTransformer struct {
	coordinator Coordinator
}

// NewAnalyticScanTransformer creates a new analytic scan transformer
func NewAnalyticScanTransformer(coordinator Coordinator) *AnalyticScanTransformer {
	return &AnalyticScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts AnalyticScanData to a FromItem representing window function operations
func (t *AnalyticScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeAnalytic || data.AnalyticScan == nil {
		return nil, fmt.Errorf("expected analytic scan data, got type %v", data.Type)
	}

	analyticData := data.AnalyticScan

	// Transform the input scan recursively
	inputFromItem, err := t.coordinator.TransformScan(analyticData.InputScan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform input scan for analytic: %w", err)
	}

	// Map to track which columns come from functions vs input
	functionColumns := make(map[int]*ComputedColumnData)
	for _, funcData := range analyticData.FunctionList {
		functionColumns[int(funcData.Column.ColumnID())] = funcData
	}

	// Pre-transform all window function expressions BEFORE registering output columns
	// This ensures window function references resolve to input columns, not output columns
	transformedFunctions := make(map[int]*SQLExpression)
	for _, funcData := range analyticData.FunctionList {
		expr, err := t.coordinator.TransformExpression(funcData.Expression, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform window function: %w", err)
		}
		transformedFunctions[int(funcData.Column.ColumnID())] = expr
	}

	// Create a SELECT statement with the window functions
	selectStmt := NewSelectStatement()
	selectStmt.FromClause = inputFromItem

	// Add all output columns to the SELECT list
	selectList := make([]*SelectListItem, 0, len(data.ColumnList))

	for _, col := range data.ColumnList {
		var expr *SQLExpression

		alias := generateIDBasedAlias(col.Name, col.ID)

		// Check if this column is from a window function
		if _, isFunction := functionColumns[col.ID]; isFunction {
			// Use pre-transformed function expression
			expr = transformedFunctions[col.ID]
		} else {
			expr = ctx.FragmentContext().GetQualifiedColumnExpression(col.ID)
		}

		// Create select list item
		item := &SelectListItem{
			Expression: expr,
			Alias:      alias,
		}
		selectList = append(selectList, item)

	}

	selectStmt.SelectList = selectList

	// Extract ORDER BY clause from window function specifications to ensure proper result ordering
	// This preserves the PARTITION BY + ORDER BY semantics from the window functions
	orderByItems := make([]*OrderByItem, 0)

	for _, funcData := range analyticData.FunctionList {
		if funcData.Expression.Type == ExpressionTypeFunction && funcData.Expression.Function != nil && funcData.Expression.Function.WindowSpec != nil {
			windowSpec := funcData.Expression.Function.WindowSpec

			// Add PARTITION BY expressions first (use unqualified column names with zetasqlite_collate)
			for _, partData := range windowSpec.PartitionBy {
				if partData.Type == ExpressionTypeColumn && partData.Column != nil {
					expr := ctx.FragmentContext().GetQualifiedColumnExpression(partData.Column.ColumnID)
					expr.Collation = "zetasqlite_collate"
					orderByItems = append(orderByItems, &OrderByItem{
						Expression: expr,
						Direction:  "ASC",
					})
				}
			}

			// Add ORDER BY expressions (use unqualified column names with zetasqlite_collate)
			for _, orderData := range windowSpec.OrderBy {
				if orderData.Expression.Type == ExpressionTypeColumn && orderData.Expression.Column != nil {
					expr := ctx.FragmentContext().GetQualifiedColumnExpression(orderData.Expression.Column.ColumnID)
					direction := "ASC"
					if orderData.IsDescending {
						direction = "DESC"
					}
					expr.Collation = "zetasqlite_collate"
					orderByItems = append(orderByItems, &OrderByItem{
						Expression: expr,
						Direction:  direction,
					})
				}
			}
		}
	}

	// Add ORDER BY clause if we have ordering expressions
	if len(orderByItems) > 0 {
		selectStmt.OrderByList = orderByItems
	}

	// Return as a subquery FROM item
	return NewSubqueryFromItem(selectStmt, ""), nil
}
