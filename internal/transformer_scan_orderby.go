package internal

import (
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// OrderByScanTransformer handles ORDER BY scan transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, ORDER BY scans sort result rows based on one or more expressions.
// This includes complex sorting semantics like NULLS FIRST/LAST, collation handling,
// and expressions that can reference columns, functions, or computed values.
//
// The transformer converts ZetaSQL OrderByScan nodes into SQLite ORDER BY clauses by:
// - Recursively transforming the input scan to get the data source
// - Transforming each ORDER BY expression through the coordinator
// - Handling ZetaSQL's NULL ordering semantics (NULLS FIRST/LAST) via additional sort keys
// - Applying zetasqlite_collate for consistent string ordering behavior
// - Creating SELECT * FROM (...) ORDER BY structure for complex queries
//
// ZetaSQL's NULL ordering is more sophisticated than SQLite's default behavior,
// requiring additional ORDER BY items to ensure consistent results.
type OrderByScanTransformer struct {
	coordinator Coordinator // For recursive transformation of the inner scan
}

// NewOrderByScanTransformer creates a new order by scan transformer
func NewOrderByScanTransformer(coordinator Coordinator) *OrderByScanTransformer {
	return &OrderByScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts OrderByScanData to FromItem with ORDER BY clause
func (t *OrderByScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeOrderBy || data.OrderByScan == nil {
		return nil, fmt.Errorf("expected order by scan data, got type %v", data.Type)
	}

	orderByScanData := data.OrderByScan

	innerFromItem, err := t.coordinator.TransformScan(orderByScanData.InputScan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform inner scan in order by: %w", err)
	}

	// Transform ORDER BY expressions
	orderByItems, err := t.transformOrderByItems(orderByScanData.OrderByColumns, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform order by items: %w", err)
	}

	selectStatement := NewSelectStatement()
	selectStatement.FromClause = innerFromItem
	selectStatement.OrderByList = orderByItems

	// Select the ColumnList explicitly (rather than SELECT *), as this scan should drop used `$orderby` columns
	// from the output columns
	for _, col := range data.ColumnList {
		selectStatement.SelectList = append(selectStatement.SelectList, &SelectListItem{
			Expression: ctx.FragmentContext().GetQualifiedColumnExpression(col.ID),
			Alias:      generateIDBasedAlias(col.Name, col.ID),
		})
	}

	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: selectStatement,
	}, nil
}

// transformOrderByItems converts OrderByItemData to OrderByItem
func (t *OrderByScanTransformer) transformOrderByItems(items []*OrderByItemData, ctx TransformContext) ([]*OrderByItem, error) {
	orderByItems := make([]*OrderByItem, 0, len(items))

	for _, itemData := range items {
		// Transform the expression
		expr, err := t.coordinator.TransformExpression(itemData.Expression, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform order by expression: %w", err)
		}

		// Handle potential multiple items for NULL ordering
		items, err := t.createOrderByItems(expr, itemData)
		if err != nil {
			return nil, fmt.Errorf("failed to create order by items: %w", err)
		}

		orderByItems = append(orderByItems, items...)
	}

	return orderByItems, nil
}

// createOrderByItems handles NULL ordering by potentially creating multiple ORDER BY items
func (t *OrderByScanTransformer) createOrderByItems(expr *SQLExpression, itemData *OrderByItemData) ([]*OrderByItem, error) {
	// Apply zetasqlite_collate collation to the expression
	expr.Collation = "zetasqlite_collate"

	items := make([]*OrderByItem, 0)

	// Handle NULL ordering if specified by creating additional ORDER BY items
	if itemData.NullOrder != ast.NullOrderModeOrderUnspecified {
		// Create IS NOT NULL expression for null ordering
		nullExpr := NewBinaryExpression(
			expr,
			"IS NOT",
			NewLiteralExpression("NULL"),
		)
		nullExpr.Collation = "zetasqlite_collate"

		switch itemData.NullOrder {
		case ast.NullOrderModeNullsFirst:
			// For NULLS FIRST, order by (expr IS NOT NULL) ASC first
			items = append(items, &OrderByItem{
				Direction:  "ASC",
				Expression: nullExpr,
			})
		case ast.NullOrderModeNullsLast:
			// For NULLS LAST, order by (expr IS NOT NULL) DESC first
			items = append(items, &OrderByItem{
				Direction:  "DESC",
				Expression: nullExpr,
			})
		}
	}

	// Create the main column ordering item
	columnItem := &OrderByItem{
		Expression: expr,
		Direction:  "ASC",
	}

	if itemData.IsDescending {
		columnItem.Direction = "DESC"
	}

	items = append(items, columnItem)

	return items, nil
}

// createOrderByItems creates ORDER BY items with proper NULL handling
func createOrderByItems(expr *SQLExpression, orderData *OrderByItemData) ([]*OrderByItem, error) {
	items := make([]*OrderByItem, 0)

	// Handle NULL ordering if specified
	if orderData.NullOrder != 0 { // Assuming NullOrderModeOrderUnspecified = 0
		nullExpr := &SQLExpression{
			Type: ExpressionTypeBinary,
			BinaryExpression: &BinaryExpression{
				Left:     expr,
				Operator: "IS NOT",
				Right:    NewLiteralExpression("NULL"),
			},
		}
		nullExpr.Collation = "zetasqlite_collate"

		// Add null handling ORDER BY item first
		direction := "ASC"
		if orderData.NullOrder == 2 { // Assuming NullOrderModeNullsLast = 2
			direction = "DESC"
		}

		items = append(items, &OrderByItem{
			Expression: nullExpr,
			Direction:  direction,
		})
	}

	// Add the main ORDER BY item
	direction := "ASC"
	if orderData.IsDescending {
		direction = "DESC"
	}

	expr.Collation = "zetasqlite_collate"
	items = append(items, &OrderByItem{
		Expression: expr,
		Direction:  direction,
	})

	return items, nil
}
