package internal

import (
	"fmt"
)

// SetOperationScanTransformer handles set operation transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, set operations combine multiple SELECT statements using UNION,
// INTERSECT, or EXCEPT operators. These operations can have ALL or DISTINCT modifiers
// and require compatible column schemas across all operands.
//
// The transformer converts ZetaSQL SetOperationScan nodes by:
// - Recursively transforming each operand statement through the coordinator
// - Creating a SetOperation structure with proper type and modifier
// - Moving WITH clauses from operands to the top level for proper scoping
// - Wrapping the result in a subquery to establish new column mappings
// - Ensuring column compatibility and proper aliasing across operands
//
// Set operations follow SQL's standard precedence and evaluation rules, with
// UNION having the lowest precedence and operations being left-associative.
type SetOperationScanTransformer struct {
	coordinator Coordinator
}

// NewSetOperationScanTransformer creates a new set operation scan transformer
func NewSetOperationScanTransformer(coordinator Coordinator) *SetOperationScanTransformer {
	return &SetOperationScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts SetOperationData to FromItem with set operation structure
func (t *SetOperationScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeSetOp || data.SetOperationScan == nil {
		return nil, fmt.Errorf("expected set operation scan data, got type %v", data.Type)
	}

	setOpData := data.SetOperationScan

	// Transform all statement items to SelectStatements
	selectStatements := make([]*SelectStatement, 0, len(setOpData.Items))
	for i, stmtData := range setOpData.Items {
		stmt, err := t.coordinator.TransformStatement(stmtData, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform set operation statement %d: %w", i, err)
		}

		// Ensure it's a SELECT statement
		selectStmt, ok := stmt.(*SelectStatement)
		if !ok {
			return nil, fmt.Errorf("set operation item %d is not a SELECT statement", i)
		}

		selectStatements = append(selectStatements, selectStmt)
	}

	// Create the set operation
	setOperation := &SetOperation{
		Type:     setOpData.Type,
		Modifier: setOpData.Modifier,
		Items:    selectStatements,
	}

	// Create a SELECT statement that contains the set operation
	setStatement := NewSelectStatement()
	setStatement.SetOperation = setOperation

	// Move all WITH queries from items to top-level
	for _, item := range setOperation.Items {
		setStatement.WithClauses = append(setStatement.WithClauses, item.WithClauses...)
		item.WithClauses = item.WithClauses[:0] // Clear the WITH clauses from the items
	}

	// Create a subquery to introduce the new logical columns into the query
	statement := NewSelectStatement()
	for i, column := range data.ColumnList {
		// Retrieve column name from the first item
		itemColumn := setOperation.Items[0].SelectList[i]
		statement.SelectList = append(statement.SelectList, &SelectListItem{
			Expression: NewColumnExpression(itemColumn.Alias),
			Alias:      generateIDBasedAlias(column.Name, column.ID),
		})
	}
	statement.FromClause = NewSubqueryFromItem(setStatement, "set_op_scan_inner")

	// Wrap in a subquery FromItem
	return NewSubqueryFromItem(statement, "set_op_scan_outer"), nil
}
