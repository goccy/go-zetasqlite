package internal

import (
	"fmt"
)

// QueryStmtTransformer handles transformation of QueryStmt nodes from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a QueryStmt represents the outermost SELECT statement in a query,
// containing the final SELECT list that defines the output columns and their aliases.
// This is the top-level entry point for transforming complete SQL queries.
//
// The transformer converts ZetaSQL QueryStmt nodes by:
// - Recursively transforming the main query scan (FROM clause) through the coordinator
// - Transforming each output column expression in the SELECT list
// - Preserving column aliases as specified in the original query
// - Creating the final SelectStatement structure for SQL generation
//
// This transformer bridges the gap between ZetaSQL's resolved AST structure and
// the SQLite SELECT statement representation, ensuring all query components are
// properly transformed and integrated.
type QueryStmtTransformer struct {
	coordinator Coordinator // For recursive transformation of the inner scan and output columns
}

// NewQueryStmtTransformer creates a new query statement transformer
func NewQueryStmtTransformer(coordinator Coordinator) *QueryStmtTransformer {
	return &QueryStmtTransformer{
		coordinator: coordinator,
	}
}

// Transform converts QueryStmt data to SelectStatement
func (t *QueryStmtTransformer) Transform(data StatementData, ctx TransformContext) (SQLFragment, error) {
	if data.Type != StatementTypeSelect || data.Select == nil {
		return nil, fmt.Errorf("expected select statement data for query stmt, got type %v", data.Type)
	}

	selectData := data.Select

	fromItem, err := t.coordinator.TransformScan(*selectData.FromClause, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform query scan: %w", err)
	}

	// Create the SELECT statement
	selectStatement := NewSelectStatement()
	selectStatement.FromClause = fromItem

	// Transform each output column using pure data
	for i, outputItem := range selectData.SelectList {
		// Transform the output column expression using pure data
		expr, err := t.coordinator.TransformExpression(outputItem.Expression, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform output column %d: %w", i, err)
		}

		// Create select list item with alias
		selectListItem := &SelectListItem{
			Expression: expr,
			Alias:      outputItem.Alias,
		}

		selectStatement.SelectList = append(selectStatement.SelectList, selectListItem)
	}

	return selectStatement, nil
}
