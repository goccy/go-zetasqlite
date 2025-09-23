package internal

import (
	"fmt"
)

// ColumnRefTransformer handles transformation of column reference expressions from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, column references can appear in various contexts (SELECT lists, WHERE clauses,
// ORDER BY, etc.) and may need qualified names to resolve ambiguity in complex queries with joins,
// subqueries, or CTEs. The ZetaSQL analyzer resolves these references to specific column IDs.
//
// The transformer converts ZetaSQL ColumnRef nodes into SQLite column references with:
// - Proper qualification using table aliases when needed
// - Column name resolution through fragment context
// - ID-based lookup for disambiguation in complex nested queries
//
// The fragment context maintains the mapping between column IDs and their qualified names,
// ensuring that column references work correctly across subquery boundaries and joins.
type ColumnRefTransformer struct {
	coordinator Coordinator
}

// NewColumnRefTransformer creates a new column reference transformer
func NewColumnRefTransformer(coordinator Coordinator) *ColumnRefTransformer {
	return &ColumnRefTransformer{
		coordinator: coordinator,
	}
}

// Transform converts ColumnRefData to SQLExpression
func (t *ColumnRefTransformer) Transform(data ExpressionData, ctx TransformContext) (*SQLExpression, error) {
	if data.Type != ExpressionTypeColumn || data.Column == nil {
		return nil, fmt.Errorf("expected column reference data, got type %v", data.Type)
	}

	columnData := data.Column
	return ctx.FragmentContext().GetQualifiedColumnExpression(columnData.ColumnID), nil
}
