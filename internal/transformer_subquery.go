package internal

import (
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// SubqueryTransformer handles transformation of subquery expressions from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, subqueries can appear in various expression contexts with different
// semantics: scalar subqueries (single value), array subqueries, EXISTS checks, and IN checks.
// Each type has specific behavior and return value expectations.
//
// The transformer converts ZetaSQL subquery expressions by:
// - Recursively transforming the subquery's scan structure
// - Wrapping the result in appropriate SQL constructs based on subquery type:
//   - Scalar: Returns single value, wrapped in parentheses
//   - Array: Wrapped with zetasqlite_array() for proper array semantics
//   - EXISTS: Wrapped in EXISTS(...) boolean expression
//   - IN: Combined with IN expression for membership testing
//
// Subqueries preserve their own column scoping and fragment context while being
// embedded as expressions in the parent query.
type SubqueryTransformer struct {
	coordinator Coordinator // For recursive transformation of the subquery
}

// NewSubqueryTransformer creates a new subquery transformer
func NewSubqueryTransformer(coordinator Coordinator) *SubqueryTransformer {
	return &SubqueryTransformer{
		coordinator: coordinator,
	}
}

// Transform converts SubqueryData to SQLExpression
func (t *SubqueryTransformer) Transform(data ExpressionData, ctx TransformContext) (*SQLExpression, error) {
	if data.Type != ExpressionTypeSubquery || data.Subquery == nil {
		return nil, fmt.Errorf("expected subquery expression data, got type %v", data.Type)
	}

	subquery := data.Subquery

	// Transform the subquery scan into a SELECT statement
	subqueryFragment, err := t.coordinator.TransformScan(subquery.Query, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform subquery scan: %w", err)
	}

	expression := &SQLExpression{
		Type:     ExpressionTypeSubquery,
		Subquery: NewSelectStarStatement(subqueryFragment),
	}

	// Wrap the subquery in parentheses and handle the subquery type
	switch subquery.SubqueryType {
	case ast.SubqueryTypeScalar:
	case ast.SubqueryTypeArray:
		if len(subquery.Query.ColumnList) == 0 {
			return nil, fmt.Errorf("failed to find computed column names for array subquery")
		}
		selectStatement := NewSelectStatement()
		selectStatement.SelectList = []*SelectListItem{
			{
				Expression: NewFunctionExpression(
					"zetasqlite_array",
					ctx.FragmentContext().GetQualifiedColumnExpression(subquery.Query.ColumnList[0].ID),
				),
			},
		}
		selectStatement.FromClause = subqueryFragment
		expression.Subquery = selectStatement
	case ast.SubqueryTypeExists:
		// EXISTS subquery: EXISTS (SELECT ...)
		return NewExistsExpression(NewSelectStarStatement(subqueryFragment)), nil
	case ast.SubqueryTypeIn:
		// IN subquery: expr IN (SELECT ...)
		inExpr, err := t.coordinator.TransformExpression(*subquery.InExpr, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform IN expression: %w", err)
		}

		return NewBinaryExpression(
			inExpr,
			"IN",
			expression,
		), nil

	}

	return expression, nil
}
