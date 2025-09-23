package internal

import (
	"fmt"
)

// LimitScanTransformer handles LIMIT/OFFSET scan transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, LIMIT scans control the number of rows returned from a query,
// optionally with an OFFSET to skip rows. This corresponds to SQL's LIMIT and OFFSET
// clauses that restrict result set size for pagination and performance.
//
// The transformer converts ZetaSQL LimitScan nodes into SQLite LIMIT clauses by:
// - Recursively transforming the input scan to get the data source
// - Transforming count and offset expressions through the coordinator
// - Wrapping the result in SELECT * FROM (...) LIMIT count OFFSET offset
// - Preserving the original column structure and availability
//
// Both count and offset can be dynamic expressions (parameters, column references, etc.)
// rather than just literal numbers, requiring full expression transformation.
type LimitScanTransformer struct {
	coordinator Coordinator // For recursive transformation of the inner scan
}

// NewLimitScanTransformer creates a new limit scan transformer
func NewLimitScanTransformer(coordinator Coordinator) *LimitScanTransformer {
	return &LimitScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts LimitScanData to FromItem with LIMIT clause
func (t *LimitScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeLimit || data.LimitScan == nil {
		return nil, fmt.Errorf("expected limit scan data, got type %v", data.Type)
	}

	limitScanData := data.LimitScan

	innerFromItem, err := t.coordinator.TransformScan(limitScanData.InputScan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform inner scan in limit: %w", err)
	}

	return t.wrapInSubqueryWithLimit(innerFromItem, limitScanData, ctx)
}

// addLimitToSelect adds LIMIT clause to an existing SELECT statement
func (t *LimitScanTransformer) addLimitToSelect(selectStmt *SelectStatement, limitData *LimitScanData, ctx TransformContext) (*FromItem, error) {
	// Transform count expression
	countExpr, err := t.coordinator.TransformExpression(limitData.Count, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform limit count: %w", err)
	}

	// Create limit clause
	limitClause := &LimitClause{
		Count: countExpr,
	}

	// Transform offset if present
	if limitData.Offset != (ExpressionData{}) {
		offsetExpr, err := t.coordinator.TransformExpression(limitData.Offset, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform limit offset: %w", err)
		}
		limitClause.Offset = offsetExpr
	}

	// Add limit clause to the existing SELECT
	selectStmt.LimitClause = limitClause

	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: selectStmt,
		Alias:    fmt.Sprintf("limit_scan_%s", ctx.FragmentContext().GetID()),
	}, nil
}

// wrapInSubqueryWithLimit wraps complex from items in a subquery with LIMIT
func (t *LimitScanTransformer) wrapInSubqueryWithLimit(fromItem *FromItem, limitData *LimitScanData, ctx TransformContext) (*FromItem, error) {
	// Create SELECT * FROM (complex_query) LIMIT count OFFSET offset
	selectStmt := NewSelectStarStatement(fromItem)

	// Add the limit clause
	return t.addLimitToSelect(selectStmt, limitData, ctx)
}
