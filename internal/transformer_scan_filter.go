package internal

import (
	"fmt"
)

// FilterScanTransformer handles WHERE clause filter transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a FilterScan represents SQL WHERE clause operations that filter
// rows from an input scan based on boolean expressions. This corresponds to row-level
// filtering that occurs before grouping, aggregation, or other operations.
//
// The transformer converts ZetaSQL FilterScan nodes into SQLite WHERE clauses by:
// - Recursively transforming the input scan to get the data source
// - Transforming the filter expression through the coordinator
// - Creating a SELECT * FROM (...) WHERE <condition> wrapper
// - Preserving column availability through the fragment context
//
// Filter expressions can be complex boolean logic involving column references,
// function calls, comparisons, and logical operators (AND, OR, NOT).
type FilterScanTransformer struct {
	coordinator Coordinator // For recursive transformation of the inner scan
}

// NewFilterScanTransformer creates a new filter scan transformer
func NewFilterScanTransformer(coordinator Coordinator) *FilterScanTransformer {
	return &FilterScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts FilterScanData to FromItem with WHERE clause
func (t *FilterScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeFilter || data.FilterScan == nil {
		return nil, fmt.Errorf("expected filter scan data, got type %v", data.Type)
	}

	filterScanData := data.FilterScan

	innerFromItem, err := t.coordinator.TransformScan(filterScanData.InputScan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform inner scan in filter: %w", err)
	}

	// Transform the filter expression
	filterExpr, err := t.coordinator.TransformExpression(filterScanData.FilterExpr, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform filter expression: %w", err)
	}

	selectStmt := NewSelectStarStatement(innerFromItem)
	selectStmt.WhereClause = filterExpr

	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: selectStmt,
	}, nil
}
