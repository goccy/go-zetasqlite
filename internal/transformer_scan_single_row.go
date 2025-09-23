package internal

import (
	"fmt"
)

// SingleRowScanTransformer handles single row scan transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a SingleRowScan represents queries that produce exactly one row
// without reading from any table - typically SELECT statements with only literal values
// or expressions that don't reference table columns (e.g., "SELECT 1, 'hello'").
//
// This corresponds to SQL's capability to SELECT constant expressions without a FROM clause.
// The transformer converts ZetaSQL SingleRowScan nodes by:
// - Creating a FromItemTypeSingleRow to indicate no table source is needed
// - Allowing the query to proceed without a FROM clause
// - Preserving expression evaluation in the SELECT list
//
// This is used for queries like "SELECT CURRENT_DATE()" or "SELECT 1 + 2" where
// no table data is required, only expression evaluation.
type SingleRowScanTransformer struct {
	coordinator Coordinator
}

// NewSingleRowScanTransformer creates a new table scan transformer
func NewSingleRowScanTransformer(coord Coordinator) *SingleRowScanTransformer {
	return &SingleRowScanTransformer{
		coordinator: coord,
	}
}

// Transform converts SingleRowScan to nil (no-op)
func (t *SingleRowScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeSingleRow {
		return nil, fmt.Errorf("expected single row scan data, got type %v", data.Type)
	}

	return &FromItem{
		Type: FromItemTypeSingleRow,
	}, nil
}
