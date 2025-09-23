package internal

import (
	"fmt"
)

// WithEntryTransformer handles WITH entry transformations (CTE definitions) from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a WithEntry represents a single Common Table Expression (CTE) definition
// within a WITH clause. Each entry defines a named temporary result set with a specific
// column list that can be referenced by name in subsequent CTEs or the main query.
//
// The transformer converts ZetaSQL WithEntry nodes into SQLite WITH clause definitions by:
// - Transforming the subquery that defines the CTE's content
// - Registering the CTE name and column mappings in the transform context
// - Creating a WithClause structure for inclusion in the parent WITH statement
// - Managing scope and visibility for CTE references
//
// This enables proper name resolution when the CTE is referenced later in the query,
// following SQL's lexical scoping rules for Common Table Expressions.
type WithEntryTransformer struct {
	coordinator Coordinator
}

// NewWithEntryTransformer creates a new WITH entry transformer
func NewWithEntryTransformer(coordinator Coordinator) *WithEntryTransformer {
	return &WithEntryTransformer{
		coordinator: coordinator,
	}
}

// Transform converts WithEntryData to WithClause for use in SELECT statements
func (t *WithEntryTransformer) Transform(data ScanData, ctx TransformContext) (*WithClause, error) {
	if data.Type != ScanTypeWithEntry || data.WithEntryScan == nil {
		return nil, fmt.Errorf("expected with entry data, got type %v", data.Type)
	}

	withEntryData := data.WithEntryScan

	// Transform the subquery
	subqueryFromItem, err := t.coordinator.TransformScan(withEntryData.WithSubquery, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform WITH entry subquery: %w", err)
	}

	// Register the WITH entry's column mappings in the context
	ctx.AddWithEntryColumnMapping(
		withEntryData.WithQueryName,
		withEntryData.ColumnList,
	)

	// Create the WithClause
	return &WithClause{
		Name:  withEntryData.WithQueryName,
		Query: NewSelectStarStatement(subqueryFromItem),
	}, nil
}
