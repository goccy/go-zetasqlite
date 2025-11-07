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

	// For recursive CTEs, register column mappings BEFORE transforming the subquery
	// so that the recursive reference can see the CTE's columns
	isRecursive := ctx.GetRecursiveCTEName() == withEntryData.WithQueryName
	if isRecursive {
		ctx.AddWithEntryColumnMapping(
			withEntryData.WithQueryName,
			withEntryData.ColumnList,
		)
	}

	// Transform the subquery
	subqueryFromItem, err := t.coordinator.TransformScan(withEntryData.WithSubquery, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform WITH entry subquery: %w", err)
	}

	// For non-recursive CTEs, register column mappings AFTER transforming
	if !isRecursive {
		ctx.AddWithEntryColumnMapping(
			withEntryData.WithQueryName,
			withEntryData.ColumnList,
		)
	}

	// For recursive CTEs, use the subquery directly to preserve the UNION ALL structure
	// For non-recursive CTEs, wrap in a SELECT *
	var queryStatement *SelectStatement
	if isRecursive && subqueryFromItem.Type == FromItemTypeSubquery && subqueryFromItem.Subquery != nil {
		// Use the inner SelectStatement directly to preserve recursive structure
		queryStatement = subqueryFromItem.Subquery
	} else {
		// Wrap in SELECT * for non-recursive CTEs
		queryStatement = NewSelectStarStatement(subqueryFromItem)
	}

	// Create the WithClause
	return &WithClause{
		Name:  withEntryData.WithQueryName,
		Query: queryStatement,
	}, nil
}
