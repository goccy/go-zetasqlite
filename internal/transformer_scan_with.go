package internal

import (
	"fmt"
)

// WithScanTransformer handles WITH scan transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a WithScan represents a complete WITH statement (Common Table Expression)
// that defines one or more named temporary result sets that can be referenced in the main query.
// This enables recursive queries, query organization, and performance optimization.
//
// The transformer converts ZetaSQL WithScan nodes into SQLite WITH clauses by:
// - Processing all WITH entry definitions into CTE declarations
// - Recursively transforming each WITH entry's subquery
// - Transforming the main query that references the CTEs
// - Ensuring proper scoping and name resolution across CTE boundaries
//
// Each WITH entry becomes a named subquery that can be referenced by name in subsequent
// WITH entries or the main query, following SQL's lexical scoping rules.
type WithScanTransformer struct {
	coordinator Coordinator
}

func NewWithScanTransformer(coordinator Coordinator) *WithScanTransformer {
	return &WithScanTransformer{coordinator: coordinator}
}

// Transform converts WithScanData to FromItem with WITH clauses
func (t *WithScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeWith || data.WithScan == nil {
		return nil, fmt.Errorf("expected with scan data, got type %v", data.Type)
	}

	withScanData := data.WithScan

	// Process all WITH entries to create CTE definitions
	withClauses := []*WithClause{}
	for _, entryData := range withScanData.WithEntryList {
		// Transform each WITH entry to a WithClause
		entryScanData := ScanData{
			Type:          ScanTypeWithEntry,
			WithEntryScan: entryData,
		}

		withClause, err := t.coordinator.TransformWithEntry(entryScanData, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform WITH entry: %w", err)
		}

		withClauses = append(withClauses, withClause)
	}

	// Transform the main query that uses the CTEs
	queryFromItem, err := t.coordinator.TransformScan(withScanData.Query, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform WITH query: %w", err)
	}

	// Create SELECT statement with WITH clauses
	selectStatement := NewSelectStarStatement(queryFromItem)
	selectStatement.WithClauses = withClauses

	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: selectStatement,
	}, nil
}
