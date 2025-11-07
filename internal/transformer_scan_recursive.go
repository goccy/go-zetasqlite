package internal

import (
	"fmt"
)

// RecursiveScanTransformer handles recursive CTE scan transformations.
//
// In BigQuery/ZetaSQL, a RecursiveScanNode represents the definition of a recursive
// CTE, consisting of:
// - A non-recursive term (base case) - the initial rows
// - A recursive term (recursive case) - rows computed by referencing the CTE itself
// - An operation type (typically UNION ALL) to combine the terms
//
// The transformer converts RecursiveScanNode by:
// - Transforming both the non-recursive and recursive terms to SELECT statements
// - Creating a SetOperation to combine them
// - Wrapping the result appropriately for use in a CTE definition
type RecursiveScanTransformer struct {
	coordinator Coordinator
}

// NewRecursiveScanTransformer creates a new recursive scan transformer
func NewRecursiveScanTransformer(coordinator Coordinator) *RecursiveScanTransformer {
	return &RecursiveScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts RecursiveScanData to a SELECT statement with set operations
func (t *RecursiveScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeRecursive || data.RecursiveScan == nil {
		return nil, fmt.Errorf("expected recursive scan data, got type %v", data.Type)
	}

	recursiveScanData := data.RecursiveScan

	// Transform the non-recursive term (base case)
	nonRecursiveStmt, err := t.coordinator.TransformStatement(recursiveScanData.NonRecursiveTerm, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform non-recursive term: %w", err)
	}

	nonRecursiveSelect, ok := nonRecursiveStmt.(*SelectStatement)
	if !ok {
		return nil, fmt.Errorf("non-recursive term is not a SELECT statement")
	}

	// Transform the recursive term (recursive case)
	recursiveStmt, err := t.coordinator.TransformStatement(recursiveScanData.RecursiveTerm, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform recursive term: %w", err)
	}

	recursiveSelect, ok := recursiveStmt.(*SelectStatement)
	if !ok {
		return nil, fmt.Errorf("recursive term is not a SELECT statement")
	}

	// CRITICAL: Flatten the recursive term to expose the table reference at the top level
	// SQLite requires the recursive CTE reference to be directly in the FROM clause,
	// not buried in nested subqueries. Our bottom-up transformers create layers of
	// subqueries for column aliasing, so we need to collapse them here.
	recursiveSelect = flattenSelectForRecursiveCTE(recursiveSelect)

	// Parse the operation type to extract type and modifier
	var opType, modifier string
	switch recursiveScanData.OpType {
	case "UNION ALL":
		opType = "UNION"
		modifier = "ALL"
	case "UNION":
		opType = "UNION"
		modifier = ""
	default:
		return nil, fmt.Errorf("unsupported recursive operation type: %s", recursiveScanData.OpType)
	}

	// For recursive CTEs, we need to ensure the final column names match data.ColumnList
	// by adding proper aliases to the SetOperation items
	for itemIdx, item := range []*SelectStatement{nonRecursiveSelect, recursiveSelect} {
		for i, column := range data.ColumnList {
			if i < len(item.SelectList) {
				// Update the alias to match the expected output column
				item.SelectList[i].Alias = generateIDBasedAlias(column.Name, column.ID)
			}
		}
		// Clear any FROM clause wrapping if it's just a passthrough
		if itemIdx == 0 {
			nonRecursiveSelect = item
		} else {
			recursiveSelect = item
		}
	}

	// Create the set operation combining non-recursive and recursive terms
	setOperation := &SetOperation{
		Type:     opType,
		Modifier: modifier,
		Items:    []*SelectStatement{nonRecursiveSelect, recursiveSelect},
	}

	// Create a SELECT statement that contains the set operation directly
	// For recursive CTEs, this will be used directly as the CTE definition
	setStatement := NewSelectStatement()
	setStatement.SetOperation = setOperation

	// Move all WITH clauses from items to top-level
	for _, item := range setOperation.Items {
		setStatement.WithClauses = append(setStatement.WithClauses, item.WithClauses...)
		item.WithClauses = item.WithClauses[:0] // Clear the WITH clauses from the items
	}

	// Return the set operation statement directly wrapped in a FromItem
	// The WithEntryTransformer will handle this appropriately for recursive CTEs
	return NewSubqueryFromItem(setStatement, "recursive_scan"), nil
}