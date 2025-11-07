package internal

import (
	"fmt"
)

// RecursiveRefScanTransformer handles recursive CTE reference transformations.
//
// In BigQuery/ZetaSQL, a RecursiveRefScanNode represents a self-reference within
// a recursive CTE's recursive term. This is the mechanism by which the CTE references
// itself to build up results iteratively.
//
// The transformer converts RecursiveRefScanNode by creating a table reference to the
// CTE name being currently defined (obtained from transform context).
type RecursiveRefScanTransformer struct {
	coordinator Coordinator
}

// NewRecursiveRefScanTransformer creates a new recursive ref scan transformer
func NewRecursiveRefScanTransformer(coordinator Coordinator) *RecursiveRefScanTransformer {
	return &RecursiveRefScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts RecursiveRefScanData to a FromItem that references the recursive CTE
func (t *RecursiveRefScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeRecursiveRef || data.RecursiveRefScan == nil {
		return nil, fmt.Errorf("expected recursive ref scan data, got type %v", data.Type)
	}

	// Get the recursive CTE name from context
	recursiveCTEName := ctx.GetRecursiveCTEName()
	if ctx.Config().Debug {
		fmt.Printf("DEBUG: RecursiveRefScan - CTE name from context: '%s'\n", recursiveCTEName)
	}
	if recursiveCTEName == "" {
		return nil, fmt.Errorf("recursive CTE name not found in context")
	}

	// Create a SELECT statement that references the recursive CTE by name
	tableAlias := fmt.Sprintf("rrs%s", ctx.FragmentContext().GetID())
	selectStatement := NewSelectStatement()
	selectStatement.FromClause = &FromItem{
		Type:      FromItemTypeTable,
		TableName: recursiveCTEName,
		Alias:     tableAlias,
	}
	selectStatement.SelectList = []*SelectListItem{}

	// Get stored column mappings from context for this recursive CTE
	mapping := ctx.GetWithEntryMapping(recursiveCTEName)
	if mapping == nil {
		return nil, fmt.Errorf("no entry mapping found for recursive CTE %v", recursiveCTEName)
	}
	if len(mapping) != len(data.RecursiveRefScan.ColumnList) {
		return nil, fmt.Errorf("incorrect number of columns found for recursive CTE %v", recursiveCTEName)
	}

	// Add SELECT items for each column
	for i, column := range data.RecursiveRefScan.ColumnList {
		alias := generateIDBasedAlias(column.Name, column.ID)

		selectStatement.SelectList = append(selectStatement.SelectList,
			&SelectListItem{
				Expression: NewColumnExpression(mapping[i], tableAlias),
				Alias:      alias,
			},
		)
	}

	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: selectStatement,
	}, nil
}