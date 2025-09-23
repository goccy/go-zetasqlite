package internal

import (
	"fmt"
)

// ArrayScanTransformer handles array scan (UNNEST operations) transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, array scans represent UNNEST operations that flatten array values
// into individual rows. This enables queries to iterate over array elements as if they
// were rows in a table, with optional position/offset information and join conditions.
//
// The transformer converts ZetaSQL ArrayScan nodes by:
// - Transforming array expressions through the coordinator
// - Using SQLite's json_each() table function with zetasqlite_decode_array() for UNNEST
// - Handling correlated arrays with proper JOIN semantics (INNER vs LEFT)
// - Managing element and offset column availability in the fragment context
// - Supporting both standalone UNNEST and UNNEST with input scans
//
// The json_each() approach provides 'key' (offset) and 'value' (element) columns
// that map to ZetaSQL's array element and offset semantics in SQLite.
type ArrayScanTransformer struct {
	coordinator Coordinator
}

// NewArrayScanTransformer creates a new ArrayScanTransformer
func NewArrayScanTransformer(coordinator Coordinator) *ArrayScanTransformer {
	return &ArrayScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts ArrayScanData to a FromItem representing UNNEST operation
func (t *ArrayScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeArray || data.ArrayScan == nil {
		return nil, fmt.Errorf("expected array scan data, got type %v", data.Type)
	}

	arrayData := data.ArrayScan

	var innerFromItem *FromItem
	if arrayData.InputScan != nil {
		// Handle input scan for correlated arrays
		// Transform the input scan
		inputFromItem, err := t.coordinator.TransformScan(*arrayData.InputScan, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform input scan for array: %w", err)
		}
		innerFromItem = inputFromItem
	}

	// Transform the array expression to UNNEST
	arrayExpr, err := t.coordinator.TransformExpression(arrayData.ArrayExpr, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform array expression: %w", err)
	}

	// Create the json_each table function call with zetasqlite_decode_array
	jsonEachFromItem := &FromItem{
		Type: FromItemTypeTableFunction,
		TableFunction: &TableFunction{
			Name: "json_each",
			Arguments: []*SQLExpression{
				NewFunctionExpression(
					"zetasqlite_decode_array",
					arrayExpr,
				),
			},
		},
		Alias: fmt.Sprintf("$array_%s", ctx.FragmentContext().GetID()),
	}

	// The element / key columns must be made available prior to the JoinExpr being transformed
	// since they reference return values from SQLite's`json_each` which do not exist in ZetaSQL
	ctx.FragmentContext().AddAvailableColumn(arrayData.ElementColumn.ID, &ColumnInfo{
		ID:   arrayData.ElementColumn.ID,
		Name: "value",
		// This column name comes from SQLite's table-valued function `json_each` (our jsonEachFromItem)
		Expression: NewColumnExpression("value", jsonEachFromItem.Alias),
	})
	ctx.FragmentContext().RegisterColumnScope(arrayData.ElementColumn.ID, jsonEachFromItem.Alias)

	if offsetColumn := arrayData.ArrayOffsetColumn; offsetColumn != nil {
		ctx.FragmentContext().AddAvailableColumn(offsetColumn.ID, &ColumnInfo{
			ID:   offsetColumn.ID,
			Name: "key",
			// This column name comes from SQLite's table-valued function `json_each` (our jsonEachFromItem)
			Expression: NewColumnExpression("key", jsonEachFromItem.Alias),
		})
		ctx.FragmentContext().RegisterColumnScope(offsetColumn.ID, jsonEachFromItem.Alias)
	}

	// Create a subquery that selects the proper column names
	unnestSelect := NewSelectStatement()

	// Always select 'value' as the element column
	unnestSelect.SelectList = []*SelectListItem{}
	unnestSelect.FromClause = jsonEachFromItem

	for _, col := range data.ColumnList {
		name, table := ctx.FragmentContext().GetQualifiedColumnRef(col.ID)
		unnestSelect.SelectList = append(unnestSelect.SelectList, &SelectListItem{
			Expression: NewColumnExpression(name, table),
			Alias:      generateIDBasedAlias(col.Name, col.ID),
		})
	}

	// If there's no InputScan() we can return the select directly
	if arrayData.InputScan == nil {
		return NewSubqueryFromItem(unnestSelect, ""), nil
	}

	// Determine join type based on IsOuter flag
	var joinType JoinType
	if arrayData.IsOuter {
		joinType = JoinTypeLeft
	} else {
		joinType = JoinTypeInner
	}

	// Handle join condition if present
	var joinCondition *SQLExpression
	if arrayData.JoinExpr != nil {
		conditionExpr, err := t.coordinator.TransformExpression(*arrayData.JoinExpr, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform join expression: %w", err)
		}
		joinCondition = conditionExpr
	} else {
		// If there is no join expression use a CROSS JOIN
		joinType = JoinTypeCross
	}

	// Set the FROM clause to be a JOIN between input and UNNEST
	unnestSelect.FromClause = &FromItem{
		Type: FromItemTypeJoin,
		Join: &JoinClause{
			Type:      joinType,
			Left:      innerFromItem,
			Right:     jsonEachFromItem,
			Condition: joinCondition,
		},
	}

	return NewSubqueryFromItem(unnestSelect, ""), nil
}
