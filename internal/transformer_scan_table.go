package internal

import (
	"fmt"
)

// TableScanTransformer handles table scan transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a TableScan represents the foundational scan operation that reads
// directly from a table. This is the base case in the recursive scan transformation tree -
// it has no input scans and corresponds to a table reference in the FROM clause.
//
// The transformer converts ZetaSQL TableScan nodes into SQLite table references with:
// - Direct table name mapping
// - Optional table aliasing for disambiguation
// - Proper FROM clause item generation
//
// This is the simplest transformer as it performs direct mapping without complex logic,
// but it's crucial as the leaf node in the scan transformation tree.
type TableScanTransformer struct {
	coordinator Coordinator
}

// NewTableScanTransformer creates a new table scan transformer
func NewTableScanTransformer(coordinator Coordinator) *TableScanTransformer {
	return &TableScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts TableScanData to FromItem
func (t *TableScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeTable || data.TableScan == nil {
		return nil, fmt.Errorf("expected table scan data, got type %v", data.Type)
	}

	tableData := data.TableScan

	// Create a SELECT statement with explicit columns based on the ColumnList
	// This ensures proper column validation and alias matching
	selectList := make([]*SelectListItem, 0, len(data.ColumnList))
	for _, col := range data.ColumnList {
		// Create column reference expression
		columnExpr := &SQLExpression{
			Type:  ExpressionTypeColumn,
			Value: col.Name,
		}

		// Generate ID-based alias for consistency with coordinator expectations
		alias := generateIDBasedAlias(col.Name, col.ID)

		selectList = append(selectList, &SelectListItem{
			Expression: columnExpr,
			Alias:      alias,
		})
	}

	// Handle synthetic columns list
	// This is used when querying Wildcard tables. A matched table may not have a column listed in the
	// original query's SELECT statement, in which case, an expression is provided in place of a Column reference
	for _, item := range tableData.SyntheticColumns {
		if column := item.Expression.Column; item.Expression.Type == ExpressionTypeColumn {
			ctx.FragmentContext().RegisterColumnScope(column.ColumnID, "")
			ctx.FragmentContext().AddAvailableColumn(column.ColumnID, &ColumnInfo{
				Name: column.ColumnName,
			})
		}

		expr, err := t.coordinator.TransformExpression(item.Expression, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform expression: %w", err)
		}
		selectList = append(selectList, &SelectListItem{
			Expression: expr,
			Alias:      item.Alias,
		})
	}

	// Create the table FROM item
	tableFromItem := &FromItem{
		Type:      FromItemTypeTable,
		TableName: tableData.TableName,
		Alias:     tableData.Alias,
	}

	// Create a SELECT statement that explicitly lists the columns
	selectStatement := &SelectStatement{
		SelectType: SelectTypeStandard,
		FromClause: tableFromItem,
		SelectList: selectList,
	}

	// Return as a subquery to ensure proper column validation
	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: selectStatement,
		Alias:    tableData.Alias,
	}, nil
}
