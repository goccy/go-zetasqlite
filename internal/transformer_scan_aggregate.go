package internal

import (
	"fmt"
)

// AggregateScanTransformer handles aggregate operation transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, aggregate scans represent GROUP BY operations with aggregate functions
// like SUM, COUNT, AVG, etc. This includes complex features like ROLLUP, CUBE, and GROUPING SETS
// that create multiple levels of aggregation in a single query.
//
// The transformer converts ZetaSQL AggregateScan nodes by:
// - Transforming the input scan that provides data for aggregation
// - Converting aggregate expressions (SUM, COUNT, etc.) with zetasqlite function wrappers
// - Processing GROUP BY expressions with proper ZetaSQL semantics
// - Handling ROLLUP and GROUPING SETS via UNION ALL of different grouping levels
// - Managing NULL values for rollup totals and subtotals
//
// Key challenges:
// - ROLLUP generates multiple grouping levels (detail, subtotals, grand total)
// - Grouping columns become NULL in higher aggregation levels
// - Preserving ZetaSQL's grouping and aggregation semantics in SQLite
type AggregateScanTransformer struct {
	coordinator Coordinator
}

// NewAggregateScanTransformer creates a new aggregate scan transformer
func NewAggregateScanTransformer(coordinator Coordinator) *AggregateScanTransformer {
	return &AggregateScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts AggregateScanData to FromItem with SELECT statement containing aggregation
func (t *AggregateScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeAggregate || data.AggregateScan == nil {
		return nil, fmt.Errorf("expected aggregate scan data, got type %v", data.Type)
	}

	aggData := data.AggregateScan

	// Transform the input scan
	inputFromItem, err := t.coordinator.TransformScan(aggData.InputScan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform input scan: %w", err)
	}

	// Build the SELECT statement with aggregation
	selectStatement := NewSelectStatement()
	selectStatement.FromClause = inputFromItem

	// Process aggregate expressions and GROUP BY expressions
	selectList := make([]*SelectListItem, 0, len(data.ColumnList))

	// Create maps for quick lookup
	aggregateMap := make(map[int]*SQLExpression)
	groupByMap := make(map[int]*SQLExpression)

	// Process aggregate expressions
	for _, aggExpr := range aggData.AggregateList {
		sqlExpr, err := t.coordinator.TransformExpression(aggExpr.Expression, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform aggregate expression: %w", err)
		}
		aggregateMap[aggExpr.Column.ColumnID()] = sqlExpr
	}

	// Process GROUP BY expressions
	groupByExprs := make([]*SQLExpression, 0, len(aggData.GroupByList))
	for _, groupByExpr := range aggData.GroupByList {
		sqlExpr, err := t.coordinator.TransformExpression(groupByExpr.Expression, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform GROUP BY expression: %w", err)
		}

		// Wrap with zetasqlite_group_by for proper ZetaSQL semantics
		wrappedExpr := NewFunctionExpression("zetasqlite_group_by", sqlExpr)
		groupByExprs = append(groupByExprs, wrappedExpr)
		groupByMap[groupByExpr.Column.ColumnID()] = sqlExpr
	}

	// Build the SELECT list based on the output columns
	for _, col := range data.ColumnList {
		var expr *SQLExpression

		// Check if this is an aggregate column
		if aggExpr, found := aggregateMap[col.ID]; found {
			expr = aggExpr
		} else if groupByExpr, found := groupByMap[col.ID]; found {
			expr = groupByExpr
		} else {
			return nil, fmt.Errorf("column %s not found in aggregate or GROUP BY expressions", col.Name)
		}

		alias := generateIDBasedAlias(col.Name, col.ID)
		selectList = append(selectList, &SelectListItem{
			Expression: expr,
			Alias:      alias,
		})
	}

	selectStatement.SelectList = selectList
	selectStatement.GroupByList = groupByExprs

	// Handle GROUPING SETS if present
	if len(aggData.GroupingSets) > 0 {
		return t.buildGroupingSetsQuery(aggData, inputFromItem, data.ColumnList, selectList, groupByMap, ctx)
	}

	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: selectStatement,
	}, nil
}

// buildGroupingSetsQuery constructs a SQL query that handles BigQuery ROLLUP() grouping sets.
//
// BigQuery ROLLUP() creates hierarchical grouping levels by generating multiple grouping sets.
// For example, "GROUP BY ROLLUP(a, b)" is equivalent to:
//
//	GROUP BY GROUPING SETS ((a, b), (a), ())
//
// This produces:
// - Detailed groups: GROUP BY a, b (most granular level)
// - Subtotals: GROUP BY a (intermediate aggregation)
// - Grand total: no GROUP BY (overall aggregation with NULL for all grouping columns)
//
// In ZetaSQL AST, ROLLUP is represented as multiple GroupingSets where:
// - Non-empty GroupingSets contain actual grouping columns (normal GROUP BY)
// - Empty GroupingSets represent the grand total row (NULL for all grouping columns)
//
// This function transforms the grouping sets into a UNION ALL query where:
// 1. Each non-empty grouping set becomes a SELECT with GROUP BY on those columns
// 2. Each empty grouping set becomes a SELECT with no GROUP BY and NULL for grouping columns
// 3. All aggregate functions are preserved and applied at each grouping level
//
// Example transformation:
//
//	ZetaSQL: SELECT day, SUM(price) FROM sales GROUP BY ROLLUP(day)
//
//	Becomes:
//	  SELECT day, SUM(price) FROM sales GROUP BY day
//	  UNION ALL
//	  SELECT NULL, SUM(price) FROM sales  -- Grand total with NULL for day
func (t *AggregateScanTransformer) buildGroupingSetsQuery(
	aggData *AggregateScanData,
	inputFromItem *FromItem,
	columnData []*ColumnData,
	outputColumns []*SelectListItem,
	groupByMap map[int]*SQLExpression,
	ctx TransformContext,
) (*FromItem, error) {
	statements := make([]*SelectStatement, 0, len(aggData.GroupingSets))

	for _, groupingSet := range aggData.GroupingSets {
		// Process columns in this grouping set
		groupBySetColumns := make([]*SQLExpression, 0)
		groupBySetColumnMap := make(map[int]struct{})

		for _, col := range groupingSet.GroupByColumns {
			// Wrap with zetasqlite_group_by
			wrappedExpr := NewFunctionExpression("zetasqlite_group_by", groupByMap[col.Column.ColumnID()])
			groupBySetColumns = append(groupBySetColumns, wrappedExpr)
			groupBySetColumnMap[col.Column.ColumnID()] = struct{}{}
		}

		// Determine which columns should be NULL
		nullColumnNameMap := make(map[int]struct{})
		for id := range groupByMap {
			if _, exists := groupBySetColumnMap[id]; !exists {
				nullColumnNameMap[id] = struct{}{}
			}
		}

		// Build SELECT list for this grouping set
		selectList := make([]*SelectListItem, 0, len(groupByMap))
		for i, originalCol := range outputColumns {
			colName := columnData[i].Name

			if _, shouldBeNull := nullColumnNameMap[columnData[i].ID]; shouldBeNull {
				// This column should be NULL in this grouping set
				selectList = append(selectList, &SelectListItem{
					Expression: NewLiteralExpression("NULL"),
					Alias:      colName,
				})
			} else {
				// Use the original expression
				selectList = append(selectList, originalCol)
			}
		}

		// Create SELECT statement for this grouping set
		stmt := NewSelectStatement()
		stmt.SelectList = selectList
		stmt.FromClause = inputFromItem

		if len(groupBySetColumns) > 0 {
			stmt.GroupByList = groupBySetColumns
		}

		statements = append(statements, stmt)
	}

	alias := fmt.Sprintf("aggregate_grouping_sets_scan_%s", ctx.FragmentContext().GetID())

	// Combine with UNION ALL
	if len(statements) == 1 {
		return NewSubqueryFromItem(statements[0], alias), nil
	}

	setOperation := &SetOperation{
		Type:     "UNION",
		Modifier: "ALL",
		Items:    statements,
	}

	unionStatement := NewSelectStatement()
	unionStatement.SetOperation = setOperation

	// Add ORDER BY with collation for proper grouping behavior
	if len(groupByMap) > 0 {
		orderByItems := make([]*OrderByItem, 0, len(groupByMap))
		for _, groupByCol := range groupByMap {
			// Create a copy with collation
			orderExpr := &SQLExpression{
				Type:      groupByCol.Type,
				Value:     groupByCol.Value,
				Collation: "zetasqlite_collate",
			}

			orderByItems = append(orderByItems, &OrderByItem{
				Expression: orderExpr,
				Direction:  "ASC",
			})
		}
		unionStatement.OrderByList = orderByItems
	}

	// Create a subquery to introduce the new logical columns into the query
	statement := NewSelectStatement()
	for i, column := range columnData {
		// Retrieve column name from the first item
		itemColumn := setOperation.Items[0].SelectList[i]
		statement.SelectList = append(statement.SelectList, &SelectListItem{
			Expression: NewColumnExpression(itemColumn.Alias),
			Alias:      generateIDBasedAlias(column.Name, column.ID),
		})
	}
	statement.FromClause = NewSubqueryFromItem(unionStatement, "set_op_scan_inner")

	// Wrap in a subquery FromItem
	return NewSubqueryFromItem(statement, alias), nil
}
