package internal

import (
	"encoding/json"
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"sync"
)

// QueryCoordinator orchestrates the transformation process by delegating to appropriate transformers
type QueryCoordinator struct {
	// Expression transformers - direct references for performance
	literalTransformer   ExpressionTransformer
	functionTransformer  ExpressionTransformer
	castTransformer      ExpressionTransformer
	columnRefTransformer ExpressionTransformer
	subqueryTransformer  ExpressionTransformer
	parameterTransformer ExpressionTransformer

	// Statement transformers - direct references
	queryStmtTransformer               StatementTransformer
	insertStmtTransformer              StatementTransformer
	updateStmtTransformer              StatementTransformer
	deleteStmtTransformer              StatementTransformer
	createViewStmtTransformer          StatementTransformer
	createTableAsSelectStmtTransformer StatementTransformer
	dropStmtTransformer                StatementTransformer
	mergeStmtTransformer               StatementTransformer

	// Scan transformers - direct references
	tableScanTransformer        ScanTransformer
	projectScanTransformer      ScanTransformer
	filterScanTransformer       ScanTransformer
	joinScanTransformer         ScanTransformer
	aggregateScanTransformer    ScanTransformer
	orderByScanTransformer      ScanTransformer
	limitScanTransformer        ScanTransformer
	setOpScanTransformer        ScanTransformer
	singleRowScanTransformer    ScanTransformer
	withScanTransformer         ScanTransformer
	withRefScanTransformer      ScanTransformer
	arrayScanTransformer        ScanTransformer
	analyticScanTransformer     ScanTransformer
	recursiveScanTransformer    ScanTransformer
	recursiveRefScanTransformer ScanTransformer

	// Node data extractors
	extractor *NodeExtractor
}

// NewQueryCoordinator creates a new coordinator with all transformers initialized directly
func NewQueryCoordinator(extractor *NodeExtractor) *QueryCoordinator {
	coordinator := &QueryCoordinator{
		extractor: extractor,
	}

	// Initialize all transformers directly - no reflection needed
	// Expression transformers
	coordinator.literalTransformer = NewLiteralTransformer()
	coordinator.functionTransformer = NewFunctionCallTransformer(coordinator)
	coordinator.castTransformer = NewCastTransformer(coordinator)
	coordinator.columnRefTransformer = NewColumnRefTransformer(coordinator)
	coordinator.subqueryTransformer = NewSubqueryTransformer(coordinator)
	coordinator.parameterTransformer = NewParameterTransformer()

	// Statement transformers
	coordinator.queryStmtTransformer = NewQueryStmtTransformer(coordinator)
	coordinator.insertStmtTransformer = NewDMLStmtTransformer(coordinator)
	coordinator.updateStmtTransformer = NewDMLStmtTransformer(coordinator)
	coordinator.deleteStmtTransformer = NewDMLStmtTransformer(coordinator)
	coordinator.createViewStmtTransformer = NewCreateViewStmtTransformer(coordinator)
	coordinator.createTableAsSelectStmtTransformer = NewCreateTableAsSelectStmtTransformer(coordinator)
	coordinator.dropStmtTransformer = NewDropStmtTransformer(coordinator)
	coordinator.mergeStmtTransformer = NewMergeStmtTransformer(coordinator)

	// Scan transformers
	coordinator.tableScanTransformer = NewTableScanTransformer(coordinator)
	coordinator.projectScanTransformer = NewProjectScanTransformer(coordinator)
	coordinator.filterScanTransformer = NewFilterScanTransformer(coordinator)
	coordinator.joinScanTransformer = NewJoinScanTransformer(coordinator)
	coordinator.aggregateScanTransformer = NewAggregateScanTransformer(coordinator)
	coordinator.orderByScanTransformer = NewOrderByScanTransformer(coordinator)
	coordinator.limitScanTransformer = NewLimitScanTransformer(coordinator)
	coordinator.setOpScanTransformer = NewSetOperationScanTransformer(coordinator)
	coordinator.singleRowScanTransformer = NewSingleRowScanTransformer(coordinator)
	coordinator.withScanTransformer = NewWithScanTransformer(coordinator)
	coordinator.withRefScanTransformer = NewWithRefScanTransformer(coordinator)
	coordinator.arrayScanTransformer = NewArrayScanTransformer(coordinator)
	coordinator.analyticScanTransformer = NewAnalyticScanTransformer(coordinator)
	coordinator.recursiveScanTransformer = NewRecursiveScanTransformer(coordinator)
	coordinator.recursiveRefScanTransformer = NewRecursiveRefScanTransformer(coordinator)

	return coordinator
}

// Global singleton for performance
var (
	globalCoordinator Coordinator
	coordinatorOnce   sync.Once
)

// GetGlobalCoordinator returns the singleton coordinator instance
// This eliminates the overhead of creating new coordinators and registering transformers
func GetGlobalCoordinator() Coordinator {
	coordinatorOnce.Do(func() {
		extractor := NewNodeExtractor()
		globalCoordinator = NewQueryCoordinator(extractor)
	})
	return globalCoordinator
}

// TransformStatement transforms a statement AST node to SQLFragment
func (c *QueryCoordinator) TransformStatementNode(node ast.Node, ctx TransformContext) (SQLFragment, error) {
	if node == nil {
		return nil, fmt.Errorf("cannot transform nil statement node")
	}

	debug := ctx.Config().Debug
	if debug {
		fmt.Println("--- AST:")
		fmt.Print(node.DebugString())
	}

	// Extract pure data from the AST node
	data, err := c.extractor.ExtractStatementData(node, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract statement data: %w", err)
	}

	token := ctx.FragmentContext().EnterScope()
	defer ctx.FragmentContext().ExitScope(token)

	if debug {
		j, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		fmt.Println("--- EXTRACTED DATA:")
		fmt.Println(string(j))
	}

	// Delegate to the appropriate transformer using direct dispatch
	result, err := c.TransformStatement(data, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform statement: %w", err)
	}

	if debug {
		fmt.Println("--- FORMATTED QUERY:")
		fmt.Println(result)
		fmt.Println("---")
	}

	return result, nil
}

// Data-based transformation methods (for transformers working with pure data)

// TransformExpression transforms expression data to SQLExpression using direct dispatch
func (c *QueryCoordinator) TransformExpression(exprData ExpressionData, ctx TransformContext) (*SQLExpression, error) {
	// Direct dispatch based on expression data type - no reflection needed
	switch exprData.Type {
	case ExpressionTypeLiteral:
		return c.literalTransformer.Transform(exprData, ctx)
	case ExpressionTypeFunction:
		return c.functionTransformer.Transform(exprData, ctx)
	case ExpressionTypeCast:
		return c.castTransformer.Transform(exprData, ctx)
	case ExpressionTypeColumn:
		return c.columnRefTransformer.Transform(exprData, ctx)
	case ExpressionTypeSubquery:
		return c.subqueryTransformer.Transform(exprData, ctx)
	case ExpressionTypeParameter:
		return c.parameterTransformer.Transform(exprData, ctx)
	default:
		return nil, fmt.Errorf("unsupported expression data type: %v", exprData.Type)
	}
}

// TransformStatement transforms statement data to SQLFragment using direct dispatch
func (c *QueryCoordinator) TransformStatement(stmtData StatementData, ctx TransformContext) (SQLFragment, error) {
	token := ctx.FragmentContext().EnterScope()
	defer ctx.FragmentContext().ExitScope(token)

	// Direct dispatch based on statement data type - no reflection needed
	switch stmtData.Type {
	case StatementTypeSelect:
		return c.queryStmtTransformer.Transform(stmtData, ctx)
	case StatementTypeInsert:
		return c.insertStmtTransformer.Transform(stmtData, ctx)
	case StatementTypeUpdate:
		return c.updateStmtTransformer.Transform(stmtData, ctx)
	case StatementTypeDelete:
		return c.deleteStmtTransformer.Transform(stmtData, ctx)
	case StatementTypeCreate:
		// For CREATE statements, dispatch based on create type
		if stmtData.Create != nil {
			switch stmtData.Create.Type {
			case CreateTypeTable:
				if stmtData.Create.Table.AsSelect != nil {
					return c.createTableAsSelectStmtTransformer.Transform(stmtData, ctx)
				}
				return nil, fmt.Errorf("unsupported CREATE TABLE statement")
			case CreateTypeView:
				return c.createViewStmtTransformer.Transform(stmtData, ctx)
			// CREATE TABLE and CREATE VIEW are handled separately
			default:
				return nil, fmt.Errorf("unsupported create statement data type: %v", stmtData.Create.Type)
			}
		}
		return nil, fmt.Errorf("unsupported create statement type")
	case StatementTypeDrop:
		return c.dropStmtTransformer.Transform(stmtData, ctx)
	case StatementTypeMerge:
		return c.mergeStmtTransformer.Transform(stmtData, ctx)
	default:
		return nil, fmt.Errorf("unsupported statement data type: %v", stmtData.Type)
	}
}

// TransformScan transforms scan data to FromItem using direct dispatch
func (c *QueryCoordinator) TransformScan(scanData ScanData, ctx TransformContext) (*FromItem, error) {
	token := ctx.FragmentContext().EnterScope()
	defer ctx.FragmentContext().ExitScope(token)

	// Direct dispatch based on scan data type - no reflection needed
	var fromItem *FromItem
	var err error
	var alias string

	switch scanData.Type {
	case ScanTypeTable:
		alias = "table_scan"
		fromItem, err = c.tableScanTransformer.Transform(scanData, ctx)
	case ScanTypeJoin:
		alias = "join_scan"
		fromItem, err = c.joinScanTransformer.Transform(scanData, ctx)
	case ScanTypeFilter:
		alias = "filter_scan"
		fromItem, err = c.filterScanTransformer.Transform(scanData, ctx)
	case ScanTypeProject:
		alias = "project_scan"
		fromItem, err = c.projectScanTransformer.Transform(scanData, ctx)
	case ScanTypeAggregate:
		alias = "aggregate_scan"
		fromItem, err = c.aggregateScanTransformer.Transform(scanData, ctx)
	case ScanTypeOrderBy:
		alias = "order_by_scan"
		fromItem, err = c.orderByScanTransformer.Transform(scanData, ctx)
	case ScanTypeLimit:
		alias = "limit_scan"
		fromItem, err = c.limitScanTransformer.Transform(scanData, ctx)
	case ScanTypeSetOp:
		alias = "set_op_scan"
		fromItem, err = c.setOpScanTransformer.Transform(scanData, ctx)
	case ScanTypeSingleRow:
		alias = "single_row_scan"
		fromItem, err = c.singleRowScanTransformer.Transform(scanData, ctx)
	case ScanTypeWith:
		alias = "with_scan"
		fromItem, err = c.withScanTransformer.Transform(scanData, ctx)
	case ScanTypeWithRef:
		alias = "with_ref_scan"
		fromItem, err = c.withRefScanTransformer.Transform(scanData, ctx)
	case ScanTypeWithEntry:
		// WithEntry is handled specially - return early
		return nil, fmt.Errorf("WithEntry scans should use TransformWithEntry method")
	case ScanTypeArray:
		alias = "array_scan"
		fromItem, err = c.arrayScanTransformer.Transform(scanData, ctx)
	case ScanTypeAnalytic:
		alias = "analytic_scan"
		fromItem, err = c.analyticScanTransformer.Transform(scanData, ctx)
	case ScanTypeRecursive:
		alias = "recursive_scan"
		fromItem, err = c.recursiveScanTransformer.Transform(scanData, ctx)
	case ScanTypeRecursiveRef:
		alias = "recursive_ref_scan"
		fromItem, err = c.recursiveRefScanTransformer.Transform(scanData, ctx)
	default:
		return nil, fmt.Errorf("unsupported scan data type: %v", scanData.Type)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to transform scan data: %w", err)
	}

	alias = fmt.Sprintf("%s_%s", alias, ctx.FragmentContext().GetID())
	fromItem.Alias = alias

	// Verify output column names against extracted data and add to scope
	if err := c.validateColumnData(fromItem, scanData.ColumnList, ctx); err != nil {
		return nil, fmt.Errorf("column validation failed for %v: %w", scanData.Type, err)
	}

	// Add available column expressions
	for _, column := range scanData.ColumnList {
		ctx.FragmentContext().AddAvailableColumn(column.ID, &ColumnInfo{
			Name: column.Name,
			ID:   column.ID,
		})
	}

	// Register scope mappings for output columns
	ctx.FragmentContext().RegisterColumnScopeMapping(alias, scanData.ColumnList)

	return fromItem, nil
}

// TransformWithEntryData transforms WITH entry data to WithClause
func (c *QueryCoordinator) TransformWithEntry(scanData ScanData, ctx TransformContext) (*WithClause, error) {
	if scanData.Type != ScanTypeWithEntry {
		return nil, fmt.Errorf("expected WITH entry data, got type %v", scanData.Type)
	}

	token := ctx.FragmentContext().EnterScope()
	defer ctx.FragmentContext().ExitScope(token)

	// Create a WithEntryTransformer for this transformation
	transformer := NewWithEntryTransformer(c)
	return transformer.Transform(scanData, ctx)
}

// Helper methods for data-based transformations

// TransformExpressionDataList transforms a list of expression data
func (c *QueryCoordinator) TransformExpressionDataList(exprDataList []ExpressionData, ctx TransformContext) ([]*SQLExpression, error) {
	if exprDataList == nil {
		return nil, nil
	}

	result := make([]*SQLExpression, 0, len(exprDataList))
	for i, exprData := range exprDataList {
		expr, err := c.TransformExpression(exprData, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform expression data at index %d: %w", i, err)
		}
		result = append(result, expr)
	}

	return result, nil
}

// TransformOptionalExpressionData transforms optional expression data
func (c *QueryCoordinator) TransformOptionalExpressionData(exprData *ExpressionData, ctx TransformContext) (*SQLExpression, error) {
	if exprData == nil {
		return nil, nil
	}
	return c.TransformExpression(*exprData, ctx)
}

// validateColumnData validates that output columns in a transformed scan's SelectList
// use id-based aliases and match what are held in the ScanData.ColumnList.
// For Select Star subqueries, recursively uses the subquery's SelectList.
func (c *QueryCoordinator) validateColumnData(fromItem *FromItem, expectedColumns []*ColumnData, ctx TransformContext) error {
	if fromItem == nil || len(expectedColumns) == 0 {
		return nil
	}

	var selectList []*SelectListItem

	// Get the SelectList to validate - handle different FromItem types
	switch fromItem.Type {
	case FromItemTypeSubquery:
		if fromItem.Subquery == nil {
			return fmt.Errorf("subquery FromItem has nil Subquery")
		}
		selectList = c.getSelectListRecursive(fromItem.Subquery)
	case FromItemTypeTable:
		// Table scans don't have SelectLists to validate
		return nil
	default:
		// For other types like joins, we may not have a direct SelectList
		return nil
	}

	if selectList == nil {
		return fmt.Errorf("could not extract SelectList from FromItem")
	}

	// Validate that we have the same number of columns
	if len(selectList) != len(expectedColumns) {
		return fmt.Errorf("SelectList length mismatch: got %d items, expected %d columns",
			len(selectList), len(expectedColumns))
	}

	// Generate expected aliases using generateIDBasedAlias
	expectedAliases := make(map[string]bool)
	for _, col := range expectedColumns {
		expectedAlias := generateIDBasedAlias(col.Name, col.ID)
		expectedAliases[expectedAlias] = true
	}

	// Validate each SelectListItem
	for i, item := range selectList {
		if item.Alias == "" {
			return fmt.Errorf("SelectListItem at index %d has empty alias", i)
		}

		// Verify the alias matches one of our expected id-based aliases
		if !expectedAliases[item.Alias] {
			return fmt.Errorf("SelectListItem at index %d has unexpected alias '%s', not found in expected id-based aliases",
				i, item.Alias)
		}
	}

	return nil
}

// getSelectListRecursive extracts SelectList from a SelectStatement, handling Select Star subqueries recursively
func (c *QueryCoordinator) getSelectListRecursive(stmt *SelectStatement) []*SelectListItem {
	if stmt.SetOperation != nil {
		items := []*SelectListItem{}
		items = append(items, c.getSelectListRecursive(stmt.SetOperation.Items[0])...)
		return items
	}

	if stmt == nil || len(stmt.SelectList) == 0 {
		return nil
	}

	// Check if this is a Select Star query
	if len(stmt.SelectList) == 1 {
		item := stmt.SelectList[0]
		if item.IsStarExpansion || (item.Expression != nil && item.Expression.Type == ExpressionTypeStar) {
			// This is a SELECT * - we need to recurse into the FROM clause
			if stmt.FromClause != nil && stmt.FromClause.Type == FromItemTypeSubquery && stmt.FromClause.Subquery != nil {
				return c.getSelectListRecursive(stmt.FromClause.Subquery)
			}
			if stmt.FromClause != nil && stmt.FromClause.Type == FromItemTypeJoin {
				items := []*SelectListItem{}
				items = append(items, c.getSelectListRecursive(stmt.FromClause.Join.Left.Subquery)...)
				items = append(items, c.getSelectListRecursive(stmt.FromClause.Join.Right.Subquery)...)
				return items
			}
		}
	}

	// Return the current SelectList
	return stmt.SelectList
}
