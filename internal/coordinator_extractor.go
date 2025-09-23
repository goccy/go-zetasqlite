package internal

import (
	"context"
	"fmt"
	parsed_ast "github.com/goccy/go-zetasql/ast"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"strings"
)

// extractColumnData converts an ast.Column to ColumnData for JSON serialization
func extractColumnData(col *ast.Column) *ColumnData {
	if col == nil {
		return &ColumnData{}
	}
	return &ColumnData{
		ID:        col.ColumnID(),
		Name:      col.Name(),
		Type:      col.Type().Kind().String(),
		TableName: col.TableName(),
	}
}

// extractColumnDataList converts a slice of ast.Column to slice of ColumnData
func extractColumnDataList(columns []*ast.Column) []*ColumnData {
	columnData := make([]*ColumnData, 0, len(columns))
	for _, col := range columns {
		columnData = append(columnData, extractColumnData(col))
	}
	return columnData
}

// NodeExtractor is responsible for extracting pure data from AST nodes
// This separates the concerns of AST traversal from data extraction
type NodeExtractor struct{}

// NewNodeExtractor creates a new node extractor
func NewNodeExtractor() *NodeExtractor {
	return &NodeExtractor{}
}

// ExtractExpressionData extracts pure data from expression AST nodes
func (e *NodeExtractor) ExtractExpressionData(node ast.Node, ctx TransformContext) (ExpressionData, error) {
	if node == nil {
		return ExpressionData{}, fmt.Errorf("cannot extract data from nil node")
	}

	switch n := node.(type) {
	case *ast.LiteralNode:
		return e.extractLiteralData(n, ctx)
	case *ast.FunctionCallNode:
		return e.extractFunctionCallData(n.BaseFunctionCallNode, ctx, false)
	case *ast.CastNode:
		return e.extractCastData(n, ctx)
	case *ast.ColumnRefNode:
		return e.extractColumnRefData(n, ctx)
	case *ast.SubqueryExprNode:
		return e.extractSubqueryData(n, ctx)
	case *ast.ComputedColumnNode:
		return e.ExtractExpressionData(n.Expr(), ctx)
	case *ast.OutputColumnNode:
		// Output columns reference other columns - use the referenced column's name, not the output name
		column := n.Column()

		return NewColumnExpressionData(column), nil
	case *ast.ParameterNode:
		return e.extractParameterData(n, ctx)
	case *ast.ArgumentRefNode:
		return e.extractArgumentRefData(n, ctx)
	case *ast.DMLValueNode:
		return e.ExtractExpressionData(n.Value(), ctx)
	case *ast.DMLDefaultNode:
		return e.extractDMLDefaultData(n, ctx)
	case *ast.MakeStructNode:
		return e.extractMakeStructData(n, ctx)
	case *ast.GetStructFieldNode:
		return e.extractGetStructFieldData(n, ctx)
	case *ast.GetJsonFieldNode:
		return e.extractGetJsonFieldData(n, ctx)
	case *ast.AggregateFunctionCallNode:
		return e.extractAggregateFunctionCallData(n, ctx)
	case *ast.AnalyticFunctionCallNode:
		return e.extractAnalyticFunctionCallData(n, ctx)
	default:
		return ExpressionData{}, fmt.Errorf("unsupported expression node type: %T", node)
	}
}

// extractLiteralData extracts data from literal nodes
func (e *NodeExtractor) extractLiteralData(node *ast.LiteralNode, ctx TransformContext) (ExpressionData, error) {
	originalValue := node.Value()
	originalType := node.Type()

	// Convert ZetaSQL value to zetasqlite Value
	zetasqliteValue, err := ValueFromZetaSQLValue(originalValue)
	if err != nil {
		return ExpressionData{}, fmt.Errorf("failed to convert ZetaSQL value to zetasqlite Value: %w", err)
	}

	var typeName string
	if originalType != nil {
		typeName = originalType.Kind().String()
	}

	return ExpressionData{
		Type: ExpressionTypeLiteral,
		Literal: &LiteralData{
			Value:    zetasqliteValue,
			TypeName: typeName,
		},
	}, nil
}

func getFuncName(ctx context.Context, n ast.Node) (string, error) {
	nodeMap := nodeMapFromContext(ctx)
	found := nodeMap.FindNodeFromResolvedNode(n)
	if len(found) == 0 {
		return "", fmt.Errorf("failed to find path node from function node %T", n)
	}
	var foundCallNode *parsed_ast.FunctionCallNode
	for _, node := range found {
		fcallNode, ok := node.(*parsed_ast.FunctionCallNode)
		if !ok {
			continue
		}
		foundCallNode = fcallNode
		break
	}
	if foundCallNode == nil {
		return "", fmt.Errorf("failed to find function call node from %T", n)
	}
	path, err := getPathFromNode(foundCallNode.Function())
	if err != nil {
		return "", fmt.Errorf("failed to find path: %w", err)
	}
	namePath := namePathFromContext(ctx)
	return namePath.format(path), nil
}

func getZetasqliteFuncName(ctx context.Context, node *ast.BaseFunctionCallNode, isWindowFunc bool) (string, error) {
	funcName := node.Function().FullName(false)
	funcName = strings.ReplaceAll(funcName, ".", "_")

	_, existsCurrentTimeFunc := currentTimeFuncMap[funcName]
	_, existsNormalFunc := normalFuncMap[funcName]
	_, existsAggregateFunc := aggregateFuncMap[funcName]
	_, existsWindowFunc := windowFuncMap[funcName]

	funcPrefix := "zetasqlite"
	if node.ErrorMode() == ast.SafeErrorMode {
		if !existsNormalFunc {
			return "", fmt.Errorf("SAFE is not supported for function %s", funcName)
		}
		funcPrefix = "zetasqlite_safe"
	}

	if strings.HasPrefix(funcName, "$") {
		if isWindowFunc {
			funcName = fmt.Sprintf("%s_window_%s", funcPrefix, funcName[1:])
		} else {
			funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName[1:])
		}
	} else if existsCurrentTimeFunc {
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if existsNormalFunc {
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if !isWindowFunc && existsAggregateFunc {
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if isWindowFunc && existsWindowFunc {
		funcName = fmt.Sprintf("%s_window_%s", funcPrefix, funcName)
	} else {
		if node.Function().IsZetaSQLBuiltin() {
			return "", fmt.Errorf("%s function is unimplemented", funcName)
		}
		fname, err := getFuncName(ctx, node)
		if err != nil {
			return "", err
		}
		funcName = fname
	}

	return funcName, nil
}

// extractFunctionCallData extracts data from function call nodes
func (e *NodeExtractor) extractFunctionCallData(node *ast.BaseFunctionCallNode, ctx TransformContext, isWindowFunc bool) (ExpressionData, error) {
	// Extract function name
	funcName, err := getZetasqliteFuncName(ctx.Context(), node, isWindowFunc)
	if err != nil {
		return ExpressionData{}, fmt.Errorf("failed to get function name: %w", err)
	}

	// Extract arguments
	arguments := make([]ExpressionData, 0, len(node.ArgumentList()))
	signature := &FunctionSignature{
		Arguments: []*ArgumentInfo{},
	}
	for _, arg := range node.ArgumentList() {
		argData, err := e.ExtractExpressionData(arg, ctx)
		if err != nil {
			return ExpressionData{}, fmt.Errorf("failed to extract function argument: %w", err)
		}
		arguments = append(arguments, argData)
	}

	for _, arg := range node.Signature().Arguments() {
		argInfo := &ArgumentInfo{Type: arg.Type()}
		if arg.HasArgumentName() {
			argInfo.Name = arg.ArgumentName()
		}
		signature.Arguments = append(signature.Arguments, argInfo)
	}

	return ExpressionData{
		Type: ExpressionTypeFunction,
		Function: &FunctionCallData{
			Name:      funcName,
			Arguments: arguments,
			Signature: signature,
		},
	}, nil
}

// extractCastData extracts data from cast nodes
func (e *NodeExtractor) extractCastData(node *ast.CastNode, ctx TransformContext) (ExpressionData, error) {
	exprData, err := e.ExtractExpressionData(node.Expr(), ctx)
	if err != nil {
		return ExpressionData{}, fmt.Errorf("failed to extract cast expression: %w", err)
	}

	return ExpressionData{
		Type: ExpressionTypeCast,
		Cast: &CastData{
			Expression:      exprData,
			FromType:        node.Expr().Type(),
			ToType:          node.Type(),
			ReturnNullOnErr: node.ReturnNullOnError(),
		},
	}, nil
}

// extractColumnRefData extracts data from column reference nodes
func (e *NodeExtractor) extractColumnRefData(node *ast.ColumnRefNode, ctx TransformContext) (ExpressionData, error) {
	return NewColumnExpressionData(node.Column()), nil
}

// extractSubqueryData extracts data from subquery nodes
func (e *NodeExtractor) extractSubqueryData(node *ast.SubqueryExprNode, ctx TransformContext) (ExpressionData, error) {
	// Extract the subquery statement data
	stmtData, err := e.ExtractScanData(node.Subquery(), ctx)
	if err != nil {
		return ExpressionData{}, fmt.Errorf("failed to extract subquery statement: %w", err)
	}

	subqueryData := &SubqueryData{
		Query:        stmtData,
		SubqueryType: node.SubqueryType(),
	}

	// Extract IN expression if present
	if node.InExpr() != nil {
		inExprData, err := e.ExtractExpressionData(node.InExpr(), ctx)
		if err != nil {
			return ExpressionData{}, fmt.Errorf("failed to extract IN expression: %w", err)
		}
		subqueryData.InExpr = &inExprData
	}

	return ExpressionData{
		Type:     ExpressionTypeSubquery,
		Subquery: subqueryData,
	}, nil
}

// extractParameterData extracts data from parameter nodes
func (e *NodeExtractor) extractParameterData(node *ast.ParameterNode, ctx TransformContext) (ExpressionData, error) {
	paramName := node.Name()
	if paramName == "" {
		paramName = "?" // Positional parameter
	} else {
		paramName = "@" + paramName // Named parameter
	}

	return ExpressionData{
		Type: ExpressionTypeParameter,
		Parameter: &ParameterData{
			Identifier: paramName,
		},
	}, nil
}

// extractArgumentRefData extracts data from argument reference nodes
func (e *NodeExtractor) extractArgumentRefData(node *ast.ArgumentRefNode, ctx TransformContext) (ExpressionData, error) {
	return ExpressionData{
		Type: ExpressionTypeParameter,
		Parameter: &ParameterData{
			Identifier: "@" + node.Name(),
		},
	}, nil
}

// extractDMLDefaultData extracts data from DML default nodes
func (e *NodeExtractor) extractDMLDefaultData(node *ast.DMLDefaultNode, ctx TransformContext) (ExpressionData, error) {
	return ExpressionData{
		Type: ExpressionTypeLiteral,
		Literal: &LiteralData{
			// DEFAULT keyword representation
		},
	}, nil
}

// Additional extraction methods for complex nodes...

// extractMakeStructData extracts data from STRUCT constructor nodes
func (e *NodeExtractor) extractMakeStructData(node *ast.MakeStructNode, ctx TransformContext) (ExpressionData, error) {
	// Extract field expressions
	fieldArgs := make([]ExpressionData, 0)
	structType := node.Type().AsStruct()

	for i, field := range node.FieldList() {
		// Add field name as literal
		fieldName := structType.Field(i).Name()

		fieldArgs = append(fieldArgs, ExpressionData{
			Type: ExpressionTypeLiteral,
			Literal: &LiteralData{
				Value:    StringValue(fieldName),
				TypeName: "STRING",
			},
		})

		// Add field expression
		fieldExpr, err := e.ExtractExpressionData(field, ctx)
		if err != nil {
			return ExpressionData{}, fmt.Errorf("failed to extract struct field: %w", err)
		}
		fieldArgs = append(fieldArgs, fieldExpr)
	}

	return ExpressionData{
		Type: ExpressionTypeFunction,
		Function: &FunctionCallData{
			Name:      "zetasqlite_make_struct",
			Arguments: fieldArgs,
		},
	}, nil
}

// extractGetStructFieldData extracts data from STRUCT field access nodes
func (e *NodeExtractor) extractGetStructFieldData(node *ast.GetStructFieldNode, ctx TransformContext) (ExpressionData, error) {
	exprData, err := e.ExtractExpressionData(node.Expr(), ctx)
	if err != nil {
		return ExpressionData{}, fmt.Errorf("failed to extract struct expression: %w", err)
	}

	// Get the field index from the node
	fieldIndex := node.FieldIdx()

	return ExpressionData{
		Type: ExpressionTypeFunction,
		Function: &FunctionCallData{
			Name: "zetasqlite_get_struct_field",
			Arguments: []ExpressionData{
				exprData,
				{
					Type: ExpressionTypeLiteral,
					Literal: &LiteralData{
						Value:    IntValue(int64(fieldIndex)),
						TypeName: "INT64",
					},
				},
			},
		},
	}, nil
}

// extractGetJsonFieldData extracts data from JSON field access nodes
func (e *NodeExtractor) extractGetJsonFieldData(node *ast.GetJsonFieldNode, ctx TransformContext) (ExpressionData, error) {
	exprData, err := e.ExtractExpressionData(node.Expr(), ctx)
	if err != nil {
		return ExpressionData{}, fmt.Errorf("failed to extract JSON expression: %w", err)
	}

	return ExpressionData{
		Type: ExpressionTypeFunction,
		Function: &FunctionCallData{
			Name: "zetasqlite_get_json_field",
			Arguments: []ExpressionData{
				exprData,
				{
					Type: ExpressionTypeLiteral,
					Literal: &LiteralData{
						TypeName: "STRING",
						Value:    StringValue(node.FieldName()),
					},
				},
			},
		},
	}, nil
}

// extractAggregateFunctionCallData extracts data from aggregate function nodes
func (e *NodeExtractor) extractAggregateFunctionCallData(node *ast.AggregateFunctionCallNode, ctx TransformContext) (ExpressionData, error) {
	// Similar to regular function call but with aggregate-specific handling

	baseData, err := e.extractFunctionCallData(node.BaseFunctionCallNode, ctx, false)
	if err != nil {
		return ExpressionData{}, fmt.Errorf("failed to extract aggregate function call function: %w", err)
	}
	function := baseData.Function

	for _, item := range node.OrderByItemList() {
		orderItem, err := e.ExtractExpressionData(item.ColumnRef(), ctx)
		if err != nil {
			return ExpressionData{}, fmt.Errorf("failed to extract aggregate function call function order by arg: %w", err)
		}
		orderBy := NewFunctionCallExpressionData(
			"zetasqlite_order_by",
			orderItem,
			ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: BoolValue(!item.IsDescending())}},
		)

		function.Arguments = append(function.Arguments, orderBy)
	}

	if node.Distinct() {
		function.Arguments = append(function.Arguments, NewFunctionCallExpressionData("zetasqlite_distinct"))
	}

	if node.Limit() != nil {
		limit, err := e.ExtractExpressionData(node.Limit(), ctx)
		if err != nil {
			return ExpressionData{}, fmt.Errorf("failed to extract aggregate function call function limit: %w", err)
		}

		function.Arguments = append(function.Arguments, NewFunctionCallExpressionData("zetasqlite_limit", limit))
	}

	switch node.NullHandlingModifier() {
	case ast.IgnoreNulls:
		function.Arguments = append(function.Arguments, NewFunctionCallExpressionData("zetasqlite_ignore_nulls"))
	case ast.RespectNulls:
	}

	return baseData, nil
}

func (e *NodeExtractor) getWindowBoundaryOptionFuncSQL(node *ast.WindowFrameNode, ctx TransformContext) (*FrameClauseData, error) {
	if node == nil {
		return &FrameClauseData{
			Unit:  "ROWS",
			Start: &FrameBoundData{Type: "UNBOUNDED PRECEDING"},
			End:   &FrameBoundData{Type: "UNBOUNDED FOLLOWING"},
		}, nil
	}

	frameNodes := [2]*ast.WindowFrameExprNode{node.StartExpr(), node.EndExpr()}
	frames := make([]FrameBoundData, 0, 2)
	for _, expr := range frameNodes {
		typ := expr.BoundaryType()
		switch typ {
		case ast.UnboundedPrecedingType, ast.CurrentRowType, ast.UnboundedFollowingType:
			frames = append(frames, getWindowBoundaryTypeData(typ, ExpressionData{}))
		case ast.OffsetPrecedingType, ast.OffsetFollowingType:
			literal, err := e.ExtractExpressionData(expr.Expression(), ctx)
			if err != nil {
				return nil, err
			}
			frames = append(frames, getWindowBoundaryTypeData(typ, literal))
		default:
			return nil, fmt.Errorf("unexpected boundary type %d", typ)
		}
	}
	var unit string
	switch node.FrameUnit() {
	case ast.FrameUnitRows:
		unit = "ROWS"
	case ast.FrameUnitRange:
		unit = "RANGE"
	default:
		return nil, fmt.Errorf("unexpected frame unit %d", node.FrameUnit())
	}
	return &FrameClauseData{Unit: unit, Start: &frames[0], End: &frames[1]}, nil
}

func getWindowBoundaryTypeData(boundaryType ast.BoundaryType, literal ExpressionData) FrameBoundData {
	switch boundaryType {
	case ast.UnboundedPrecedingType:
		return FrameBoundData{Type: "UNBOUNDED PRECEDING"}
	case ast.OffsetPrecedingType:
		return FrameBoundData{Type: "PRECEDING", Offset: literal}
	case ast.CurrentRowType:
		return FrameBoundData{Type: "CURRENT ROW"}
	case ast.OffsetFollowingType:
		return FrameBoundData{Type: "FOLLOWING", Offset: literal}
	case ast.UnboundedFollowingType:
		return FrameBoundData{Type: "UNBOUNDED FOLLOWING"}
	}
	return FrameBoundData{}
}

var windowFuncFixedRanges = map[string]*FrameClauseData{
	"zetasqlite_window_ntile": {
		Unit:  "ROWS",
		Start: &FrameBoundData{Type: "CURRENT ROW"},
		End:   &FrameBoundData{Type: "UNBOUNDED FOLLOWING"},
	},
	"zetasqlite_window_cume_dist": {
		Unit: "GROUPS",
		Start: &FrameBoundData{Type: "FOLLOWING",
			Offset: ExpressionData{
				Type:    ExpressionTypeLiteral,
				Literal: &LiteralData{Value: IntValue(1)},
			},
		},
		End: &FrameBoundData{Type: "UNBOUNDED FOLLOWING"},
	},
	"zetasqlite_window_dense_rank": {
		Unit:  "RANGE",
		Start: &FrameBoundData{Type: "UNBOUNDED PRECEDING"},
		End:   &FrameBoundData{Type: "CURRENT ROW"},
	},
	"zetasqlite_window_rank": {
		Unit:  "GROUPS",
		Start: &FrameBoundData{Type: "UNBOUNDED PRECEDING"},
		End:   &FrameBoundData{Type: "CURRENT ROW EXCLUDE TIES"},
	},
	"zetasqlite_window_percent_rank": {
		Unit:  "GROUPS",
		Start: &FrameBoundData{Type: "CURRENT ROW"},
		End:   &FrameBoundData{Type: "UNBOUNDED FOLLOWING"},
	},
	"zetasqlite_window_row_number": {
		Unit:  "ROWS",
		Start: &FrameBoundData{Type: "UNBOUNDED PRECEDING"},
		End:   &FrameBoundData{Type: "CURRENT ROW"},
	},
	"zetasqlite_window_lag": {
		Unit:  "ROWS",
		Start: &FrameBoundData{Type: "UNBOUNDED PRECEDING"},
		End:   &FrameBoundData{Type: "CURRENT ROW"},
	},
	"zetasqlite_window_lead": {
		Unit:  "ROWS",
		Start: &FrameBoundData{Type: "CURRENT ROW"},
		End:   &FrameBoundData{Type: "UNBOUNDED FOLLOWING"},
	},
}

var windowFunctionsIgnoreNullsByDefault = map[string]bool{
	"zetasqlite_window_percentile_disc": true,
}

// extractAnalyticFunctionCallData extracts data from analytic function nodes
func (e *NodeExtractor) extractAnalyticFunctionCallData(node *ast.AnalyticFunctionCallNode, ctx TransformContext) (ExpressionData, error) {
	// Extract the base function call
	baseData, err := e.extractFunctionCallData(node.BaseFunctionCallNode, ctx, true)
	if err != nil {
		return ExpressionData{}, fmt.Errorf("failed to extract analytic function base: %w", err)
	}
	function := baseData.Function

	if node.Distinct() {
		function.Arguments = append(function.Arguments, NewFunctionCallExpressionData("zetasqlite_distinct"))
	}

	_, ignoreNullsByDefault := windowFunctionsIgnoreNullsByDefault[baseData.Function.Name]

	switch node.NullHandlingModifier() {
	case ast.IgnoreNulls:
		function.Arguments = append(function.Arguments, NewFunctionCallExpressionData("zetasqlite_ignore_nulls"))
	case ast.DefaultNullHandling:
		if ignoreNullsByDefault {
			function.Arguments = append(function.Arguments, NewFunctionCallExpressionData("zetasqlite_ignore_nulls"))
		}
	}

	frame := node.WindowFrame()
	frameClause, found := windowFuncFixedRanges[function.Name]
	if found && frame != nil {
		return ExpressionData{}, fmt.Errorf("%s: window framing clause is not allowed for analytic function", node.BaseFunctionCallNode.Function().Name())
	}
	if !found {
		frameClause, err = e.getWindowBoundaryOptionFuncSQL(node.WindowFrame(), ctx)
		if err != nil {
			return ExpressionData{}, err
		}
	}

	// Ordering and partitioning comes from the AnalyticFunctionGroupNode; omit it here
	baseData.Function.WindowSpec = &WindowSpecificationData{
		FrameClause: frameClause,
	}

	return baseData, nil
}

// ExtractStatementData extracts pure data from statement AST nodes
func (e *NodeExtractor) ExtractStatementData(node ast.Node, ctx TransformContext) (StatementData, error) {
	if node == nil {
		return StatementData{}, fmt.Errorf("cannot extract data from nil statement node")
	}

	switch n := node.(type) {
	case *ast.QueryStmtNode:
		return e.extractQueryStatementData(n, ctx)
	case *ast.CreateTableStmtNode:
		return e.extractCreateTableStatementData(n, ctx)
	case *ast.CreateTableAsSelectStmtNode:
		return e.extractCreateTableAsSelectStatementData(n, ctx)
	case *ast.CreateViewStmtNode:
		return e.extractCreateViewStatementData(n, ctx)
	case *ast.InsertStmtNode:
		return e.extractInsertStatementData(n, ctx)
	case *ast.UpdateStmtNode:
		return e.extractUpdateStatementData(n, ctx)
	case *ast.DeleteStmtNode:
		return e.extractDeleteStatementData(n, ctx)
	case *ast.MergeStmtNode:
		return e.extractMergeStatementData(n, ctx)
	case *ast.DropStmtNode:
		return e.extractDropStatementData(n, ctx)
	case *ast.DropFunctionStmtNode:
		return e.extractDropFunctionStatementData(n, ctx)
	default:
		return StatementData{}, fmt.Errorf("unsupported statement node type: %T", node)
	}
}

// extractQueryStatementData extracts data from query statements
func (e *NodeExtractor) extractQueryStatementData(node *ast.QueryStmtNode, ctx TransformContext) (StatementData, error) {
	// Extract the main query scan
	scanData, err := e.ExtractScanData(node.Query(), ctx)
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract query scan: %w", err)
	}

	// Extract output column information
	selectItems := make([]*SelectItemData, 0, len(node.OutputColumnList()))
	for _, col := range node.OutputColumnList() {
		exprData, err := e.ExtractExpressionData(col, ctx)
		if err != nil {
			return StatementData{}, fmt.Errorf("failed to extract output column: %w", err)
		}

		selectItems = append(selectItems, &SelectItemData{
			Expression: exprData,
			Alias:      col.Name(),
		})
	}

	return StatementData{
		Type: StatementTypeSelect,
		Select: &SelectData{
			SelectList: selectItems,
			FromClause: &scanData,
		},
	}, nil
}

// ExtractScanData extracts pure data from scan AST nodes
func (e *NodeExtractor) ExtractScanData(node ast.Node, ctx TransformContext) (ScanData, error) {
	if node == nil {
		return ScanData{}, fmt.Errorf("cannot extract data from nil scan node")
	}

	switch n := node.(type) {
	case *ast.TableScanNode:
		return e.extractTableScanData(n, ctx)
	case *ast.JoinScanNode:
		return e.extractJoinScanData(n, ctx)
	case *ast.FilterScanNode:
		return e.extractFilterScanData(n, ctx)
	case *ast.ProjectScanNode:
		return e.extractProjectScanData(n, ctx)
	case *ast.AggregateScanNode:
		return e.extractAggregateScanData(n, ctx)
	case *ast.OrderByScanNode:
		return e.extractOrderByScanData(n, ctx)
	case *ast.LimitOffsetScanNode:
		return e.extractLimitScanData(n, ctx)
	case *ast.SingleRowScanNode:
		return e.extractSingleRowScanData(n, ctx)
	case *ast.WithScanNode:
		return e.extractWithScanNode(n, ctx)
	case *ast.WithRefScanNode:
		return e.extractWithRefScanNode(n, ctx)
	case *ast.SetOperationScanNode:
		return e.extractSetOperationScanData(n, ctx)
	case *ast.ArrayScanNode:
		return e.extractArrayScanData(n, ctx)
	case *ast.AnalyticScanNode:
		return e.extractAnalyticScanData(n, ctx)
	default:
		return ScanData{}, fmt.Errorf("unsupported scan node type: %T", node)
	}
}

func getPathFromNode(n parsed_ast.Node) ([]string, error) {
	var path []string
	switch node := n.(type) {
	case *parsed_ast.IdentifierNode:
		path = append(path, node.Name())
	case *parsed_ast.PathExpressionNode:
		for _, name := range node.Names() {
			path = append(path, name.Name())
		}
	case *parsed_ast.TablePathExpressionNode:
		switch {
		case node.PathExpr() != nil:
			for _, name := range node.PathExpr().Names() {
				path = append(path, name.Name())
			}
		}
	default:
		return nil, fmt.Errorf("found unknown path node: %T", node)
	}
	return path, nil
}

func getTableName(ctx context.Context, n ast.Node) (string, error) {
	nodeMap := nodeMapFromContext(ctx)
	found := nodeMap.FindNodeFromResolvedNode(n)
	if len(found) == 0 {
		return "", fmt.Errorf("failed to find path node from table node %T", n)
	}
	path, err := getPathFromNode(found[0])
	if err != nil {
		return "", fmt.Errorf("failed to find path: %w", err)
	}
	namePath := namePathFromContext(ctx)
	return namePath.format(path), nil
}

// extractTableScanData extracts data from table scan nodes
func (e *NodeExtractor) extractTableScanData(node *ast.TableScanNode, ctx TransformContext) (ScanData, error) {
	// Check if this is a wildcard table
	table := node.Table()
	if wildcardTable, isWildcard := table.(*WildcardTable); isWildcard {
		// Extract wildcard table data as a SetOp (UNION ALL)
		return e.extractWildcardTableAsSetOp(wildcardTable, node, ctx)
	}

	tableName, err := getTableName(ctx.Context(), node)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract table name from table node %T: %w", node, err)
	}

	return ScanData{
		Type:       ScanTypeTable,
		ColumnList: extractColumnDataList(node.ColumnList()),
		TableScan: &TableScanData{
			TableName: tableName,
			Alias:     node.Alias(),
		},
	}, nil
}

// extractWildcardTableAsSetOp converts a wildcard table to a SetOp (UNION ALL) structure
func (e *NodeExtractor) extractWildcardTableAsSetOp(wildcardTable *WildcardTable, node *ast.TableScanNode, ctx TransformContext) (ScanData, error) {
	// Create individual SELECT statements for each table matched by the wildcard
	items := make([]StatementData, 0, len(wildcardTable.tables))

	columnData := extractColumnDataList(node.ColumnList())
	columnIdsByName := make(map[string]int)
	for _, col := range columnData {
		columnIdsByName[col.Name] = col.ID
	}

	for _, tableSpec := range wildcardTable.tables {
		// Create select items based on the wildcard table's column specification
		selectItems := make([]*SelectItemData, 0, len(wildcardTable.spec.Columns))

		for _, col := range wildcardTable.spec.Columns {
			if col.Name == tableSuffixColumnName {
				// Handle _TABLE_SUFFIX column by calculating the suffix from the table name
				fullName := tableSpec.TableName()
				var tableSuffix string
				if len(fullName) > len(wildcardTable.prefix) {
					tableSuffix = fullName[len(wildcardTable.prefix):]
				} else {
					tableSuffix = ""
				}

				// Create a literal expression for the table suffix
				selectItems = append(selectItems, &SelectItemData{
					Expression: ExpressionData{
						Type: ExpressionTypeLiteral,
						Literal: &LiteralData{
							Value:    StringValue(tableSuffix),
							TypeName: "STRING",
						},
					},
					Alias: tableSuffixColumnName,
				})
			} else {
				// Check if this column exists in the current table
				var columnExpr ExpressionData
				if wildcardTable.existsColumn(tableSpec, col.Name) {
					// Column exists - reference it directly
					columnExpr = ExpressionData{
						Type: ExpressionTypeColumn,
						Column: &ColumnRefData{
							ColumnID:   columnIdsByName[col.Name],
							ColumnName: col.Name,
							TableName:  tableSpec.TableName(),
						},
					}
				} else {
					// Column doesn't exist - use NULL
					columnExpr = ExpressionData{
						Type: ExpressionTypeLiteral,
						Literal: &LiteralData{
							Value: nil,
						},
					}
				}

				selectItems = append(selectItems, &SelectItemData{
					Expression: columnExpr,
					Alias:      col.Name,
				})
			}
		}

		tableScanData := ScanData{
			Type:       ScanTypeTable,
			ColumnList: []*ColumnData{},
			TableScan: &TableScanData{
				TableName:        tableSpec.TableName(),
				SyntheticColumns: selectItems,
				Alias:            "", // No alias for individual tables in wildcard
			},
		}

		// Create a SELECT statement for this table
		stmtData := StatementData{
			Type: StatementTypeSelect,
			Select: &SelectData{
				SelectList: selectItems,
				FromClause: &tableScanData,
			},
		}

		items = append(items, stmtData)
	}

	return ScanData{
		Type:       ScanTypeSetOp,
		ColumnList: columnData,
		SetOperationScan: &SetOperationData{
			Type:     "UNION",
			Modifier: "ALL",
			Items:    items,
		},
	}, nil
}

// Additional scan extraction methods would be implemented here...

// extractJoinScanData extracts data from join scan nodes
func (e *NodeExtractor) extractJoinScanData(node *ast.JoinScanNode, ctx TransformContext) (ScanData, error) {
	leftScan, err := e.ExtractScanData(node.LeftScan(), ctx)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract left scan: %w", err)
	}

	rightScan, err := e.ExtractScanData(node.RightScan(), ctx)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract right scan: %w", err)
	}

	var joinCondition *ExpressionData
	if node.JoinExpr() != nil {
		conditionData, err := e.ExtractExpressionData(node.JoinExpr(), ctx)
		if err != nil {
			return ScanData{}, fmt.Errorf("failed to extract join condition: %w", err)
		}
		joinCondition = &conditionData
	}

	return ScanData{
		Type:       ScanTypeJoin,
		ColumnList: extractColumnDataList(node.ColumnList()),
		JoinScan: &JoinScanData{
			JoinType:      node.JoinType(),
			LeftScan:      leftScan,
			RightScan:     rightScan,
			JoinCondition: joinCondition,
		},
	}, nil
}

// Placeholder implementations for remaining extraction methods
func (e *NodeExtractor) extractFilterScanData(node *ast.FilterScanNode, ctx TransformContext) (ScanData, error) {
	// Extract the input scan data recursively
	inputScanData, err := e.ExtractScanData(node.InputScan(), ctx)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract input scan for filter: %w", err)
	}

	// Extract the filter expression data
	filterExprData, err := e.ExtractExpressionData(node.FilterExpr(), ctx)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract filter expression: %w", err)
	}

	return ScanData{
		Type:       ScanTypeFilter,
		ColumnList: extractColumnDataList(node.ColumnList()),
		FilterScan: &FilterScanData{
			InputScan:  inputScanData,
			FilterExpr: filterExprData,
		},
	}, nil
}

func (e *NodeExtractor) extractProjectScanData(node *ast.ProjectScanNode, ctx TransformContext) (ScanData, error) {
	inputScanData, err := e.ExtractScanData(node.InputScan(), ctx)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract input scan for project: %w", err)
	}

	// Extract projection expressions
	computedColumns := make([]*ComputedColumnData, 0, len(node.ExprList()))
	for _, expr := range node.ExprList() {
		exprData, err := e.ExtractExpressionData(expr, ctx)
		if err != nil {
			return ScanData{}, fmt.Errorf("failed to extract projection expression: %w", err)
		}

		computedColumns = append(computedColumns, &ComputedColumnData{
			Column:     expr.Column(),
			Expression: exprData,
		})
	}

	return ScanData{
		Type:       ScanTypeProject,
		ColumnList: extractColumnDataList(node.ColumnList()),
		ProjectScan: &ProjectScanData{
			InputScan: inputScanData, // The nested scan structure!
			ExprList:  computedColumns,
		},
	}, nil
}

func (e *NodeExtractor) extractAggregateScanData(node *ast.AggregateScanNode, ctx TransformContext) (ScanData, error) {
	// Extract the input scan data recursively
	inputScanData, err := e.ExtractScanData(node.InputScan(), ctx)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract input scan for aggregate: %w", err)
	}

	// Extract aggregate expressions
	aggregateList := make([]*ComputedColumnData, 0, len(node.AggregateList()))
	for _, agg := range node.AggregateList() {
		exprData, err := e.ExtractExpressionData(agg, ctx)
		if err != nil {
			return ScanData{}, fmt.Errorf("failed to extract aggregate expression: %w", err)
		}

		aggregateList = append(aggregateList, &ComputedColumnData{
			Column:     agg.Column(),
			Expression: exprData,
		})
	}

	// Extract group by expressions
	groupByList := make([]*ComputedColumnData, 0, len(node.GroupByList()))
	for _, groupBy := range node.GroupByList() {
		exprData, err := e.ExtractExpressionData(groupBy, ctx)
		if err != nil {
			return ScanData{}, fmt.Errorf("failed to extract group by expression: %w", err)
		}

		groupByList = append(groupByList, &ComputedColumnData{
			Column:     groupBy.Column(),
			Expression: exprData,
		})
	}

	// Extract grouping sets if present
	groupingSets := make([]*GroupingSetData, 0, len(node.GroupingSetList()))
	for _, groupingSet := range node.GroupingSetList() {
		groupingSetItems := make([]*ComputedColumnData, 0, len(groupingSet.GroupByColumnList()))
		for _, groupByCol := range groupingSet.GroupByColumnList() {
			exprData, err := e.ExtractExpressionData(groupByCol, ctx)
			if err != nil {
				return ScanData{}, fmt.Errorf("failed to extract grouping set expression: %w", err)
			}

			groupingSetItems = append(groupingSetItems, &ComputedColumnData{
				Column:     groupByCol.Column(),
				Expression: exprData,
			})
		}

		groupingSets = append(groupingSets, &GroupingSetData{
			GroupByColumns: groupingSetItems,
		})
	}

	return ScanData{
		Type:       ScanTypeAggregate,
		ColumnList: extractColumnDataList(node.ColumnList()),
		AggregateScan: &AggregateScanData{
			InputScan:     inputScanData,
			GroupByList:   groupByList,
			AggregateList: aggregateList,
			GroupingSets:  groupingSets,
		},
	}, nil
}

func (e *NodeExtractor) extractOrderByScanData(node *ast.OrderByScanNode, ctx TransformContext) (ScanData, error) {
	inputScanData, err := e.ExtractScanData(node.InputScan(), ctx)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract input scan for order by: %w", err)
	}

	// Extract ORDER BY items data
	orderByItems := make([]*OrderByItemData, 0, len(node.OrderByItemList()))
	for _, itemNode := range node.OrderByItemList() {
		// Extract expression data
		exprData, err := e.ExtractExpressionData(itemNode.ColumnRef(), ctx)
		if err != nil {
			return ScanData{}, fmt.Errorf("failed to extract order by expression: %w", err)
		}

		orderByItems = append(orderByItems, &OrderByItemData{
			Expression:   exprData,
			IsDescending: itemNode.IsDescending(),
			NullOrder:    itemNode.NullOrder(),
		})
	}

	return ScanData{
		Type:       ScanTypeOrderBy,
		ColumnList: extractColumnDataList(node.ColumnList()),
		OrderByScan: &OrderByScanData{
			InputScan:      inputScanData,
			OrderByColumns: orderByItems,
		},
	}, nil
}

func (e *NodeExtractor) extractLimitScanData(node *ast.LimitOffsetScanNode, ctx TransformContext) (ScanData, error) {
	inputScanData, err := e.ExtractScanData(node.InputScan(), ctx)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract input scan for limit: %w", err)
	}

	// Extract limit expression data
	var limitExprData ExpressionData
	if node.Limit() != nil {
		limitData, err := e.ExtractExpressionData(node.Limit(), ctx)
		if err != nil {
			return ScanData{}, fmt.Errorf("failed to extract limit expression: %w", err)
		}
		limitExprData = limitData
	}

	// Extract offset expression data if present
	var offsetExprData ExpressionData
	if node.Offset() != nil {
		offsetData, err := e.ExtractExpressionData(node.Offset(), ctx)
		if err != nil {
			return ScanData{}, fmt.Errorf("failed to extract offset expression: %w", err)
		}
		offsetExprData = offsetData
	}

	return ScanData{
		Type:       ScanTypeLimit,
		ColumnList: extractColumnDataList(node.ColumnList()),
		LimitScan: &LimitScanData{
			InputScan: inputScanData, // The nested scan structure!
			Count:     limitExprData,
			Offset:    offsetExprData,
		},
	}, nil
}

func (e *NodeExtractor) extractWithScanNode(n *ast.WithScanNode, ctx TransformContext) (ScanData, error) {
	query, err := e.ExtractScanData(n.Query(), ctx)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract with query data: %w", err)
	}

	// Extract all WITH entries
	withEntryList := make([]*WithEntryData, 0, len(n.WithEntryList()))
	for _, entry := range n.WithEntryList() {
		entryData, err := e.extractWithEntryData(entry, ctx)
		if err != nil {
			return ScanData{}, fmt.Errorf("failed to extract WITH entry: %w", err)
		}
		withEntryList = append(withEntryList, entryData)
	}

	return ScanData{
		Type:       ScanTypeWith,
		ColumnList: extractColumnDataList(n.ColumnList()),
		WithScan: &WithScanData{
			WithEntryList: withEntryList,
			Query:         query,
		},
	}, nil
}

func (e *NodeExtractor) extractWithEntryData(node *ast.WithEntryNode, ctx TransformContext) (*WithEntryData, error) {
	subquery, err := e.ExtractScanData(node.WithSubquery(), ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract WITH entry subquery: %w", err)
	}

	return &WithEntryData{
		WithQueryName: node.WithQueryName(),
		WithSubquery:  subquery,
		ColumnList:    extractColumnDataList(node.WithSubquery().ColumnList()),
	}, nil
}

func (e *NodeExtractor) extractWithRefScanNode(node *ast.WithRefScanNode, ctx TransformContext) (ScanData, error) {
	return ScanData{
		Type:       ScanTypeWithRef,
		ColumnList: extractColumnDataList(node.ColumnList()),
		WithRefScan: &WithRefScanData{
			WithQueryName: node.WithQueryName(),
			ColumnList:    extractColumnDataList(node.ColumnList()),
		},
	}, nil
}

func (e *NodeExtractor) extractSetOperationScanData(node *ast.SetOperationScanNode, ctx TransformContext) (ScanData, error) {
	// Map ZetaSQL set operation types to SQLite equivalents
	var opType string
	var modifier string
	switch node.OpType() {
	case ast.SetOperationTypeUnionAll:
		opType = "UNION"
		modifier = "ALL"
	case ast.SetOperationTypeUnionDistinct:
		opType = "UNION"
	case ast.SetOperationTypeIntersectAll:
		opType = "INTERSECT"
		modifier = "ALL"
	case ast.SetOperationTypeIntersectDistinct:
		opType = "INTERSECT"
	case ast.SetOperationTypeExceptAll:
		opType = "EXCEPT"
		modifier = "ALL"
	case ast.SetOperationTypeExceptDistinct:
		opType = "EXCEPT"
	default:
		opType = "UNKNOWN"
	}

	// Get the final output column names from the SetOperationScan.ColumnList()
	outputColumnNames := make([]string, 0, len(node.ColumnList()))
	for _, col := range node.ColumnList() {
		outputColumnNames = append(outputColumnNames, col.Name())
	}

	// Extract all input items (subqueries) in the set operation
	items := make([]StatementData, 0, len(node.InputItemList()))
	for i, item := range node.InputItemList() {
		// Extract the scan data from each input item
		scanData, err := e.ExtractScanData(item.Scan(), ctx)
		if err != nil {
			return ScanData{}, fmt.Errorf("failed to extract scan for set operation item %d: %w", i, err)
		}

		selectItems := make([]*SelectItemData, 0, len(item.OutputColumnList()))
		for j, column := range item.OutputColumnList() {
			// Use the final output column name instead of the item's internal column name
			var aliasName string
			if j < len(outputColumnNames) {
				aliasName = outputColumnNames[j]
			} else {
				aliasName = column.Name() // fallback
			}

			selectItems = append(selectItems, &SelectItemData{
				Expression: NewColumnExpressionData(column),
				Alias:      aliasName, // Use the consistent output column name
			})
		}

		// Wrap the scan data in a SELECT statement data
		stmtData := StatementData{
			Type: StatementTypeSelect,
			Select: &SelectData{
				SelectList: selectItems,
				FromClause: &scanData,
			},
		}

		items = append(items, stmtData)
	}

	return ScanData{
		Type:       ScanTypeSetOp,
		ColumnList: extractColumnDataList(node.ColumnList()),
		SetOperationScan: &SetOperationData{
			Type:     opType,
			Modifier: modifier,
			Items:    items,
		},
	}, nil
}

func (e *NodeExtractor) extractSingleRowScanData(node *ast.SingleRowScanNode, ctx TransformContext) (ScanData, error) {
	return ScanData{Type: ScanTypeSingleRow}, nil
}

// Placeholder implementations for remaining statement extraction methods
func (e *NodeExtractor) extractCreateTableStatementData(node *ast.CreateTableStmtNode, ctx TransformContext) (StatementData, error) {
	// TODO: Implement. Currently managed by spec.go Table.SQLiteSchema()
	return StatementData{}, fmt.Errorf("create table statement extraction not implemented")
}

func (e *NodeExtractor) extractInsertStatementData(node *ast.InsertStmtNode, ctx TransformContext) (StatementData, error) {
	// Extract table name
	tableName, err := getTableName(ctx.Context(), node.TableScan())
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract table name from table node %T: %w", node, err)
	}

	// Extract column names
	columns := make([]string, 0, len(node.InsertColumnList()))
	for _, col := range node.InsertColumnList() {
		columns = append(columns, col.Name())
	}

	insertData := &InsertData{
		TableName: tableName,
		Columns:   columns,
	}

	// Handle INSERT ... SELECT vs INSERT ... VALUES
	if node.Query() != nil {
		// This is an INSERT ... SELECT statement
		queryScanData, err := e.ExtractScanData(node.Query(), ctx)
		if err != nil {
			return StatementData{}, fmt.Errorf("failed to extract insert query scan: %w", err)
		}

		// Convert ScanData to SelectData for the query
		selectItems := make([]*SelectItemData, 0, len(queryScanData.ColumnList))
		for _, col := range node.Query().ColumnList() {
			selectItems = append(selectItems, &SelectItemData{
				Expression: NewColumnExpressionData(col),
				Alias:      col.Name(),
			})
		}

		insertData.Query = &SelectData{
			SelectList: selectItems,
			FromClause: &queryScanData,
		}
	} else {
		// This is an INSERT ... VALUES statement
		values := make([][]ExpressionData, 0, len(node.RowList()))
		for _, row := range node.RowList() {
			rowValues := make([]ExpressionData, 0, len(row.ValueList()))
			for _, value := range row.ValueList() {
				valueData, err := e.ExtractExpressionData(value, ctx)
				if err != nil {
					return StatementData{}, fmt.Errorf("failed to extract insert value: %w", err)
				}
				rowValues = append(rowValues, valueData)
			}
			values = append(values, rowValues)
		}
		insertData.Values = values
	}

	return StatementData{
		Type:   StatementTypeInsert,
		Insert: insertData,
	}, nil
}

func (e *NodeExtractor) extractUpdateStatementData(node *ast.UpdateStmtNode, ctx TransformContext) (StatementData, error) {
	// Extract table name from table scan
	tableName, err := getTableName(ctx.Context(), node.TableScan())
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract table name from table node %T: %w", node, err)
	}

	// Extract table scan data to provide column information for WHERE clause and SET value resolution
	tableScanData, err := e.ExtractScanData(node.TableScan(), ctx)
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract update table scan: %w", err)
	}

	// Extract SET items
	setItems := make([]*SetItemData, 0, len(node.UpdateItemList()))
	for _, item := range node.UpdateItemList() {
		// Extract target column
		targetData, err := e.ExtractExpressionData(item.Target(), ctx)
		if err != nil {
			return StatementData{}, fmt.Errorf("failed to extract update target: %w", err)
		}

		// Extract set value
		valueData, err := e.ExtractExpressionData(item.SetValue(), ctx)
		if err != nil {
			return StatementData{}, fmt.Errorf("failed to extract update value: %w", err)
		}

		// Get column name from target expression
		var columnName string
		if targetData.Type == ExpressionTypeColumn && targetData.Column != nil {
			columnName = targetData.Column.ColumnName
		} else {
			// Fallback to string representation
			columnName = "unknown_column"
		}

		setItems = append(setItems, &SetItemData{
			Column: columnName,
			Value:  valueData,
		})
	}

	// Extract WHERE clause if present
	var whereClause *ExpressionData
	if node.WhereExpr() != nil {
		whereData, err := e.ExtractExpressionData(node.WhereExpr(), ctx)
		if err != nil {
			return StatementData{}, fmt.Errorf("failed to extract update where clause: %w", err)
		}
		whereClause = &whereData
	}

	// Extract FROM clause if present (for JOINs in UPDATE)
	var fromClause *ScanData
	if node.FromScan() != nil {
		fromData, err := e.ExtractScanData(node.FromScan(), ctx)
		if err != nil {
			return StatementData{}, fmt.Errorf("failed to extract update from clause: %w", err)
		}
		fromClause = &fromData
	}

	return StatementData{
		Type: StatementTypeUpdate,
		Update: &UpdateData{
			TableName:   tableName,
			TableScan:   &tableScanData,
			SetItems:    setItems,
			FromClause:  fromClause,
			WhereClause: whereClause,
		},
	}, nil
}

func (e *NodeExtractor) extractDeleteStatementData(node *ast.DeleteStmtNode, ctx TransformContext) (StatementData, error) {
	// Extract table name from table scan
	tableName, err := getTableName(ctx.Context(), node.TableScan())
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract table name from table node %T: %w", node, err)
	}

	// Extract table scan data to provide column information for WHERE clause resolution
	tableScanData, err := e.ExtractScanData(node.TableScan(), ctx)
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract delete table scan: %w", err)
	}

	// Extract WHERE clause if present
	var whereClause *ExpressionData
	if node.WhereExpr() != nil {
		whereData, err := e.ExtractExpressionData(node.WhereExpr(), ctx)
		if err != nil {
			return StatementData{}, fmt.Errorf("failed to extract delete where clause: %w", err)
		}
		whereClause = &whereData
	}

	return StatementData{
		Type: StatementTypeDelete,
		Delete: &DeleteData{
			TableName:   tableName,
			TableScan:   &tableScanData,
			WhereClause: whereClause,
		},
	}, nil
}

// extractMergeStatementData extracts data from MERGE statement nodes
func (e *NodeExtractor) extractMergeStatementData(node *ast.MergeStmtNode, ctx TransformContext) (StatementData, error) {
	// Extract target table name
	targetTableName, err := getTableName(ctx.Context(), node.TableScan())
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract table name from table node %T: %w", node, err)
	}

	// Extract target table scan data
	targetScanData, err := e.ExtractScanData(node.TableScan(), ctx)
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract merge target scan: %w", err)
	}

	// Extract source scan data
	sourceScanData, err := e.ExtractScanData(node.FromScan(), ctx)
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract merge source scan: %w", err)
	}

	// Extract merge expression (typically an equality condition)
	mergeExprData, err := e.ExtractExpressionData(node.MergeExpr(), ctx)
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract merge expression: %w", err)
	}

	// Extract WHEN clauses
	whenClauses := make([]*MergeWhenClauseData, 0, len(node.WhenClauseList()))
	for _, when := range node.WhenClauseList() {
		whenData := &MergeWhenClauseData{
			MatchType:  when.MatchType(),
			ActionType: when.ActionType(),
		}

		// Extract condition if present
		if when.MatchExpr() != nil {
			conditionData, err := e.ExtractExpressionData(when.MatchExpr(), ctx)
			if err != nil {
				return StatementData{}, fmt.Errorf("failed to extract merge when condition: %w", err)
			}
			whenData.Condition = &conditionData
		}

		// Handle different action types
		switch when.ActionType() {
		case ast.ActionTypeInsert:
			// Extract INSERT columns
			columns := make([]*ColumnData, 0, len(when.InsertColumnList()))
			for _, col := range when.InsertColumnList() {
				columns = append(columns, &ColumnData{
					ID:   col.ColumnID(),
					Name: col.Name(),
				})
			}
			whenData.InsertColumns = columns

			// Extract INSERT values
			if when.InsertRow() != nil {
				values := make([]ExpressionData, 0, len(when.InsertRow().ValueList()))
				for _, value := range when.InsertRow().ValueList() {
					valueData, err := e.ExtractExpressionData(value, ctx)
					if err != nil {
						return StatementData{}, fmt.Errorf("failed to extract merge insert value: %w", err)
					}
					values = append(values, valueData)
				}
				whenData.InsertValues = values
			}

		case ast.ActionTypeUpdate:
			// Extract UPDATE SET items
			setItems := make([]*SetItemData, 0, len(when.UpdateItemList()))
			for _, item := range when.UpdateItemList() {
				// Extract target column name
				var columnName string
				if item.Target() != nil {
					if columnRef, ok := item.Target().(*ast.ColumnRefNode); ok {
						columnName = columnRef.Column().Name()
					}
				}

				// Extract set value
				valueData, err := e.ExtractExpressionData(item.SetValue(), ctx)
				if err != nil {
					return StatementData{}, fmt.Errorf("failed to extract merge update value: %w", err)
				}

				setItems = append(setItems, &SetItemData{
					Column: columnName,
					Value:  valueData,
				})
			}
			whenData.SetItems = setItems

		case ast.ActionTypeDelete:
			// DELETE action has no additional data to extract
		}

		whenClauses = append(whenClauses, whenData)
	}

	return StatementData{
		Type: StatementTypeMerge,
		Merge: &MergeData{
			TargetTable: targetTableName,
			TargetScan:  &targetScanData,
			SourceScan:  &sourceScanData,
			MergeExpr:   mergeExprData,
			WhenClauses: whenClauses,
		},
	}, nil
}

// extractArrayScanData extracts data from array scan (UNNEST) nodes
func (e *NodeExtractor) extractArrayScanData(node *ast.ArrayScanNode, ctx TransformContext) (ScanData, error) {
	// Extract the array expression to UNNEST
	arrayExprData, err := e.ExtractExpressionData(node.ArrayExpr(), ctx)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract array expression: %w", err)
	}

	// Extract optional input scan for correlated arrays
	var inputScanData *ScanData
	if node.InputScan() != nil {
		inputData, err := e.ExtractScanData(node.InputScan(), ctx)
		if err != nil {
			return ScanData{}, fmt.Errorf("failed to extract input scan for array: %w", err)
		}
		inputScanData = &inputData
	}

	// Extract element column data
	var elementColumnData *ColumnData
	if node.ElementColumn() != nil {
		elementColumnData = extractColumnData(node.ElementColumn())
	}

	// Extract optional array offset column data
	var arrayOffsetColumnData *ColumnData
	if node.ArrayOffsetColumn() != nil {
		arrayOffsetColumnData = extractColumnData(node.ArrayOffsetColumn().Column())
	}

	// Extract optional join expression
	var joinExprData *ExpressionData
	if node.JoinExpr() != nil {
		joinData, err := e.ExtractExpressionData(node.JoinExpr(), ctx)
		if err != nil {
			return ScanData{}, fmt.Errorf("failed to extract join expression: %w", err)
		}
		joinExprData = &joinData
	}

	return ScanData{
		Type:       ScanTypeArray,
		ColumnList: extractColumnDataList(node.ColumnList()),
		ArrayScan: &ArrayScanData{
			InputScan:         inputScanData,
			ArrayExpr:         arrayExprData,
			ElementColumn:     elementColumnData,
			ArrayOffsetColumn: arrayOffsetColumnData,
			IsOuter:           node.IsOuter(),
			JoinExpr:          joinExprData,
		},
	}, nil
}

// extractAnalyticScanData extracts data from analytic scan (window function) nodes
func (e *NodeExtractor) extractAnalyticScanData(node *ast.AnalyticScanNode, ctx TransformContext) (ScanData, error) {
	// Extract the input scan data recursively
	inputScanData, err := e.ExtractScanData(node.InputScan(), ctx)
	if err != nil {
		return ScanData{}, fmt.Errorf("failed to extract input scan for analytic: %w", err)
	}

	// Extract function list data from function groups
	functionList := make([]*ComputedColumnData, 0)
	for _, group := range node.FunctionGroupList() {
		// Extract PARTITION BY expressions from the group
		var groupPartitionBy []*ExpressionData
		if group.PartitionBy() != nil {
			for _, partExpr := range group.PartitionBy().PartitionByList() {
				partData, err := e.ExtractExpressionData(partExpr, ctx)
				if err != nil {
					return ScanData{}, fmt.Errorf("failed to extract partition by expression: %w", err)
				}
				groupPartitionBy = append(groupPartitionBy, &partData)
			}
		}

		// Extract ORDER BY expressions from the group
		var groupOrderBy []*OrderByItemData
		if group.OrderBy() != nil {
			for _, orderItem := range group.OrderBy().OrderByItemList() {
				orderExpr, err := e.ExtractExpressionData(orderItem.ColumnRef(), ctx)
				if err != nil {
					return ScanData{}, fmt.Errorf("failed to extract order by expression: %w", err)
				}
				groupOrderBy = append(groupOrderBy, &OrderByItemData{
					Expression:   orderExpr,
					IsDescending: orderItem.IsDescending(),
					NullOrder:    orderItem.NullOrder(),
				})
			}
		}

		for _, funcExpr := range group.AnalyticFunctionList() {
			exprData, err := e.ExtractExpressionData(funcExpr, ctx)
			if err != nil {
				return ScanData{}, fmt.Errorf("failed to extract analytic function expression: %w", err)
			}

			// If this function has window spec data, we need to merge the group's ORDER BY and PARTITION BY
			if exprData.Type == ExpressionTypeFunction && exprData.Function != nil && exprData.Function.WindowSpec != nil {
				windowSpec := exprData.Function.WindowSpec

				// Add group's PARTITION BY to the window spec
				if len(groupPartitionBy) > 0 {
					windowSpec.PartitionBy = groupPartitionBy
				}

				// Add group's ORDER BY to the window spec
				if len(groupOrderBy) > 0 {
					windowSpec.OrderBy = groupOrderBy
				}
			}

			functionList = append(functionList, &ComputedColumnData{
				Column:     funcExpr.Column(),
				Expression: exprData,
			})
		}
	}

	return ScanData{
		Type:       ScanTypeAnalytic,
		ColumnList: extractColumnDataList(node.ColumnList()),
		AnalyticScan: &AnalyticScanData{
			InputScan:    inputScanData,
			FunctionList: functionList,
		},
	}, nil
}

// extractDropStatementData extracts data from DROP statement nodes
func (e *NodeExtractor) extractDropStatementData(node *ast.DropStmtNode, ctx TransformContext) (StatementData, error) {
	var objectType string
	switch node.ObjectType() {
	case "TABLE":
		objectType = "TABLE"
	case "VIEW":
		objectType = "VIEW"
	case "INDEX":
		objectType = "INDEX"
	case "SCHEMA":
		objectType = "SCHEMA"
	default:
		objectType = node.ObjectType()
	}

	// Get the name path from context and format the object name
	var objectName string
	if namePath := namePathFromContext(ctx.Context()); namePath != nil {
		objectName = namePath.format(node.NamePath())
	} else {
		// Fallback to simple name formatting if no context
		if len(node.NamePath()) > 0 {
			objectName = node.NamePath()[len(node.NamePath())-1]
		}
	}

	return StatementData{
		Type: StatementTypeDrop,
		Drop: &DropData{
			IfExists:   node.IsIfExists(),
			ObjectType: objectType,
			ObjectName: objectName,
		},
	}, nil
}

// extractDropFunctionStatementData extracts data from DROP FUNCTION statement nodes
func (e *NodeExtractor) extractDropFunctionStatementData(node *ast.DropFunctionStmtNode, ctx TransformContext) (StatementData, error) {
	// Get the name path from context and format the function name
	var objectName string
	if namePath := namePathFromContext(ctx.Context()); namePath != nil {
		objectName = namePath.format(node.NamePath())
	} else {
		// Fallback to simple name formatting if no context
		if len(node.NamePath()) > 0 {
			objectName = node.NamePath()[len(node.NamePath())-1]
		}
	}

	return StatementData{
		Type: StatementTypeDrop,
		Drop: &DropData{
			IfExists:   node.IsIfExists(),
			ObjectType: "FUNCTION",
			ObjectName: objectName,
		},
	}, nil
}

// extractCreateTableAsSelectStatementData extracts data from CREATE TABLE AS SELECT statement nodes
func (e *NodeExtractor) extractCreateTableAsSelectStatementData(node *ast.CreateTableAsSelectStmtNode, ctx TransformContext) (StatementData, error) {
	// Get the table name from the name path
	var tableName string
	if namePath := namePathFromContext(ctx.Context()); namePath != nil {
		tableName = namePath.format(node.NamePath())
	} else {
		// Fallback to simple name formatting if no context
		if len(node.NamePath()) > 0 {
			tableName = node.NamePath()[len(node.NamePath())-1]
		}
	}

	// Extract the SELECT query scan data
	queryScanData, err := e.ExtractScanData(node.Query(), ctx)
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract CREATE TABLE AS SELECT query scan: %w", err)
	}

	// Extract output column information from the AS SELECT clause
	selectItems := make([]*SelectItemData, 0, len(node.OutputColumnList()))
	for _, col := range node.OutputColumnList() {
		exprData, err := e.ExtractExpressionData(col, ctx)
		if err != nil {
			return StatementData{}, fmt.Errorf("failed to extract CREATE TABLE AS SELECT output column: %w", err)
		}

		selectItems = append(selectItems, &SelectItemData{
			Expression: exprData,
			Alias:      col.Name(),
		})
	}

	// Create the SELECT data for the AS SELECT clause
	asSelectData := &SelectData{
		SelectList: selectItems,
		FromClause: &queryScanData,
	}

	// Create the CREATE TABLE data
	createTableData := &CreateTableData{
		TableName:   tableName,
		AsSelect:    asSelectData,
		IfNotExists: node.CreateMode() == ast.CreateIfNotExistsMode,
	}

	return StatementData{
		Type: StatementTypeCreate,
		Create: &CreateData{
			Type:  CreateTypeTable,
			Table: createTableData,
		},
	}, nil
}

// extractCreateViewStatementData extracts data from CREATE VIEW statement nodes
func (e *NodeExtractor) extractCreateViewStatementData(node *ast.CreateViewStmtNode, ctx TransformContext) (StatementData, error) {
	// Get the view name from the name path
	var viewName string
	if namePath := namePathFromContext(ctx.Context()); namePath != nil {
		viewName = namePath.format(node.NamePath())
	} else {
		// Fallback to simple name formatting if no context
		if len(node.NamePath()) > 0 {
			viewName = node.NamePath()[len(node.NamePath())-1]
		}
	}

	// Extract the SELECT query scan data
	queryScanData, err := e.ExtractScanData(node.Query(), ctx)
	if err != nil {
		return StatementData{}, fmt.Errorf("failed to extract CREATE VIEW query scan: %w", err)
	}

	// Extract output column information from the view's query
	selectItems := make([]*SelectItemData, 0, len(node.OutputColumnList()))
	for _, col := range node.OutputColumnList() {
		exprData, err := e.ExtractExpressionData(col, ctx)
		if err != nil {
			return StatementData{}, fmt.Errorf("failed to extract CREATE VIEW output column: %w", err)
		}

		selectItems = append(selectItems, &SelectItemData{
			Expression: exprData,
			Alias:      col.Name(),
		})
	}

	// Create the SELECT data for the view's query
	queryData := SelectData{
		SelectList: selectItems,
		FromClause: &queryScanData,
	}

	// Create the CREATE VIEW data
	createViewData := &CreateViewData{
		ViewName: viewName,
		Query:    queryData,
	}

	return StatementData{
		Type: StatementTypeCreate,
		Create: &CreateData{
			Type: CreateTypeView,
			View: createViewData,
		},
	}, nil
}
