package internal

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/goccy/go-json"
	googlesql "github.com/goccy/go-googlesql"
)

type Formatter interface {
	FormatSQL(context.Context) (string, error)
}

func New(node googlesql.ResolvedNodeNode) Formatter {
	return newNode(node)
}

func getTableName(ctx context.Context, n googlesql.ResolvedNodeNode) (string, error) {
	// Preferred path: pull the bound Table off a ResolvedTableScan and
	// ask it for its name. The go-zetasql version used a side-car
	// nodeMap that mapped ResolvedTableScan → ASTPathExpression to
	// recover the original user-supplied identifier, but that side
	// channel is not exposed through the wasm bridge. For the common
	// case (single-segment table names) the catalog-bound Table.Name()
	// is equivalent.
	if scan, ok := n.(*googlesql.ResolvedTableScan); ok {
		if table, err := scan.TableMethod(); err == nil && table != nil {
			if name, err := table.Name(); err == nil && name != "" {
				namePath := namePathFromContext(ctx)
				return namePath.format([]string{name}), nil
			}
		}
	}
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

func getFuncName(ctx context.Context, n googlesql.ResolvedNodeNode) (string, error) {
	nodeMap := nodeMapFromContext(ctx)
	found := nodeMap.FindNodeFromResolvedNode(n)
	if len(found) == 0 {
		return "", fmt.Errorf("failed to find path node from function node %T", n)
	}
	var foundCallNode googlesql.ASTFunctionCallNode
	for _, node := range found {
		fcallNode, ok := node.(googlesql.ASTFunctionCallNode)
		if !ok {
			continue
		}
		foundCallNode = fcallNode
		break
	}
	if foundCallNode == nil {
		return "", fmt.Errorf("failed to find function call node from %T", n)
	}
	path, err := getPathFromNode(m1(foundCallNode.FunctionMethod()))
	if err != nil {
		return "", fmt.Errorf("failed to find path: %w", err)
	}
	namePath := namePathFromContext(ctx)
	return namePath.format(path), nil
}

func getPathFromNode(n googlesql.ASTNodeNode) ([]string, error) {
	var path []string
	switch node := n.(type) {
	case googlesql.ASTIdentifierNode:
		path = append(path, m1(node.GetAsString()))
	case googlesql.ASTPathExpressionNode:
		for _, name := range m1(node.ToIdentifierVector()) {
			path = append(path, name)
		}
	case googlesql.ASTTablePathExpressionNode:
		switch {
		case m1(node.PathExpr()) != nil:
			for _, name := range m1(m1(node.PathExpr()).ToIdentifierVector()) {
				path = append(path, name)
			}
		}
	default:
		return nil, fmt.Errorf("found unknown path node: %T", node)
	}
	return path, nil
}

func uniqueColumnName(ctx context.Context, col *googlesql.ResolvedColumn) string {
	colName, _ := col.Name()
	if useTableNameForColumn(ctx) {
		return fmt.Sprintf("%s.%s", m1(col.TableName()), colName)
	}
	if useColumnID(ctx) {
		colID, _ := col.ColumnId()
		return fmt.Sprintf("%s#%d", colName, colID)
	}
	return colName
}

type InputPattern int

const (
	InputKeep      InputPattern = 0
	InputNeedsWrap InputPattern = 1
	InputNeedsFrom InputPattern = 2
)

func getInputPattern(input string) InputPattern {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return InputKeep
	}
	if strings.HasPrefix(trimmed, "FROM") {
		return InputKeep
	}
	if strings.HasPrefix(trimmed, "SELECT") {
		return InputNeedsWrap
	}
	if strings.HasPrefix(trimmed, "WITH") {
		return InputNeedsWrap
	}
	return InputNeedsFrom
}

func formatInput(input string) (string, error) {
	switch getInputPattern(input) {
	case InputKeep:
		return input, nil
	case InputNeedsWrap:
		return fmt.Sprintf("FROM (%s)", input), nil
	case InputNeedsFrom:
		return fmt.Sprintf("FROM %s", input), nil
	}
	return "", fmt.Errorf("unexpected input pattern: %s", input)
}

func getFuncNameAndArgs(ctx context.Context, node *ResolvedBaseFunctionCallNode, isWindowFunc bool) (string, []string, error) {
	args := []string{}
	for _, a := range m1(node.ArgumentList()) {
		arg, err := newNode(a).FormatSQL(ctx)
		if err != nil {
			return "", nil, err
		}
		args = append(args, arg)
	}
	funcName := m1(m1(node.FunctionMethod()).FullName(false))
	funcName = strings.Replace(funcName, ".", "_", -1)

	_, existsCurrentTimeFunc := currentTimeFuncMap[funcName]
	_, existsNormalFunc := normalFuncMap[funcName]
	_, existsAggregateFunc := aggregateFuncMap[funcName]
	_, existsWindowFunc := windowFuncMap[funcName]
	currentTime := CurrentTime(ctx)

	funcPrefix := "zetasqlite"
	if ResolvedFunctionCallBaseErrorMode(node) == googlesql.ResolvedFunctionCallBaseEnums_ErrorModeSafeErrorMode {
		if !existsNormalFunc {
			return "", nil, fmt.Errorf("SAFE is not supported for function %s", funcName)
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
		if currentTime != nil {
			args = append(
				args,
				fmt.Sprint(currentTime.UnixNano()),
			)
		}
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if existsNormalFunc {
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if !isWindowFunc && existsAggregateFunc {
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if isWindowFunc && existsWindowFunc {
		funcName = fmt.Sprintf("%s_window_%s", funcPrefix, funcName)
	} else {
		if false {
			return "", nil, fmt.Errorf("%s function is unimplemented", funcName)
		}
		fname, err := getFuncName(ctx, node)
		if err != nil {
			return "", nil, err
		}
		funcName = fname
	}
	return funcName, args, nil
}

func (n *LiteralNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return LiteralFromZetaSQLValue(*m1(n.node.ValueMethod()))
}

func (n *ParameterNode) FormatSQL(ctx context.Context) (string, error) {
	name, _ := n.node.Name()
	if name == "" {
		return "?", nil
	}
	return fmt.Sprintf("@%s", name), nil
}

func (n *ExpressionColumnNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ColumnRefNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	columnMap := columnRefMap(ctx)
	col, _ := n.node.Column()
	colName := uniqueColumnName(ctx, col)
	if ref, exists := columnMap[colName]; exists {
		delete(columnMap, colName)
		return ref, nil
	}
	return fmt.Sprintf("`%s`", colName), nil
}

func (n *ConstantNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SystemVariableNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *InlineLambdaNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FilterFieldArgNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FilterFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FunctionCallNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	funcName, args, err := getFuncNameAndArgs(ctx, n.node.ResolvedFunctionCallBase, false)
	if err != nil {
		return "", err
	}
	switch funcName {
	case "zetasqlite_ifnull":
		return fmt.Sprintf(
			"CASE WHEN %s IS NULL THEN %s ELSE %s END",
			args[0],
			args[1],
			args[0],
		), nil
	case "zetasqlite_if":
		return fmt.Sprintf(
			"CASE WHEN %s THEN %s ELSE %s END",
			args[0],
			args[1],
			args[2],
		), nil
	case "zetasqlite_case_no_value":
		var whenStmts []string
		for i := 0; i < len(args)-1; i += 2 {
			whenStmts = append(whenStmts, fmt.Sprintf("WHEN %s THEN %s", args[i], args[i+1]))
		}
		stmt := fmt.Sprintf("CASE %s", strings.Join(whenStmts, " "))
		// if args length is odd number, else statement exists.
		if len(args) > (len(args)/2)*2 {
			stmt += fmt.Sprintf(" ELSE %s", args[len(args)-1])
		}
		stmt += " END"
		return stmt, nil
	case "zetasqlite_case_with_value":
		if len(args) < 2 {
			return "", fmt.Errorf("not enough arguments for case with value")
		}
		val := args[0]
		args = args[1:]
		var whenStmts []string
		for i := 0; i < len(args)-1; i += 2 {
			whenStmts = append(whenStmts, fmt.Sprintf("WHEN %s THEN %s", args[i], args[i+1]))
		}
		stmt := fmt.Sprintf("CASE %s %s", val, strings.Join(whenStmts, " "))
		// if args length is odd number, else statement exists.
		if len(args) > (len(args)/2)*2 {
			stmt += fmt.Sprintf(" ELSE %s", args[len(args)-1])
		}
		stmt += " END"
		return stmt, nil
	}
	funcMap := funcMapFromContext(ctx)
	if spec, exists := funcMap[funcName]; exists {
		return spec.CallSQL(ctx, n.node.ResolvedFunctionCallBase, args)
	}
	return fmt.Sprintf(
		"%s(%s)",
		funcName,
		strings.Join(args, ","),
	), nil
}

func (n *AggregateFunctionCallNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	funcName, args, err := getFuncNameAndArgs(ctx, n.node.ResolvedFunctionCallBase, false)
	if err != nil {
		return "", err
	}
	funcMap := funcMapFromContext(ctx)
	if spec, exists := funcMap[funcName]; exists {
		return spec.CallSQL(ctx, n.node.ResolvedFunctionCallBase, args)
	}
	var opts []string
	for _, item := range m1(n.node.OrderByItemList()) {
		columnRef := m1(item.ColumnRef())
		colName := uniqueColumnName(ctx, m1(columnRef.Column()))
		if m1(item.IsDescending()) {
			opts = append(opts, fmt.Sprintf("zetasqlite_order_by(`%s`, false)", colName))
		} else {
			opts = append(opts, fmt.Sprintf("zetasqlite_order_by(`%s`, true)", colName))
		}
	}
	if m1(n.node.Distinct()) {
		opts = append(opts, "zetasqlite_distinct()")
	}
	if m1(n.node.Limit()) != nil {
		limitValue, err := newNode(nn(n.node.Limit())).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		opts = append(opts, fmt.Sprintf("zetasqlite_limit(%s)", limitValue))
	}
	switch ResolvedAggregateFunctionCallNullHandlingModifier(n.node) {
	case googlesql.ResolvedNonScalarFunctionCallBaseEnums_NullHandlingModifierIgnoreNulls:
		opts = append(opts, "zetasqlite_ignore_nulls()")
	case googlesql.ResolvedNonScalarFunctionCallBaseEnums_NullHandlingModifierRespectNulls:
	}
	args = append(args, opts...)
	return fmt.Sprintf(
		"%s(%s)",
		funcName,
		strings.Join(args, ","),
	), nil
}

func (n *AnalyticFunctionCallNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	orderColumnNames := analyticOrderColumnNamesFromContext(ctx)
	orderColumns := orderColumnNames.values
	funcName, args, err := getFuncNameAndArgs(ctx, n.node.ResolvedFunctionCallBase, true)
	if err != nil {
		return "", err
	}
	var opts []string
	if m1(n.node.Distinct()) {
		opts = append(opts, "zetasqlite_distinct()")
	}
	switch ResolvedAnalyticFunctionCallNullHandlingModifier(n.node) {
	case googlesql.ResolvedNonScalarFunctionCallBaseEnums_NullHandlingModifierRespectNulls:
		// do nothing
	default:
		opts = append(opts, "zetasqlite_ignore_nulls()")
	}
	args = append(args, opts...)
	for _, column := range analyticPartitionColumnNamesFromContext(ctx) {
		args = append(args, getWindowPartitionOptionFuncSQL(column))
	}
	for _, col := range orderColumns {
		args = append(args, getWindowOrderByOptionFuncSQL(col.column, col.isAsc))
	}
	windowFrame, _ := n.node.WindowFrame()
	if windowFrame != nil {
		args = append(args, getWindowFrameUnitOptionFuncSQL(ResolvedWindowFrameFrameUnit(windowFrame)))
		startSQL, err := n.getWindowBoundaryOptionFuncSQL(ctx, m1(windowFrame.StartExpr()), true)
		if err != nil {
			return "", err
		}
		endSQL, err := n.getWindowBoundaryOptionFuncSQL(ctx, m1(windowFrame.EndExpr()), false)
		if err != nil {
			return "", err
		}
		args = append(args, startSQL, endSQL)
	}
	args = append(args, getWindowRowIDOptionFuncSQL())
	input := analyticInputScanFromContext(ctx)
	funcMap := funcMapFromContext(ctx)
	if spec, exists := funcMap[funcName]; exists {
		return spec.CallSQL(ctx, n.node.ResolvedFunctionCallBase, args)
	}
	return fmt.Sprintf(
		"( SELECT %s(%s) %s )",
		funcName,
		strings.Join(args, ","),
		input,
	), nil
}

func (n *AnalyticFunctionCallNode) getWindowBoundaryOptionFuncSQL(ctx context.Context, expr googlesql.ResolvedWindowFrameExprNode, isStart bool) (string, error) {
	typ := ResolvedWindowFrameExprBoundaryType(expr)
	switch typ {
	case googlesql.ResolvedWindowFrameExprEnums_BoundaryTypeUnboundedPreceding, googlesql.ResolvedWindowFrameExprEnums_BoundaryTypeCurrentRow, googlesql.ResolvedWindowFrameExprEnums_BoundaryTypeUnboundedFollowing:
		if isStart {
			return getWindowBoundaryStartOptionFuncSQL(typ, ""), nil
		}
		return getWindowBoundaryEndOptionFuncSQL(typ, ""), nil
	case googlesql.ResolvedWindowFrameExprEnums_BoundaryTypeOffsetPreceding, googlesql.ResolvedWindowFrameExprEnums_BoundaryTypeOffsetFollowing:
		literal, err := newNode(nn(expr.Expression())).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		if isStart {
			return getWindowBoundaryStartOptionFuncSQL(typ, literal), nil
		}
		return getWindowBoundaryEndOptionFuncSQL(typ, literal), nil
	}
	return "", fmt.Errorf("unexpected boundary type %d", typ)
}

func (n *ExtendedCastElementNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExtendedCastNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CastNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	fromType := newType(m1(AsResolvedExpr(m1(n.node.Expr())).Type()))
	jsonEncodedFromType, err := json.Marshal(fromType)
	if err != nil {
		return "", err
	}
	toType := newType(m1(n.node.Type()))
	jsonEncodedToType, err := json.Marshal(toType)
	if err != nil {
		return "", err
	}
	encodedFromType, err := EncodeGoValue(StringType(), string(jsonEncodedFromType))
	if err != nil {
		return "", err
	}
	encodedToType, err := EncodeGoValue(StringType(), string(jsonEncodedToType))
	if err != nil {
		return "", err
	}
	expr, err := newNode(nn(n.node.Expr())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"zetasqlite_cast(%s, '%s', '%s', %t)",
		expr, encodedFromType, encodedToType, m1(n.node.ReturnNullOnError()),
	), nil
}

func (n *MakeStructNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	typ := m1(m1(n.node.Type()).AsStruct())
	typFields := m1(typ.Fields())
	fields, _ := n.node.FieldList()
	args := make([]string, 0, len(typFields)*2)
	for i, sf := range typFields {
		fieldName := sf.Name
		key, err := LiteralFromValue(StringValue(fieldName))
		if err != nil {
			return "", err
		}
		args = append(args, key)
		field, err := newNode(fields[i]).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		args = append(args, field)
	}
	return fmt.Sprintf("zetasqlite_make_struct(%s)", strings.Join(args, ",")), nil
}

func (n *MakeProtoNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *MakeProtoFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GetStructFieldNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	expr, err := newNode(nn(n.node.Expr())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	idx, _ := n.node.FieldIdx()
	return fmt.Sprintf("zetasqlite_get_struct_field(%s, %d)", expr, idx), nil
}

func (n *GetProtoFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GetJsonFieldNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	expr, err := newNode(nn(n.node.Expr())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	name, _ := n.node.FieldName()
	encodedName, err := EncodeGoValue(StringType(), name)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("zetasqlite_get_json_field(%s, '%s')", expr, encodedName), nil
}

func (n *FlattenNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FlattenedArgNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ReplaceFieldItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ReplaceFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SubqueryExprNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	columnNames := &arraySubqueryColumnNames{}
	ctx = withArraySubqueryColumnName(ctx, columnNames)
	sql, err := newNode(nn(n.node.Subquery())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	switch ResolvedSubqueryExprSubqueryType(n.node) {
	case googlesql.ResolvedSubqueryExprEnums_SubqueryTypeScalar:
	case googlesql.ResolvedSubqueryExprEnums_SubqueryTypeArray:
		subCols := m1(AsResolvedScan(m1(n.node.Subquery())).ColumnList())
		if len(subCols) == 0 {
			return "", fmt.Errorf("failed to find computed column names for array subquery")
		}
		colName := uniqueColumnName(ctx, subCols[0])
		return fmt.Sprintf("(SELECT zetasqlite_array(`%s`) FROM (%s))", colName, sql), nil
	case googlesql.ResolvedSubqueryExprEnums_SubqueryTypeExists:
		return fmt.Sprintf("EXISTS (%s)", sql), nil
	case googlesql.ResolvedSubqueryExprEnums_SubqueryTypeIn:
		expr, err := newNode(nn(n.node.InExpr())).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s IN (%s)", expr, sql), nil
	case googlesql.ResolvedSubqueryExprEnums_SubqueryTypeLikeAny:
	case googlesql.ResolvedSubqueryExprEnums_SubqueryTypeLikeAll:
	}
	return fmt.Sprintf("(%s)", sql), nil
}

func (n *LetExprNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ModelNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ConnectionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DescriptorNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SingleRowScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *TableScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	var columns []string
	for _, col := range m1(n.node.ColumnList()) {
		columns = append(
			columns,
			fmt.Sprintf("`%s` AS `%s`", m1(col.Name()), uniqueColumnName(ctx, col)),
		)
	}

	// If the underlying TableNode is actually a wildcard — recorded in
	// wildcardTableRegistry under the SimpleTable's wasm handle ptr —
	// rewrite the scan into the UNION-ALL pattern. Otherwise fall through
	// to a regular table reference.
	table := m1(n.node.TableMethod())
	if wc := lookupWildcardTable(table); wc != nil {
		query, err := wc.FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(SELECT %s FROM (%s))", strings.Join(columns, ","), query), nil
	}
	tableName, err := getTableName(ctx, n.node)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("(SELECT %s FROM `%s`)", strings.Join(columns, ","), tableName), nil
}

func (n *JoinScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	left, err := newNode(nn(n.node.LeftScan())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	right, err := newNode(nn(n.node.RightScan())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	if getInputPattern(left) == InputNeedsWrap {
		left = fmt.Sprintf("(%s)", left)
	}
	if getInputPattern(right) == InputNeedsWrap {
		right = fmt.Sprintf("(%s)", right)
	}
	if m1(n.node.JoinExpr()) == nil {
		return fmt.Sprintf("%s CROSS JOIN %s", left, right), nil
	}
	joinExpr, err := newNode(nn(n.node.JoinExpr())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	switch ResolvedJoinScanJoinType(n.node) {
	case googlesql.ResolvedJoinScanEnums_JoinTypeInner:
		return fmt.Sprintf("%s JOIN %s ON %s", left, right, joinExpr), nil
	case googlesql.ResolvedJoinScanEnums_JoinTypeLeft:
		return fmt.Sprintf("%s LEFT JOIN %s ON %s", left, right, joinExpr), nil
	case googlesql.ResolvedJoinScanEnums_JoinTypeRight:
		return fmt.Sprintf("%s RIGHT JOIN %s ON %s", left, right, joinExpr), nil
	case googlesql.ResolvedJoinScanEnums_JoinTypeFull:
		return fmt.Sprintf("%s FULL OUTER JOIN %s ON %s", left, right, joinExpr), nil
	}
	return "", fmt.Errorf("unexpected join type %d", ResolvedJoinScanJoinType(n.node))
}

func (n *ArrayScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	arrayExpr, err := newNode(nn(n.node.ArrayExpr())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	colName := uniqueColumnName(ctx, m1(n.node.ElementColumn()))
	columns := []string{fmt.Sprintf("json_each.value AS `%s`", colName)}

	if offsetColumn, _ := n.node.ArrayOffsetColumn(); offsetColumn != nil {
		offsetColName := uniqueColumnName(ctx, m1(offsetColumn.Column()))
		columns = append(columns, fmt.Sprintf("json_each.key AS `%s`", offsetColName))
	}
	if m1(n.node.InputScan()) != nil {
		input, err := newNode(nn(n.node.InputScan())).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		formattedInput, err := formatInput(input)
		if err != nil {
			return "", err
		}

		array := fmt.Sprintf("json_each(zetasqlite_decode_array(%s))", arrayExpr)
		var arrayJoinExpr string
		if m1(n.node.JoinExpr()) != nil {
			arrayJoinExpr, err = newNode(nn(n.node.JoinExpr())).FormatSQL(ctx)
			if err != nil {
				return "", err
			}
			// RIGHT JOINs on array expressions are not supported by BigQuery
			var joinMode string
			if m1(n.node.IsOuter()) {
				joinMode = "LEFT OUTER JOIN"
			} else {
				joinMode = "INNER JOIN"
			}
			arrayJoinExpr = fmt.Sprintf("%s %s ON %s",
				joinMode,
				array,
				arrayJoinExpr,
			)
		} else {
			// If there is no join expression, use a CROSS JOIN
			arrayJoinExpr = fmt.Sprintf(", %s", array)
		}

		return fmt.Sprintf(
			"SELECT *, %s %s %s",
			strings.Join(columns, ","),
			formattedInput,
			arrayJoinExpr,
		), nil
	}
	return fmt.Sprintf(
		"SELECT %s FROM json_each(zetasqlite_decode_array(%s))",
		strings.Join(columns, ","),
		arrayExpr,
	), nil
}

func (n *ColumnHolderNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

var tokensAfterFromClause = [...]string{"WHERE", "GROUP BY", "HAVING", "QUALIFY", "WINDOW", "ORDER BY", "COLLATE"}
var removeExpressions = regexp.MustCompile(`\(.+?\)`)

func (n *FilterScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(nn(n.node.InputScan())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	filter, err := newNode(nn(n.node.FilterExpr())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	currentQuery := removeExpressions.ReplaceAllString(input, "")

	// Qualify the statement if the input is not wrapped in parens
	queryWrappedInParens := currentQuery == ""
	containsTokens := false
	// and the input contains a token that would result in a syntax error
	for _, token := range tokensAfterFromClause {
		containsTokens = containsTokens || strings.Contains(currentQuery, token)
	}

	if !queryWrappedInParens && containsTokens {
		return fmt.Sprintf("( %s ) WHERE %s", input, filter), nil
	}
	return fmt.Sprintf("%s WHERE %s", input, filter), nil
}

func (n *GroupingSetNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AggregateScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	for _, agg := range m1(n.node.AggregateList()) {
		// assign sql to column ref map
		if _, err := newNode(agg).FormatSQL(ctx); err != nil {
			return "", err
		}
	}
	input, err := newNode(nn(n.node.InputScan())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	groupByColumns := []string{}
	groupByColumnMap := map[string]struct{}{}
	for _, col := range m1(n.node.GroupByList()) {
		if _, err := newNode(col).FormatSQL(ctx); err != nil {
			return "", err
		}
		colName := uniqueColumnName(ctx, m1(col.Column()))
		groupByColumns = append(groupByColumns, fmt.Sprintf("`%s`", colName))
		groupByColumnMap[colName] = struct{}{}
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	columnNames := []string{}
	for _, col := range m1(n.node.ColumnList()) {
		colName := uniqueColumnName(ctx, col)
		columnNames = append(columnNames, colName)
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else {
			columns = append(columns, fmt.Sprintf("`%s`", colName))
		}
	}
	if gsList := m1(n.node.GroupingSetList()); len(gsList) != 0 {
		columnPatterns := [][]string{}
		groupByColumnPatterns := [][]string{}
		for _, set := range gsList {
			concreteSet, ok := set.(*googlesql.ResolvedGroupingSet)
			if !ok {
				continue
			}
			groupBySetColumns := []string{}
			groupBySetColumnMap := map[string]struct{}{}
			for _, col := range m1(concreteSet.GroupByColumnList()) {
				colName := uniqueColumnName(ctx, m1(col.Column()))
				groupBySetColumns = append(groupBySetColumns, fmt.Sprintf("`%s`", colName))
				groupBySetColumnMap[colName] = struct{}{}
			}
			nullColumnNameMap := map[string]struct{}{}
			for colName := range groupByColumnMap {
				if _, exists := groupBySetColumnMap[colName]; !exists {
					nullColumnNameMap[colName] = struct{}{}
				}
			}
			groupBySetColumnPattern := []string{}
			for idx, col := range columnNames {
				if _, exists := nullColumnNameMap[col]; exists {
					groupBySetColumnPattern = append(groupBySetColumnPattern, fmt.Sprintf("NULL AS `%s`", col))
				} else {
					groupBySetColumnPattern = append(groupBySetColumnPattern, columns[idx])
				}
			}
			columnPatterns = append(columnPatterns, groupBySetColumnPattern)
			annotatedGroupBySetColumns := make([]string, 0, len(groupBySetColumns))
			for _, column := range groupBySetColumns {
				annotatedGroupBySetColumns = append(
					annotatedGroupBySetColumns,
					fmt.Sprintf("zetasqlite_group_by(%s)", column),
				)
			}
			groupByColumnPatterns = append(groupByColumnPatterns, annotatedGroupBySetColumns)
		}
		stmts := []string{}
		for i := 0; i < len(columnPatterns); i++ {
			var groupBy string
			if len(groupByColumnPatterns[i]) != 0 {
				groupBy = fmt.Sprintf("GROUP BY %s", strings.Join(groupByColumnPatterns[i], ","))
			}
			formattedColumns := strings.Join(columnPatterns[i], ",")
			switch getInputPattern(input) {
			case InputKeep:
				stmts = append(stmts, fmt.Sprintf("SELECT %s %s %s", formattedColumns, input, groupBy))
			case InputNeedsWrap:
				stmts = append(stmts, fmt.Sprintf("SELECT %s FROM (%s) %s", formattedColumns, input, groupBy))
			case InputNeedsFrom:
				stmts = append(stmts, fmt.Sprintf("SELECT %s FROM %s %s", formattedColumns, input, groupBy))
			}
		}
		groupByWithCollates := make([]string, 0, len(groupByColumns))
		for _, groupByColumn := range groupByColumns {
			groupByWithCollates = append(
				groupByWithCollates,
				fmt.Sprintf("%s COLLATE zetasqlite_collate", groupByColumn),
			)
		}
		return fmt.Sprintf(
			"%s ORDER BY %s",
			strings.Join(stmts, " UNION ALL "),
			strings.Join(groupByWithCollates, ","),
		), nil
	}
	var groupBy string
	if len(groupByColumns) > 0 {
		annotatedGroupByColumns := make([]string, 0, len(groupByColumns))
		for _, groupByColumn := range groupByColumns {
			annotatedGroupByColumns = append(
				annotatedGroupByColumns,
				fmt.Sprintf("zetasqlite_group_by(%s)", groupByColumn),
			)
		}
		groupBy = fmt.Sprintf("GROUP BY %s", strings.Join(annotatedGroupByColumns, ","))
	}
	formattedColumns := strings.Join(columns, ",")
	switch getInputPattern(input) {
	case InputKeep:
		return fmt.Sprintf("SELECT %s %s %s", formattedColumns, input, groupBy), nil
	case InputNeedsWrap:
		return fmt.Sprintf("SELECT %s FROM (%s) %s", formattedColumns, input, groupBy), nil
	case InputNeedsFrom:
		return fmt.Sprintf("SELECT %s FROM %s %s", formattedColumns, input, groupBy), nil
	}
	return "", fmt.Errorf("unexpected input pattern: %s", input)
}

func (n *AnonymizedAggregateScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SetOperationItemNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return newNode(nn(n.node.Scan())).FormatSQL(ctx)
}

func (n *SetOperationScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	var opType string
	switch ResolvedSetOperationScanOpType(n.node) {
	case googlesql.ResolvedSetOperationScanEnums_SetOperationTypeUnionAll:
		opType = "UNION ALL"
	case googlesql.ResolvedSetOperationScanEnums_SetOperationTypeUnionDistinct:
		opType = "UNION"
	case googlesql.ResolvedSetOperationScanEnums_SetOperationTypeIntersectAll:
		opType = "INTERSECT ALL"
	case googlesql.ResolvedSetOperationScanEnums_SetOperationTypeIntersectDistinct:
		opType = "INTERSECT"
	case googlesql.ResolvedSetOperationScanEnums_SetOperationTypeExceptAll:
		opType = "EXCEPT ALL"
	case googlesql.ResolvedSetOperationScanEnums_SetOperationTypeExceptDistinct:
		opType = "EXCEPT"
	default:
		opType = "UNKNOWN"
	}
	var queries []string
	for _, item := range m1(n.node.InputItemList()) {
		var outputColumns []string
		for _, outputColumn := range m1(item.OutputColumnList()) {
			outputColumns = append(outputColumns, fmt.Sprintf("`%s`", uniqueColumnName(ctx, outputColumn)))
		}
		query, err := newNode(item).FormatSQL(ctx)
		if err != nil {
			return "", err
		}

		formattedInput, err := formatInput(query)
		if err != nil {
			return "", err
		}

		queries = append(
			queries,
			fmt.Sprintf("SELECT %s %s",
				strings.Join(outputColumns, ", "),
				formattedInput,
			),
		)
	}
	columnMaps := []string{}
	if inputItems := m1(n.node.InputItemList()); len(inputItems) != 0 {
		for idx, col := range m1(inputItems[0].OutputColumnList()) {
			columnMaps = append(
				columnMaps,
				fmt.Sprintf(
					"`%s` AS `%s`",
					uniqueColumnName(ctx, col),
					uniqueColumnName(ctx, m1(n.node.ColumnList())[idx]),
				),
			)
		}
	}
	return fmt.Sprintf(
		"SELECT %s FROM (%s)",
		strings.Join(columnMaps, ","),
		strings.Join(queries, fmt.Sprintf(" %s ", opType)),
	), nil
}

func (n *OrderByScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(nn(n.node.InputScan())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range m1(n.node.ColumnList()) {
		colName := uniqueColumnName(ctx, col)
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else {
			columns = append(
				columns,
				fmt.Sprintf("`%s`", colName),
			)
		}
	}
	orderByColumns := []string{}
	for _, item := range m1(n.node.OrderByItemList()) {
		colName := uniqueColumnName(ctx, m1(m1(item.ColumnRef()).Column()))
		switch ResolvedOrderByItemNullOrder(item) {
		case googlesql.ResolvedOrderByItemEnums_NullOrderModeNullsFirst:
			orderByColumns = append(
				orderByColumns,
				fmt.Sprintf("(`%s` IS NOT NULL)", colName),
			)
		case googlesql.ResolvedOrderByItemEnums_NullOrderModeNullsLast:
			orderByColumns = append(
				orderByColumns,
				fmt.Sprintf("(`%s` IS NULL)", colName),
			)
		}
		if m1(item.IsDescending()) {
			orderByColumns = append(orderByColumns, fmt.Sprintf("`%s` COLLATE zetasqlite_collate DESC", colName))
		} else {
			orderByColumns = append(orderByColumns, fmt.Sprintf("`%s` COLLATE zetasqlite_collate", colName))
		}
	}
	formattedInput, err := formatInput(input)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"SELECT %s %s ORDER BY %s",
		strings.Join(columns, ","),
		formattedInput,
		strings.Join(orderByColumns, ","),
	), nil
}

func (n *LimitOffsetScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(nn(n.node.InputScan())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range m1(n.node.ColumnList()) {
		colName := uniqueColumnName(ctx, col)
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else {
			columns = append(
				columns,
				fmt.Sprintf("`%s`", colName),
			)
		}
	}
	formattedInput, err := formatInput(input)
	if err != nil {
		return "", err
	}
	var limitExpr string
	if m1(n.node.Limit()) != nil {
		expr, err := newNode(nn(n.node.Limit())).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		limitExpr = fmt.Sprintf("LIMIT %s", expr)
	}
	var offsetExpr string
	if m1(n.node.Offset()) != nil {
		expr, err := newNode(nn(n.node.Offset())).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		offsetExpr = fmt.Sprintf("OFFSET %s", expr)
	}
	return fmt.Sprintf(
		"SELECT %s %s %s %s",
		strings.Join(columns, ","),
		formattedInput,
		limitExpr,
		offsetExpr,
	), nil
}

func (n *WithRefScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	tableName, _ := n.node.WithQueryName()
	tableToColumnListMap := tableNameToColumnListMap(ctx)
	columnDefs := tableToColumnListMap[tableName]
	columns := m1(n.node.ColumnList())
	if len(columnDefs) != len(columns) {
		return "", fmt.Errorf(
			"column num mismatch. defined column num is %d but used %d column",
			len(columnDefs), len(columns),
		)
	}
	formattedColumns := []string{}
	for i := 0; i < len(columnDefs); i++ {
		formattedColumns = append(
			formattedColumns,
			fmt.Sprintf("`%s` AS `%s`", uniqueColumnName(ctx, columnDefs[i]), uniqueColumnName(ctx, columns[i])),
		)
	}
	return fmt.Sprintf("(SELECT %s FROM `%s`)", strings.Join(formattedColumns, ","), tableName), nil
}

func (n *AnalyticScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(nn(n.node.InputScan())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	formattedInput, err := formatInput(input)
	if err != nil {
		return "", err
	}
	ctx = withAnalyticInputScan(ctx, formattedInput)
	orderColumnNames := analyticOrderColumnNamesFromContext(ctx)
	var scanOrderBy []*analyticOrderBy
	for _, group := range m1(n.node.FunctionGroupList()) {
		scanOrderBy = []*analyticOrderBy{}

		if m1(group.PartitionBy()) != nil {
			var partitionColumns []string
			for _, columnRef := range m1(m1(group.PartitionBy()).PartitionByList()) {
				colName := fmt.Sprintf("`%s`", uniqueColumnName(ctx, m1(columnRef.Column())))
				partitionColumns = append(
					partitionColumns,
					colName,
				)
				order := &analyticOrderBy{
					column: colName,
					isAsc:  true,
				}
				orderColumnNames.values = append(orderColumnNames.values, order)
				scanOrderBy = append(scanOrderBy, order)
			}
			ctx = withAnalyticPartitionColumnNames(ctx, partitionColumns)
		}
		if m1(group.OrderBy()) != nil {
			for _, item := range m1(m1(group.OrderBy()).OrderByItemList()) {
				colName := uniqueColumnName(ctx, m1(m1(item.ColumnRef()).Column()))
				formattedColName := fmt.Sprintf("`%s`", colName)
				order := &analyticOrderBy{
					column: formattedColName,
					isAsc:  !m1(item.IsDescending()),
				}
				orderColumnNames.values = append(orderColumnNames.values, order)
				scanOrderBy = append(scanOrderBy, order)
			}
		}
		if _, err := newNode(group).FormatSQL(ctx); err != nil {
			return "", err
		}

		// Reset context after each analytic function group
		orderColumnNames.values = []*analyticOrderBy{}
		ctx = withAnalyticPartitionColumnNames(ctx, nil)
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range m1(n.node.ColumnList()) {
		colName := uniqueColumnName(ctx, col)
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else {
			columns = append(
				columns,
				fmt.Sprintf("`%s`", colName),
			)
		}
	}
	var orderColumnFormattedNames []string
	for _, col := range scanOrderBy {
		if col.isAsc {
			orderColumnFormattedNames = append(
				orderColumnFormattedNames,
				fmt.Sprintf("%s COLLATE zetasqlite_collate", col.column),
			)
		} else {
			orderColumnFormattedNames = append(
				orderColumnFormattedNames,
				fmt.Sprintf("%s COLLATE zetasqlite_collate DESC", col.column),
			)
		}
	}
	var orderBy string
	if len(orderColumnFormattedNames) != 0 {
		orderBy = fmt.Sprintf("ORDER BY %s", strings.Join(orderColumnFormattedNames, ","))
	}
	orderColumnNames.values = []*analyticOrderBy{}
	return fmt.Sprintf(
		"SELECT %s FROM (SELECT *, ROW_NUMBER() OVER() AS `row_id` %s) %s",
		strings.Join(columns, ","),
		formattedInput,
		orderBy,
	), nil
}

func (n *SampleScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ComputedColumnNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	expr, err := newNode(nn(n.node.Expr())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	col, _ := n.node.Column()
	uniqueName := uniqueColumnName(ctx, col)
	query := fmt.Sprintf("%s AS `%s`", expr, uniqueColumnName(ctx, col))
	columnMap := columnRefMap(ctx)
	columnMap[uniqueName] = query
	arraySubqueryColumnNames := arraySubqueryColumnNameFromContext(ctx)
	if arraySubqueryColumnNames != nil {
		arraySubqueryColumnNames.names = append(arraySubqueryColumnNames.names, fmt.Sprintf("`%s`", m1(col.Name())))
	}
	return query, nil
}

func (n *OrderByItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ColumnAnnotationsNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GeneratedColumnInfoNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ColumnDefaultValueNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ColumnDefinitionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *PrimaryKeyNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ForeignKeyNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CheckConstraintNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *OutputColumnNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	columnMap := columnRefMap(ctx)
	col, _ := n.node.Column()
	uniqueName := uniqueColumnName(ctx, col)
	if ref, exists := columnMap[uniqueName]; exists {
		return ref, nil
	}
	return fmt.Sprintf("`%s`", m1(col.Name())), nil
}

func (n *ProjectScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	for _, col := range m1(n.node.ExprList()) {
		// assign expr to columnRefMap
		if _, err := newNode(col).FormatSQL(ctx); err != nil {
			return "", err
		}
	}
	input, err := newNode(nn(n.node.InputScan())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range m1(n.node.ColumnList()) {
		colName := uniqueColumnName(ctx, col)
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else {
			columns = append(
				columns,
				fmt.Sprintf("`%s`", colName),
			)
		}
	}
	formattedInput, err := formatInput(input)
	if err != nil {
		return "", err
	}
	formattedColumns := strings.Join(columns, ",")
	return fmt.Sprintf("SELECT %s %s", formattedColumns, formattedInput), nil
}

func (n *TVFScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GroupRowsScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FunctionArgumentNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExplainStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

// FormatSQL Formats the outermost query statement that runs and produces rows of output, like a SELECT
// The node's `OutputColumnList()` gives user-visible column names that should be returned. There may be duplicate names,
// and multiple output columns may reference the same column from `Query()`
// https://github.com/google/zetasql/blob/master/docs/resolved_ast.md#ResolvedQueryStmt
func (n *QueryStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(nn(n.node.Query())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}

	var columns []string
	for _, outputColumnNode := range m1(n.node.OutputColumnList()) {
		columns = append(
			columns,
			fmt.Sprintf("`%s` AS `%s`",
				uniqueColumnName(ctx, m1(outputColumnNode.Column())),
				m1(outputColumnNode.Name()),
			),
		)
	}

	return fmt.Sprintf(
		"SELECT %s FROM (%s)",
		strings.Join(columns, ", "),
		input,
	), nil
}

func (n *CreateDatabaseStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *IndexItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *UnnestItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateIndexStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateSchemaStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateTableAsSelectStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateModelStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *WithPartitionColumnsNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateSnapshotTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateExternalTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExportModelStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExportDataStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DefineTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DescribeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ShowStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *BeginStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SetTransactionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CommitStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RollbackStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *StartBatchStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RunBatchStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AbortBatchStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	namePath := namePathFromContext(ctx)
	tableName := namePath.format2(n.node.NamePath())
	objectType, _ := n.node.ObjectType()
	if m1(n.node.IsIfExists()) {
		return fmt.Sprintf("DROP %s IF EXISTS `%s`", objectType, tableName), nil
	}
	return fmt.Sprintf("DROP %s `%s`", objectType, tableName), nil
}

func (n *DropMaterializedViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropSnapshotTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RecursiveRefScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RecursiveScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *WithScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	queries := []string{}
	for _, entry := range m1(n.node.WithEntryList()) {
		sql, err := newNode(entry).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		queries = append(queries, sql)
	}
	query, err := newNode(nn(n.node.Query())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"WITH %s %s",
		strings.Join(queries, ", "),
		query,
	), nil
}

func (n *WithEntryNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	queryName, _ := n.node.WithQueryName()
	subquery, err := newNode(nn(n.node.WithSubquery())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	tableToColumnList := tableNameToColumnListMap(ctx)
	tableToColumnList[queryName] = m1(AsResolvedScan(m1(n.node.WithSubquery())).ColumnList())
	return fmt.Sprintf("%s AS ( %s )", queryName, subquery), nil
}

func (n *OptionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *WindowPartitioningNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *WindowOrderingNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *WindowFrameNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AnalyticFunctionGroupNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}

	var queries []string
	for _, column := range m1(n.node.AnalyticFunctionList()) {
		sql, err := newNode(column).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		queries = append(queries, sql)
	}
	return strings.Join(queries, ","), nil
}

func (n *WindowFrameExprNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DMLValueNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	return newNode(nn(m1(n.node.ValueMethod()))).FormatSQL(ctx)
}

func (n *DMLDefaultNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AssertStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AssertRowsModifiedNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *InsertRowNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	values := []string{}
	for _, value := range m1(n.node.ValueList()) {
		sql, err := newNode(value).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		values = append(values, sql)
	}
	return strings.Join(values, ","), nil
}

func (n *InsertStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	table, err := getTableName(ctx, m1(n.node.TableScan()))
	if err != nil {
		return "", err
	}
	columns := []string{}
	for _, col := range m1(n.node.InsertColumnList()) {
		columns = append(columns, fmt.Sprintf("`%s`", m1(col.Name())))
	}
	query, _ := n.node.Query()
	if query != nil {
		stmt, err := newNode(query).FormatSQL(withUseColumnID(ctx))
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("INSERT INTO `%s` (%s) %s",
			table,
			strings.Join(columns, ","),
			stmt,
		), nil
	}
	rows := []string{}
	for _, row := range m1(n.node.RowList()) {
		sql, err := newNode(row).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		rows = append(rows, fmt.Sprintf("(%s)", sql))
	}
	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
		table,
		strings.Join(columns, ","),
		strings.Join(rows, ","),
	), nil
}

func (n *DeleteStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	table, err := getTableName(ctx, m1(n.node.TableScan()))
	if err != nil {
		return "", err
	}
	where, err := newNode(nn(n.node.WhereExpr())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"DELETE FROM `%s` WHERE %s",
		table,
		where,
	), nil
}

func (n *UpdateItemNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	target, err := newNode(nn(n.node.Target())).FormatSQL(unuseColumnID(withoutUseTableNameForColumn(ctx)))
	if err != nil {
		return "", err
	}
	setValue, err := newNode(nn(n.node.SetValue())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s=%s", target, setValue), nil
}

func (n *UpdateArrayItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *UpdateStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	table, err := getTableName(ctx, m1(n.node.TableScan()))
	if err != nil {
		return "", err
	}
	updateItems := []string{}
	for _, item := range m1(n.node.UpdateItemList()) {
		sql, err := newNode(item).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		updateItems = append(updateItems, sql)
	}
	where, err := newNode(nn(n.node.WhereExpr())).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"UPDATE `%s` SET %s WHERE %s",
		table,
		strings.Join(updateItems, ","),
		where,
	), nil
}

func (n *MergeWhenNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *MergeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *TruncateStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ObjectUnitNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *PrivilegeNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GrantStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RevokeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterDatabaseStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterMaterializedViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterSchemaStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SetOptionsActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AddColumnActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AddConstraintActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropConstraintActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropPrimaryKeyActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterColumnOptionsActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterColumnDropNotNullActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterColumnSetDataTypeActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterColumnSetDefaultActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterColumnDropDefaultActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropColumnActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RenameColumnActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SetAsActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SetCollateClauseNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterTableSetOptionsStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RenameStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreatePrivilegeRestrictionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateRowAccessPolicyStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropPrivilegeRestrictionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropRowAccessPolicyStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropSearchIndexStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GrantToActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RestrictToActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AddToRestricteeListActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RemoveFromRestricteeListActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FilterUsingActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RevokeFromActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RenameToActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterPrivilegeRestrictionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterRowAccessPolicyStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterAllRowAccessPoliciesStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateConstantStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ArgumentDefNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ArgumentRefNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return fmt.Sprintf("@%s", m1(n.node.Name())), nil
}

func (n *CreateTableFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RelationArgumentScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ArgumentListNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FunctionSignatureHolderNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropTableFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CallStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ImportStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ModuleStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AggregateHavingModifierNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateMaterializedViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateProcedureStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExecuteImmediateArgumentNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExecuteImmediateStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AssignmentStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateEntityStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterEntityStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *PivotColumnNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *PivotScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ReturningClauseNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *UnpivotArgNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *UnpivotScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CloneDataStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *TableAndColumnInfoNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AnalyzeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AuxLoadDataStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}
