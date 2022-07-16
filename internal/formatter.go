package internal

import (
	"context"
	"fmt"
	"strings"

	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
)

type Formatter interface {
	FormatSQL(context.Context) (string, error)
}

func New(node ast.Node) Formatter {
	return newNode(node)
}

func FormatName(namePath []string) string {
	namePath = FormatPath(namePath)
	return strings.Join(namePath, "_")
}

func FormatPath(path []string) []string {
	ret := []string{}
	for _, p := range path {
		splitted := strings.Split(p, ".")
		ret = append(ret, splitted...)
	}
	return ret
}

func MergeNamePath(namePath []string, queryPath []string) []string {
	namePath = FormatPath(namePath)
	queryPath = FormatPath(queryPath)
	if len(queryPath) == 0 {
		return namePath
	}

	merged := []string{}
	for _, path := range namePath {
		if queryPath[0] == path {
			break
		}
		merged = append(merged, path)
	}
	return append(merged, queryPath...)
}

func getFuncNameAndArgs(ctx context.Context, node *ast.BaseFunctionCallNode, isWindowFunc bool) (string, []string, error) {
	args := []string{}
	for _, a := range node.ArgumentList() {
		arg, err := newNode(a).FormatSQL(ctx)
		if err != nil {
			return "", nil, err
		}
		args = append(args, arg)
	}
	returnType := node.Signature().ResultType().Type()
	var suffixName string
	switch returnType.Kind() {
	case types.ARRAY:
		suffixName = "array"
	case types.STRUCT:
		suffixName = "struct"
	default:
		suffixName = strings.ToLower(returnType.TypeName(0))
	}
	funcName := node.Function().FullName(false)

	_, existsCurrentTimeFunc := currentTimeFuncMap[funcName]
	_, existsNormalFunc := normalFuncMap[funcName]
	_, existsAggregateFunc := aggregateFuncMap[funcName]
	_, existsWindowFunc := windowFuncMap[funcName]
	currentTime := CurrentTime(ctx)
	fullpath := fullNamePathFromContext(ctx)
	if strings.HasPrefix(funcName, "$") {
		if isWindowFunc {
			funcName = fmt.Sprintf("zetasqlite_window_%s_%s", funcName[1:], suffixName)
		} else {
			funcName = fmt.Sprintf("zetasqlite_%s_%s", funcName[1:], suffixName)
		}
	} else if existsCurrentTimeFunc {
		if currentTime != nil {
			args = append(
				args,
				fmt.Sprint(currentTime.UnixNano()),
			)
		}
		funcName = fmt.Sprintf("zetasqlite_%s_%s", funcName, suffixName)
	} else if existsNormalFunc {
		funcName = fmt.Sprintf("zetasqlite_%s_%s", funcName, suffixName)
	} else if !isWindowFunc && existsAggregateFunc {
		funcName = fmt.Sprintf("zetasqlite_%s_%s", funcName, suffixName)
	} else if isWindowFunc && existsWindowFunc {
		funcName = fmt.Sprintf("zetasqlite_window_%s_%s", funcName, suffixName)
	} else {
		path := fullpath.paths[fullpath.idx]
		funcName = FormatName(
			MergeNamePath(
				namePathFromContext(ctx),
				path,
			),
		)
	}
	fullpath.idx++
	return funcName, args, nil
}

func (n *LiteralNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return JSONFromZetaSQLValue(n.node.Value()), nil
}

func (n *ParameterNode) FormatSQL(ctx context.Context) (string, error) {
	return fmt.Sprintf("@%s", n.node.Name()), nil
}

func (n *ExpressionColumnNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ColumnRefNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	columnMap := columnRefMap(ctx)
	colName := n.node.Column().Name()
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
	funcName, args, err := getFuncNameAndArgs(ctx, n.node.BaseFunctionCallNode, false)
	if err != nil {
		return "", err
	}
	funcMap := funcMapFromContext(ctx)
	if spec, exists := funcMap[funcName]; exists {
		body := spec.Body
		for _, arg := range args {
			// TODO: Need to recognize the argument exactly.
			body = strings.Replace(body, "?", arg, 1)
		}
		return fmt.Sprintf("( %s )", body), nil
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
	funcName, args, err := getFuncNameAndArgs(ctx, n.node.BaseFunctionCallNode, false)
	if err != nil {
		return "", err
	}
	funcMap := funcMapFromContext(ctx)
	if spec, exists := funcMap[funcName]; exists {
		body := spec.Body
		for _, arg := range args {
			// TODO: Need to recognize the argument exactly.
			body = strings.Replace(body, "?", arg, 1)
		}
		return fmt.Sprintf("( %s )", body), nil
	}
	var opts []string
	for _, item := range n.node.OrderByItemList() {
		columnRef := item.ColumnRef()
		columnName, err := newNode(columnRef).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		if item.IsDescending() {
			opts = append(opts, fmt.Sprintf("zetasqlite_order_by_string(%s, false)", columnName))
		} else {
			opts = append(opts, fmt.Sprintf("zetasqlite_order_by_string(%s, true)", columnName))
		}
	}
	if n.node.Distinct() {
		opts = append(opts, "zetasqlite_distinct_string()")
	}
	if n.node.Limit() != nil {
		limitValue, err := newNode(n.node.Limit()).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		opts = append(opts, fmt.Sprintf("zetasqlite_limit_string(%s)", limitValue))
	}
	switch n.node.NullHandlingModifier() {
	case ast.IgnoreNulls:
		opts = append(opts, "zetasqlite_ignore_nulls_string()")
	case ast.RespectNulls:
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
	for _, a := range n.node.ArgumentList() {
		switch t := a.(type) {
		case *ast.ColumnRefNode:
			ctx = withAnalyticTableName(ctx, FormatName([]string{t.Column().TableName()}))
		case *ast.LiteralNode:
			continue
		default:
			return "", fmt.Errorf("unexpected argument node type %T for analytic function", a)
		}
		arg, err := newNode(a).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		orderColumnNames.values = append(orderColumnNames.values, arg)
	}
	tableName := analyticTableNameFromContext(ctx)
	if tableName == "" {
		return "", fmt.Errorf("failed to find table name from analytic query")
	}
	funcName, args, err := getFuncNameAndArgs(ctx, n.node.BaseFunctionCallNode, true)
	if err != nil {
		return "", err
	}
	funcMap := funcMapFromContext(ctx)
	if spec, exists := funcMap[funcName]; exists {
		body := spec.Body
		for _, arg := range args {
			// TODO: Need to recognize the argument exactly.
			body = strings.Replace(body, "?", arg, 1)
		}
		return fmt.Sprintf("( %s )", body), nil
	}
	var opts []string
	if n.node.Distinct() {
		opts = append(opts, "zetasqlite_distinct_string()")
	}
	args = append(args, opts...)
	for _, column := range analyticPartitionColumnNamesFromContext(ctx) {
		args = append(args, getWindowPartitionOptionFuncSQL(column))
	}
	for _, column := range orderColumns {
		args = append(args, getWindowOrderByOptionFuncSQL(column))
	}
	windowFrame := n.node.WindowFrame()
	if windowFrame != nil {
		args = append(args, getWindowFrameUnitOptionFuncSQL(windowFrame.FrameUnit()))
		startSQL, err := n.getWindowBoundaryOptionFuncSQL(ctx, windowFrame.StartExpr(), true)
		if err != nil {
			return "", err
		}
		endSQL, err := n.getWindowBoundaryOptionFuncSQL(ctx, windowFrame.EndExpr(), false)
		if err != nil {
			return "", err
		}
		args = append(args, startSQL)
		args = append(args, endSQL)
	}
	args = append(args, getWindowRowIDOptionFuncSQL())
	return fmt.Sprintf(
		"( SELECT %s(%s) FROM `%s` )",
		funcName,
		strings.Join(args, ","),
		tableName,
	), nil

	return "", nil
}

func (n *AnalyticFunctionCallNode) getWindowBoundaryOptionFuncSQL(ctx context.Context, expr *ast.WindowFrameExprNode, isStart bool) (string, error) {
	typ := expr.BoundaryType()
	switch typ {
	case ast.UnboundedPrecedingType, ast.CurrentRowType, ast.UnboundedFollowingType:
		if isStart {
			return getWindowBoundaryStartOptionFuncSQL(typ, ""), nil
		}
		return getWindowBoundaryEndOptionFuncSQL(typ, ""), nil
	case ast.OffsetPrecedingType, ast.OffsetFollowingType:
		literal, err := newNode(expr.Expression()).FormatSQL(ctx)
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
	return "", nil
}

func (n *MakeStructNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	var fields []string
	for _, field := range n.node.FieldList() {
		col, err := newNode(field).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		fields = append(fields, col)
	}
	return fmt.Sprintf("zetasqlite_make_struct_struct(%s)", strings.Join(fields, ",")), nil
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
	expr, err := newNode(n.node.Expr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	typeSuffix := strings.ToLower(n.node.Type().TypeName(0))
	if strings.HasPrefix(typeSuffix, "struct") {
		typeSuffix = "struct"
	} else if strings.HasPrefix(typeSuffix, "array") {
		typeSuffix = "array"
	}
	idx := n.node.FieldIdx()
	return fmt.Sprintf("zetasqlite_get_struct_field_%s(%s, %d)", typeSuffix, expr, idx), nil
}

func (n *GetProtoFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GetJsonFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
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
	sql, err := newNode(n.node.Subquery()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	switch n.node.SubqueryType() {
	case ast.SubqueryTypeScalar:
	case ast.SubqueryTypeArray:
		if len(columnNames.names) == 0 {
			return "", fmt.Errorf("failed to find computed column names for array subquery")
		}
		colName := columnNames.names[0]
		return fmt.Sprintf("zetasqlite_array_array(%s) FROM (%s)", colName, sql), nil
	case ast.SubqueryTypeExists:
		return fmt.Sprintf("EXISTS (%s)", sql), nil
	case ast.SubqueryTypeIn:
	case ast.SubqueryTypeLikeAny:
	case ast.SubqueryTypeLikeAll:
	}
	return sql, nil
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
	fullpath := fullNamePathFromContext(ctx)
	path := fullpath.paths[fullpath.idx]
	tableName := FormatName(
		MergeNamePath(
			namePathFromContext(ctx),
			path,
		),
	)
	fullpath.idx++
	return fmt.Sprintf("FROM `%s`", tableName), nil
}

func (n *JoinScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	leftRef, ok := n.node.LeftScan().(*ast.WithRefScanNode)
	if !ok {
		return "", fmt.Errorf("unexpected leftscan node %T", n.node.LeftScan())
	}
	rightRef, ok := n.node.RightScan().(*ast.WithRefScanNode)
	if !ok {
		return "", fmt.Errorf("unexpected rightscan node %T", n.node.RightScan())
	}
	equalFunc, ok := n.node.JoinExpr().(*ast.FunctionCallNode)
	if !ok {
		return "", fmt.Errorf("unexpected joinexpr node %T", n.node.JoinExpr())
	}
	args := equalFunc.ArgumentList()
	if len(args) != 2 {
		return "", fmt.Errorf("join argument must be two arguments but got %d", len(args))
	}
	leftColumn, err := newNode(args[0]).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	rightColumn, err := newNode(args[1]).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	leftTableName := leftRef.WithQueryName()
	rightTableName := rightRef.WithQueryName()
	var joinType string
	switch n.node.JoinType() {
	case ast.JoinTypeInner:
		joinType = "JOIN"
	case ast.JoinTypeLeft:
		joinType = "LEFT JOIN"
	case ast.JoinTypeRight:
		joinType = "RIGHT JOIN"
	case ast.JoinTypeFull:
		joinType = "FULL JOIN"
	}
	return fmt.Sprintf(
		"FROM `%s` %s `%s` ON `%s`.%s = `%s`.%s",
		leftTableName,
		joinType,
		rightTableName,
		leftTableName,
		leftColumn,
		rightTableName,
		rightColumn,
	), nil
}

func (n *ArrayScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	arrayExpr, err := newNode(n.node.ArrayExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	colName := n.node.ElementColumn().Name()
	return fmt.Sprintf(
		"FROM ( SELECT json_each.value AS `%s` FROM json_each(zetasqlite_decode_array_string(%s)) )",
		colName,
		arrayExpr,
	), nil
}

func (n *ColumnHolderNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FilterScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	ctx = withExistsGroupBy(ctx, &existsGroupBy{})
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	usedGroupBy := existsGroupByFromContext(ctx).exists
	filter, err := newNode(n.node.FilterExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	if usedGroupBy {
		return fmt.Sprintf("%s HAVING %s", input, filter), nil
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
	for _, agg := range n.node.AggregateList() {
		// assign sql to column ref map
		if _, err := newNode(agg).FormatSQL(ctx); err != nil {
			return "", err
		}
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range n.node.ColumnList() {
		colName := col.Name()
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
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	groupByColumns := []string{}
	groupByColumnMap := map[string]struct{}{}
	for _, col := range n.node.GroupByList() {
		colName := fmt.Sprintf("`%s`", col.Column().Name())
		groupByColumns = append(groupByColumns, colName)
		groupByColumnMap[colName] = struct{}{}
	}
	if len(groupByColumns) != 0 {
		existsGroupBy := existsGroupByFromContext(ctx)
		if existsGroupBy != nil {
			existsGroupBy.exists = true
		}
	}
	if len(n.node.GroupingSetList()) != 0 {
		columnPatterns := [][]string{}
		groupByColumnPatterns := [][]string{}
		for _, set := range n.node.GroupingSetList() {
			groupBySetColumns := []string{}
			groupBySetColumnMap := map[string]struct{}{}
			for _, col := range set.GroupByColumnList() {
				colName := fmt.Sprintf("`%s`", col.Column().Name())
				groupBySetColumns = append(groupBySetColumns, colName)
				groupBySetColumnMap[colName] = struct{}{}
			}
			nullColumnNameMap := map[string]struct{}{}
			for _, col := range groupByColumns {
				if _, exists := groupBySetColumnMap[col]; !exists {
					nullColumnNameMap[col] = struct{}{}
				}
			}
			groupBySetColumnPattern := []string{}
			for _, col := range columns {
				if _, exists := nullColumnNameMap[col]; exists {
					groupBySetColumnPattern = append(groupBySetColumnPattern, "NULL")
				} else {
					groupBySetColumnPattern = append(groupBySetColumnPattern, col)
				}
			}
			columnPatterns = append(columnPatterns, groupBySetColumnPattern)
			groupByColumnPatterns = append(groupByColumnPatterns, groupBySetColumns)
		}
		stmts := []string{}
		for i := 0; i < len(columnPatterns); i++ {
			var groupBy string
			if len(groupByColumnPatterns[i]) != 0 {
				groupBy = fmt.Sprintf("GROUP BY %s", strings.Join(groupByColumnPatterns[i], ","))
			}
			if strings.HasPrefix(input, "SELECT") {
				stmts = append(stmts, fmt.Sprintf("SELECT %s FROM ( %s %s )", strings.Join(columnPatterns[i], ","), input, groupBy))
			} else {
				stmts = append(stmts, fmt.Sprintf("SELECT %s %s %s", strings.Join(columnPatterns[i], ","), input, groupBy))
			}
		}
		return fmt.Sprintf(
			"%s ORDER BY %s",
			strings.Join(stmts, " UNION ALL "),
			strings.Join(groupByColumns, ","),
		), nil
	}
	var groupBy string
	if len(groupByColumns) > 0 {
		groupBy = fmt.Sprintf("GROUP BY %s", strings.Join(groupByColumns, ","))
	}
	if strings.HasPrefix(input, "SELECT") {
		return fmt.Sprintf("SELECT %s FROM ( %s %s )", strings.Join(columns, ","), input, groupBy), nil
	}
	return fmt.Sprintf("SELECT %s %s %s", strings.Join(columns, ","), input, groupBy), nil
}

func (n *AnonymizedAggregateScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SetOperationItemNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return newNode(n.node.Scan()).FormatSQL(ctx)
}

func (n *SetOperationScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	var opType string
	switch n.node.OpType() {
	case ast.SetOperationTypeUnionAll:
		opType = "UNION ALL"
	case ast.SetOperationTypeUnionDistinct:
		opType = "UNION DISTINCT"
	case ast.SetOperationTypeIntersectAll:
		opType = "INTERSECT ALL"
	case ast.SetOperationTypeIntersectDistinct:
		opType = "INTERSECT DISTINCT"
	case ast.SetOperationTypeExceptAll:
		opType = "EXCEPT ALL"
	case ast.SetOperationTypeExceptDistinct:
		opType = "EXCEPT DISTINCT"
	default:
		opType = "UNKONWN"
	}
	var queries []string
	for _, item := range n.node.InputItemList() {
		query, err := newNode(item).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		queries = append(queries, query)
	}
	return strings.Join(queries, fmt.Sprintf(" %s ", opType)), nil
}

func (n *OrderByScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range n.node.ColumnList() {
		colName := col.Name()
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
	for _, item := range n.node.OrderByItemList() {
		colName := item.ColumnRef().Column().Name()
		switch item.NullOrder() {
		case ast.NullOrderModeNullsFirst:
			orderByColumns = append(
				orderByColumns,
				fmt.Sprintf("(`%s` IS NOT NULL)", colName),
			)
		case ast.NullOrderModeNullsLast:
			orderByColumns = append(
				orderByColumns,
				fmt.Sprintf("(`%s` IS NULL)", colName),
			)
		}
		if item.IsDescending() {
			orderByColumns = append(orderByColumns, fmt.Sprintf("`%s` DESC", colName))
		} else {
			orderByColumns = append(orderByColumns, fmt.Sprintf("`%s`", colName))
		}
	}
	if strings.HasPrefix(input, "SELECT") {
		return fmt.Sprintf(
			"SELECT %s FROM ( %s ) ORDER BY %s",
			strings.Join(columns, ","),
			input,
			strings.Join(orderByColumns, ","),
		), nil
	}
	return fmt.Sprintf(
		"SELECT %s %s ORDER BY %s",
		strings.Join(columns, ","),
		input,
		strings.Join(orderByColumns, ","),
	), nil
}

func (n *LimitOffsetScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *WithRefScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	queryName := n.node.WithQueryName()
	return fmt.Sprintf("FROM %s", queryName), nil
}

func (n *AnalyticScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	orderColumnNames := analyticOrderColumnNamesFromContext(ctx)
	for _, group := range n.node.FunctionGroupList() {
		if group.PartitionBy() != nil {
			var partitionColumns []string
			for _, columnRef := range group.PartitionBy().PartitionByList() {
				ctx = withAnalyticTableName(ctx, columnRef.Column().TableName())
				partitionColumns = append(
					partitionColumns,
					fmt.Sprintf("`%s`", columnRef.Column().Name()),
				)
			}
			orderColumnNames.values = append(orderColumnNames.values, partitionColumns...)
			ctx = withAnalyticPartitionColumnNames(ctx, partitionColumns)
		}
		if group.OrderBy() != nil {
			var orderByColumns []string
			for _, item := range group.OrderBy().OrderByItemList() {
				ctx = withAnalyticTableName(ctx, item.ColumnRef().Column().TableName())
				orderByColumns = append(
					orderByColumns,
					fmt.Sprintf("`%s`", item.ColumnRef().Column().Name()),
				)
			}
			orderColumnNames.values = append(orderColumnNames.values, orderByColumns...)
		}
		if _, err := newNode(group).FormatSQL(ctx); err != nil {
			return "", err
		}
	}
	orderBy := fmt.Sprintf("ORDER BY %s", strings.Join(orderColumnNames.values, ","))
	orderColumnNames.values = []string{}
	return fmt.Sprintf("FROM ( SELECT *, ROW_NUMBER() OVER() AS `rowid` %s ) %s", input, orderBy), nil
}

func (n *SampleScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ComputedColumnNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	expr, err := newNode(n.node.Expr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	name := n.node.Column().Name()
	query := fmt.Sprintf("%s AS `%s`", expr, name)
	columnMap := columnRefMap(ctx)
	columnMap[name] = query
	arraySubqueryColumnNames := arraySubqueryColumnNameFromContext(ctx)
	if arraySubqueryColumnNames != nil {
		arraySubqueryColumnNames.names = append(arraySubqueryColumnNames.names, fmt.Sprintf("`%s`", name))
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
	if ref, exists := columnMap[n.node.Name()]; exists {
		return ref, nil
	}
	return fmt.Sprintf("`%s`", n.node.Name()), nil
}

func (n *ProjectScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	for _, col := range n.node.ExprList() {
		// assign expr to columnRefMap
		if _, err := newNode(col).FormatSQL(ctx); err != nil {
			return "", err
		}
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	_, isJoinScan := n.node.InputScan().(*ast.JoinScanNode)
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range n.node.ColumnList() {
		tableName := FormatName([]string{col.TableName()})
		colName := col.Name()
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else if isJoinScan {
			columns = append(
				columns,
				fmt.Sprintf("`%s`.`%s`", tableName, colName),
			)
		} else {
			columns = append(
				columns,
				fmt.Sprintf("`%s`", colName),
			)
		}
	}
	if strings.HasPrefix(input, "SELECT") {
		return fmt.Sprintf("SELECT %s FROM ( %s )", strings.Join(columns, ","), input), nil
	}
	return fmt.Sprintf("SELECT %s %s", strings.Join(columns, ","), input), nil
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

func (n *QueryStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return newNode(n.node.Query()).FormatSQL(ctx)
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
	return "", nil
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
	for _, entry := range n.node.WithEntryList() {
		sql, err := newNode(entry).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		queries = append(queries, sql)
	}
	query, err := newNode(n.node.Query()).FormatSQL(ctx)
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
	queryName := n.node.WithQueryName()
	subquery, err := newNode(n.node.WithSubquery()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
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
	orderColumnNames := analyticOrderColumnNamesFromContext(ctx)
	var queries []string
	for _, column := range n.node.AnalyticFunctionList() {
		sql, err := newNode(column).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		queries = append(queries, sql)
		orderColumnNames.values = append(
			orderColumnNames.values,
			fmt.Sprintf("`%s`", column.Column().Name()),
		)
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
	return newNode(n.node.Value()).FormatSQL(ctx)
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
	for _, value := range n.node.ValueList() {
		sql, err := newNode(value).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		values = append(values, sql)
	}
	return fmt.Sprintf("(%s)", strings.Join(values, ",")), nil
}

func (n *InsertStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	fullpath := fullNamePathFromContext(ctx)
	path := fullpath.paths[fullpath.idx]
	formattedTableName := FormatName(
		MergeNamePath(
			namePathFromContext(ctx),
			path,
		),
	)
	fullpath.idx++
	columns := []string{}
	for _, col := range n.node.InsertColumnList() {
		columns = append(columns, fmt.Sprintf("`%s`", col.Name()))
	}
	rows := []string{}
	for _, row := range n.node.RowList() {
		sql, err := newNode(row).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		rows = append(rows, sql)
	}
	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
		formattedTableName,
		strings.Join(columns, ","),
		strings.Join(rows, ","),
	), nil
}

func (n *DeleteStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	fullpath := fullNamePathFromContext(ctx)
	path := fullpath.paths[fullpath.idx]
	formattedTableName := FormatName(
		MergeNamePath(
			namePathFromContext(ctx),
			path,
		),
	)
	fullpath.idx++
	where, err := newNode(n.node.WhereExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"DELETE FROM `%s` WHERE %s",
		formattedTableName,
		where,
	), nil
}

func (n *UpdateItemNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	target, err := newNode(n.node.Target()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	setValue, err := newNode(n.node.SetValue()).FormatSQL(ctx)
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
	fullpath := fullNamePathFromContext(ctx)
	path := fullpath.paths[fullpath.idx]
	formattedTableName := FormatName(
		MergeNamePath(
			namePathFromContext(ctx),
			path,
		),
	)
	fullpath.idx++
	updateItems := []string{}
	for _, item := range n.node.UpdateItemList() {
		sql, err := newNode(item).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		updateItems = append(updateItems, sql)
	}
	where, err := newNode(n.node.WhereExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"UPDATE `%s` SET %s WHERE %s",
		formattedTableName,
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
	return "?", nil
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
