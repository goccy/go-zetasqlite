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

func getTableName(ctx context.Context, t types.Table) string {
	fullNamePathMap := fullNamePathMapFromContext(ctx)
	path := fullNamePathMap[t.Name()]
	return FormatName(
		MergeNamePath(
			namePathFromContext(ctx),
			path,
		),
	)
}

func uniqueColumnName(ctx context.Context, col *ast.Column) []byte {
	colName := string([]byte(col.Name()))
	if !useColumnID(ctx) {
		copied := make([]byte, 0, len(colName))
		copied = append(copied, colName...)
		return copied
	}
	colID := col.ColumnID()
	copied := make([]byte, 0, len(colName)+len(fmt.Sprint(colID))+1)
	copied = append(copied, fmt.Sprintf("%s#%d", colName, colID)...)
	return copied
}

func existsJoinExpr(node ast.Node) bool {
	var exists bool
	ast.Walk(node, func(n ast.Node) error {
		if _, ok := n.(*ast.JoinScanNode); ok {
			exists = true
		}
		return nil
	})
	return exists
}

type InputPattern int

const (
	InputKeep      InputPattern = 0
	InputNeedsWrap InputPattern = 1
	InputNeedsFrom InputPattern = 2
)

func getInputPattern(input string) InputPattern {
	trimmed := strings.TrimSpace(input)
	if len(trimmed) == 0 {
		return InputKeep
	}
	if strings.HasPrefix(trimmed, "FROM") {
		return InputKeep
	}
	if strings.HasPrefix(trimmed, "SELECT") {
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
	fullNamePathMap := fullNamePathMapFromContext(ctx)
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
		if node.Function().IsZetaSQLBuiltin() {
			return "", nil, fmt.Errorf("%s function is unimplemented", funcName)
		}
		path := fullNamePathMap[funcName]
		funcName = FormatName(
			MergeNamePath(
				namePathFromContext(ctx),
				path,
			),
		)
	}
	return funcName, args, nil
}

func (n *LiteralNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return JSONFromZetaSQLValue(n.node.Value()), nil
}

func (n *ParameterNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node.Name() == "" {
		return "?", nil
	}
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
	col := n.node.Column()
	colName := string(uniqueColumnName(ctx, col))
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
		colName := []byte(uniqueColumnName(ctx, columnRef.Column()))
		if item.IsDescending() {
			opts = append(opts, fmt.Sprintf("zetasqlite_order_by_string(`%s`, false)", string(colName)))
		} else {
			opts = append(opts, fmt.Sprintf("zetasqlite_order_by_string(`%s`, true)", string(colName)))
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
		arg, err := newNode(a).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		orderColumnNames.values = append(orderColumnNames.values, &analyticOrderBy{
			column: arg,
			isAsc:  true,
		})
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
	for _, col := range orderColumns {
		args = append(args, getWindowOrderByOptionFuncSQL(col.column, col.isAsc))
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
	input := analyticInputScanFromContext(ctx)
	return fmt.Sprintf(
		"( SELECT %s(%s) %s )",
		funcName,
		strings.Join(args, ","),
		input,
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
	if n.node == nil {
		return "", nil
	}
	typeSuffix := strings.ToLower(n.node.Type().TypeName(0))
	expr, err := newNode(n.node.Expr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("zetasqlite_cast_%s(%s)", typeSuffix, expr), nil
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
		if len(n.node.Subquery().ColumnList()) == 0 {
			return "", fmt.Errorf("failed to find computed column names for array subquery")
		}
		colName := string(uniqueColumnName(ctx, n.node.Subquery().ColumnList()[0]))
		return fmt.Sprintf("zetasqlite_array_array(`%s`) FROM (%s)", colName, sql), nil
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
	tableName := getTableName(ctx, n.node.Table())
	var columns []string
	for _, col := range n.node.ColumnList() {
		columns = append(
			columns,
			fmt.Sprintf("`%s` AS `%s`", col.Name(), uniqueColumnName(ctx, col)),
		)
	}
	return fmt.Sprintf("(SELECT %s FROM %s)", strings.Join(columns, ","), tableName), nil
}

func (n *JoinScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	left, err := newNode(n.node.LeftScan()).FormatSQL(withRowIDColumn(ctx))
	if err != nil {
		return "", err
	}
	right, err := newNode(n.node.RightScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	if getInputPattern(left) == InputNeedsWrap {
		left = fmt.Sprintf("(%s)", left)
	}
	if getInputPattern(right) == InputNeedsWrap {
		right = fmt.Sprintf("(%s)", right)
	}
	if n.node.JoinExpr() == nil {
		return fmt.Sprintf("%s CROSS JOIN %s", left, right), nil
	}
	joinExpr, err := newNode(n.node.JoinExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	switch n.node.JoinType() {
	case ast.JoinTypeInner:
		return fmt.Sprintf("%s JOIN %s ON %s", left, right, joinExpr), nil
	case ast.JoinTypeLeft:
		return fmt.Sprintf("%s LEFT JOIN %s ON %s", left, right, joinExpr), nil
	case ast.JoinTypeRight:
		// SQLite doesn't support RIGHT JOIN at v3.38.0, so emulate by using LEFT JOIN.
		// ROW_NUMBER() OVER() AS `row_id`
		return fmt.Sprintf("%s LEFT JOIN %s ON %s ORDER BY `row_id` NULLS LAST", right, left, joinExpr), nil
	case ast.JoinTypeFull:
		// SQLite doesn't support FULL OUTER JOIN at v3.38.0,
		// so emulate by combination of LEFT JOIN and UNION ALL and DISTINCT.
		var (
			columns   []string
			columnMap = columnRefMap(ctx)
		)
		for _, col := range n.node.ColumnList() {
			colName := string(uniqueColumnName(ctx, col))
			if ref, exists := columnMap[colName]; exists {
				columns = append(columns, ref)
				delete(columnMap, colName)
			} else {
				columns = append(columns, fmt.Sprintf("`%s`", colName))
			}
		}
		return fmt.Sprintf(
			"SELECT DISTINCT %[1]s FROM (SELECT %[1]s FROM %[2]s LEFT JOIN %[3]s ON %[4]s UNION ALL SELECT %[1]s FROM %[3]s LEFT JOIN %[2]s ON %[4]s)",
			strings.Join(columns, ","),
			left, right, joinExpr,
		), nil
	}
	return "", fmt.Errorf("unexpected join type %s", n.node.JoinType())
}

func (n *ArrayScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	arrayExpr, err := newNode(n.node.ArrayExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	colName := string(uniqueColumnName(ctx, n.node.ElementColumn()))
	if n.node.InputScan() != nil {
		input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		formattedInput, err := formatInput(input)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(
			"SELECT *, json_each.value AS `%s` %s, json_each(zetasqlite_decode_array_string(%s))",
			colName,
			formattedInput,
			arrayExpr,
		), nil
	}
	return fmt.Sprintf(
		"SELECT json_each.value AS `%s` FROM json_each(zetasqlite_decode_array_string(%s))",
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
	if strings.Contains(input, "WHERE") && input[len(input)-1] != ')' {
		// expected to qualify clause
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
	for _, agg := range n.node.AggregateList() {
		// assign sql to column ref map
		if _, err := newNode(agg).FormatSQL(ctx); err != nil {
			return "", err
		}
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	groupByColumns := []string{}
	groupByColumnMap := map[string]struct{}{}
	for _, col := range n.node.GroupByList() {
		if _, err := newNode(col).FormatSQL(ctx); err != nil {
			return "", err
		}
		colName := string(uniqueColumnName(ctx, col.Column()))
		groupByColumns = append(groupByColumns, fmt.Sprintf("`%s`", colName))
		groupByColumnMap[colName] = struct{}{}
	}
	if len(groupByColumns) != 0 {
		existsGroupBy := existsGroupByFromContext(ctx)
		if existsGroupBy != nil {
			existsGroupBy.exists = true
		}
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	columnNames := []string{}
	for _, col := range n.node.ColumnList() {
		colName := string(uniqueColumnName(ctx, col))
		columnNames = append(columnNames, colName)
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else {
			columns = append(columns, fmt.Sprintf("`%s`", colName))
		}
	}
	if needsRowIDColumn(ctx) {
		columns = append(
			columns,
			"ROW_NUMBER() OVER() AS `row_id`",
		)
	}
	if len(n.node.GroupingSetList()) != 0 {
		columnPatterns := [][]string{}
		groupByColumnPatterns := [][]string{}
		for _, set := range n.node.GroupingSetList() {
			groupBySetColumns := []string{}
			groupBySetColumnMap := map[string]struct{}{}
			for _, col := range set.GroupByColumnList() {
				colName := string(uniqueColumnName(ctx, col.Column()))
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
			groupByColumnPatterns = append(groupByColumnPatterns, groupBySetColumns)
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
				stmts = append(stmts, fmt.Sprintf("SELECT %s FROM (%s %s)", formattedColumns, input, groupBy))
			case InputNeedsFrom:
				stmts = append(stmts, fmt.Sprintf("SELECT %s FROM %s %s", formattedColumns, input, groupBy))
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
	formattedColumns := strings.Join(columns, ",")
	switch getInputPattern(input) {
	case InputKeep:
		return fmt.Sprintf("SELECT %s %s %s", formattedColumns, input, groupBy), nil
	case InputNeedsWrap:
		return fmt.Sprintf("SELECT %s FROM (%s %s)", formattedColumns, input, groupBy), nil
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
	columnMaps := []string{}
	if len(n.node.InputItemList()) != 0 {
		for idx, col := range n.node.InputItemList()[0].OutputColumnList() {
			columnMaps = append(
				columnMaps,
				fmt.Sprintf(
					"`%s` AS `%s`",
					uniqueColumnName(ctx, col),
					uniqueColumnName(ctx, n.node.ColumnList()[idx]),
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
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range n.node.ColumnList() {
		colName := string(uniqueColumnName(ctx, col))
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
		colName := uniqueColumnName(ctx, item.ColumnRef().Column())
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
	return "", nil
}

func (n *WithRefScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	tableName := n.node.WithQueryName()
	tableToColumnListMap := tableNameToColumnListMap(ctx)
	columnDefs := tableToColumnListMap[tableName]
	columns := n.node.ColumnList()
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
	if needsRowIDColumn(ctx) {
		formattedColumns = append(
			formattedColumns,
			"ROW_NUMBER() OVER() AS `row_id`",
		)
	}
	return fmt.Sprintf("(SELECT %s FROM %s)", strings.Join(formattedColumns, ","), tableName), nil
}

func (n *AnalyticScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	formattedInput, err := formatInput(input)
	if err != nil {
		return "", err
	}
	ctx = withAnalyticInputScan(ctx, formattedInput)
	orderColumnNames := analyticOrderColumnNamesFromContext(ctx)
	for _, group := range n.node.FunctionGroupList() {
		if group.PartitionBy() != nil {
			var partitionColumns []string
			for _, columnRef := range group.PartitionBy().PartitionByList() {
				colName := fmt.Sprintf("`%s`", uniqueColumnName(ctx, columnRef.Column()))
				partitionColumns = append(
					partitionColumns,
					colName,
				)
				orderColumnNames.values = append(orderColumnNames.values, &analyticOrderBy{
					column: colName,
					isAsc:  true,
				})
			}
			ctx = withAnalyticPartitionColumnNames(ctx, partitionColumns)
		}
		if group.OrderBy() != nil {
			var orderByColumns []string
			for _, item := range group.OrderBy().OrderByItemList() {
				colName := uniqueColumnName(ctx, item.ColumnRef().Column())
				formattedColName := fmt.Sprintf("`%s`", colName)
				orderByColumns = append(
					orderByColumns,
					string(formattedColName),
				)
				orderColumnNames.values = append(orderColumnNames.values, &analyticOrderBy{
					column: string(formattedColName),
					isAsc:  !item.IsDescending(),
				})
			}
		}
		if _, err := newNode(group).FormatSQL(ctx); err != nil {
			return "", err
		}
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range n.node.ColumnList() {
		colName := string(uniqueColumnName(ctx, col))
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
	for _, col := range orderColumnNames.values {
		if col.isAsc {
			orderColumnFormattedNames = append(
				orderColumnFormattedNames,
				col.column,
			)
		} else {
			orderColumnFormattedNames = append(
				orderColumnFormattedNames,
				fmt.Sprintf("%s DESC", col.column),
			)
		}
	}
	orderBy := fmt.Sprintf("ORDER BY %s", strings.Join(orderColumnFormattedNames, ","))
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
	expr, err := newNode(n.node.Expr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	col := n.node.Column()
	uniqueName := string(uniqueColumnName(ctx, col))
	query := fmt.Sprintf("%s AS `%s`", expr, uniqueColumnName(ctx, col))
	columnMap := columnRefMap(ctx)
	columnMap[uniqueName] = query
	arraySubqueryColumnNames := arraySubqueryColumnNameFromContext(ctx)
	if arraySubqueryColumnNames != nil {
		arraySubqueryColumnNames.names = append(arraySubqueryColumnNames.names, fmt.Sprintf("`%s`", col.Name()))
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
	col := n.node.Column()
	uniqueName := string(uniqueColumnName(ctx, col))
	if ref, exists := columnMap[uniqueName]; exists {
		return ref, nil
	}
	return fmt.Sprintf("`%s`", col.Name()), nil
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
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range n.node.ColumnList() {
		colName := string(uniqueColumnName(ctx, col))
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
	if needsRowIDColumn(ctx) {
		columns = append(
			columns,
			"ROW_NUMBER() OVER() AS `row_id`",
		)
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
	tableToColumnList := tableNameToColumnListMap(ctx)
	tableToColumnList[queryName] = n.node.WithSubquery().ColumnList()
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
	for _, column := range n.node.AnalyticFunctionList() {
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
	table := getTableName(ctx, n.node.TableScan().Table())
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
		table,
		strings.Join(columns, ","),
		strings.Join(rows, ","),
	), nil
}

func (n *DeleteStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	table := getTableName(ctx, n.node.TableScan().Table())
	where, err := newNode(n.node.WhereExpr()).FormatSQL(ctx)
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
	table := getTableName(ctx, n.node.TableScan().Table())
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
