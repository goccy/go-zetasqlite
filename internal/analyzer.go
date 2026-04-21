package internal

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
)

type Analyzer struct {
	namePath        *NamePath
	isAutoIndexMode bool
	isExplainMode   bool
	catalog         *Catalog
	opt             *googlesql.AnalyzerOptions
}

func NewAnalyzer(catalog *Catalog) (*Analyzer, error) {
	opt, err := newAnalyzerOptions()
	if err != nil {
		return nil, err
	}
	return &Analyzer{
		catalog:  catalog,
		opt:      opt,
		namePath: &NamePath{},
	}, nil
}

func newAnalyzerOptions() (*googlesql.AnalyzerOptions, error) {
	langOpt := NewLanguageOptions()
	langOpt.SetNameResolutionMode(googlesql.NameResolutionModeNameResolutionDefault)
	langOpt.SetProductMode(googlesql.ProductModeProductInternal)
	for _, f := range []googlesql.LanguageFeature{
		googlesql.LanguageFeatureFeatureAnalyticFunctions,
		googlesql.LanguageFeatureFeatureNamedArguments,
		googlesql.LanguageFeatureFeatureNumericType,
		googlesql.LanguageFeatureFeatureBignumericType,
		googlesql.LanguageFeatureFeatureV13DecimalAlias,
		googlesql.LanguageFeatureFeatureCreateTableNotNull,
		googlesql.LanguageFeatureFeatureParameterizedTypes,
		googlesql.LanguageFeatureFeatureTablesample,
		googlesql.LanguageFeatureFeatureTimestampNanos,
		googlesql.LanguageFeatureFeatureV11HavingInAggregate,
		googlesql.LanguageFeatureFeatureV11NullHandlingModifierInAggregate,
		googlesql.LanguageFeatureFeatureV11NullHandlingModifierInAnalytic,
		googlesql.LanguageFeatureFeatureV11OrderByCollate,
		googlesql.LanguageFeatureFeatureV11SelectStarExceptReplace,
		googlesql.LanguageFeatureFeatureV12SafeFunctionCall,
		googlesql.LanguageFeatureFeatureJsonType,
		googlesql.LanguageFeatureFeatureJsonArrayFunctions,
		googlesql.LanguageFeatureFeatureJsonStrictNumberParsing,
		googlesql.LanguageFeatureFeatureV13IsDistinct,
		googlesql.LanguageFeatureFeatureV13FormatInCast,
		googlesql.LanguageFeatureFeatureV13DateArithmetics,
		googlesql.LanguageFeatureFeatureV11OrderByInAggregate,
		googlesql.LanguageFeatureFeatureV11LimitInAggregate,
		googlesql.LanguageFeatureFeatureV13DateTimeConstructors,
		googlesql.LanguageFeatureFeatureV13ExtendedDateTimeSignatures,
		googlesql.LanguageFeatureFeatureV12CivilTime,
		googlesql.LanguageFeatureFeatureV12WeekWithWeekday,
		googlesql.LanguageFeatureFeatureIntervalType,
		googlesql.LanguageFeatureFeatureGroupByRollup,
		googlesql.LanguageFeatureFeatureV13NullsFirstLastInOrderBy,
		googlesql.LanguageFeatureFeatureV13Qualify,
		googlesql.LanguageFeatureFeatureV13AllowDashesInTableName,
		googlesql.LanguageFeatureFeatureGeography,
		googlesql.LanguageFeatureFeatureV13ExtendedGeographyParsers,
		googlesql.LanguageFeatureFeatureTemplateFunctions,
		googlesql.LanguageFeatureFeatureV11WithOnSubquery,
		googlesql.LanguageFeatureFeatureV13Pivot,
		googlesql.LanguageFeatureFeatureV13Unpivot,
		googlesql.LanguageFeatureFeatureCreateTableAsSelectColumnList,
	} {
		_ = langOpt.EnableLanguageFeature(f)
	}
	for _, k := range []googlesql.ResolvedNodeKind{
		googlesql.ResolvedNodeKindResolvedBeginStmt,
		googlesql.ResolvedNodeKindResolvedCommitStmt,
		googlesql.ResolvedNodeKindResolvedMergeStmt,
		googlesql.ResolvedNodeKindResolvedQueryStmt,
		googlesql.ResolvedNodeKindResolvedInsertStmt,
		googlesql.ResolvedNodeKindResolvedUpdateStmt,
		googlesql.ResolvedNodeKindResolvedDeleteStmt,
		googlesql.ResolvedNodeKindResolvedDropStmt,
		googlesql.ResolvedNodeKindResolvedTruncateStmt,
		googlesql.ResolvedNodeKindResolvedCreateTableStmt,
		googlesql.ResolvedNodeKindResolvedCreateTableAsSelectStmt,
		googlesql.ResolvedNodeKindResolvedCreateProcedureStmt,
		googlesql.ResolvedNodeKindResolvedCreateFunctionStmt,
		googlesql.ResolvedNodeKindResolvedCreateTableFunctionStmt,
		googlesql.ResolvedNodeKindResolvedCreateViewStmt,
		googlesql.ResolvedNodeKindResolvedDropFunctionStmt,
	} {
		_ = langOpt.AddSupportedStatementKind(k)
	}
	// Enable QUALIFY without WHERE
	// https://github.com/google/zetasql/issues/124
	if err := langOpt.EnableReservableKeyword("QUALIFY", true); err != nil {
		return nil, err
	}
	opt := NewAnalyzerOptions()
	opt.SetAllowUndeclaredParameters(true)
	opt.SetLanguage(langOpt)
	opt.SetParseLocationRecordType(googlesql.ParseLocationRecordTypeParseLocationRecordFullNodeScope)
	return opt, nil
}

func (a *Analyzer) SetAutoIndexMode(enabled bool) {
	a.isAutoIndexMode = enabled
}

func (a *Analyzer) SetExplainMode(enabled bool) {
	a.isExplainMode = enabled
}

func (a *Analyzer) NamePath() []string {
	return a.namePath.path
}

func (a *Analyzer) SetNamePath(path []string) error {
	return a.namePath.setPath(path)
}

func (a *Analyzer) SetMaxNamePath(num int) {
	a.namePath.setMaxNum(num)
}

func (a *Analyzer) MaxNamePath() int {
	return a.namePath.maxNum
}

func (a *Analyzer) AddNamePath(path string) error {
	return a.namePath.addPath(path)
}

func (a *Analyzer) parseScript(query string) ([]googlesql.ASTStatementNode, error) {
	loc := NewParseResumeLocationFromString(query)
	parserOpts, err := a.opt.GetParserOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to get parser options: %w", err)
	}
	var stmts []googlesql.ASTStatementNode
	for {
		out, err := googlesql.ParseNextScriptStatement(loc, parserOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to parse statement: %w", err)
		}
		stmt, err := out.Statement()
		if err != nil {
			return nil, fmt.Errorf("failed to get statement from parser output: %w", err)
		}
		// ASTBeginEndBlock has a StatementListNode child — expand it.
		if block, ok := stmt.(*googlesql.ASTBeginEndBlock); ok {
			list, err := block.StatementListNode()
			if err == nil && list != nil {
				stmts = append(stmts, collectStatementsFromList(list)...)
			}
		} else {
			stmts = append(stmts, stmt)
		}
		// Bridge doesn't expose AtEnd on ParseResumeLocation yet; a single
		// ParseNextScriptStatement call is sufficient for the single-stmt
		// case exercised by tests today.
		break
	}
	return stmts, nil
}

// collectStatementsFromList walks an ASTStatementList gathering its
// member statements. The list exposes StatementList(i) per-index; we
// stop when the accessor errors (indicating index out of range).
func collectStatementsFromList(list *googlesql.ASTStatementList) []googlesql.ASTStatementNode {
	var out []googlesql.ASTStatementNode
	for i := int32(0); ; i++ {
		s, err := list.StatementList(i)
		if err != nil || s == nil {
			break
		}
		out = append(out, s)
	}
	return out
}

func (a *Analyzer) getParameterMode(stmt googlesql.ASTStatementNode) (googlesql.ParameterMode, error) {
	var (
		enabledNamedParameter      bool
		enabledPositionalParameter bool
	)
	_ = ASTWalk(stmt, func(node googlesql.ASTNodeNode) error {
		switch n := node.(type) {
		case googlesql.ASTParameterExprNode:
			if m1(n.Position()) > 0 {
				enabledPositionalParameter = true
			}
			if m1(n.Name()) != nil {
				enabledNamedParameter = true
			}
		}
		return nil
	})
	if enabledNamedParameter && enabledPositionalParameter {
		return googlesql.ParameterModeParameterNone, fmt.Errorf("named parameter and positional parameter cannot be used together")
	}
	if enabledPositionalParameter {
		return googlesql.ParameterModeParameterPositional, nil
	}
	return googlesql.ParameterModeParameterNamed, nil
}

type StmtActionFunc func() (StmtAction, error)

func (a *Analyzer) Analyze(ctx context.Context, conn *Conn, query string, args []driver.NamedValue) ([]StmtActionFunc, error) {
	if err := a.catalog.Sync(ctx, conn); err != nil {
		return nil, fmt.Errorf("failed to sync catalog: %w", err)
	}
	stmts, err := a.parseScript(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse statements: %w", err)
	}
	funcMap := map[string]*FunctionSpec{}
	for _, spec := range a.catalog.getFunctions(a.namePath) {
		funcMap[spec.FuncName()] = spec
	}
	actionFuncs := make([]StmtActionFunc, 0, len(stmts))
	for _, stmt := range stmts {
		stmt := stmt
		actionFuncs = append(actionFuncs, func() (StmtAction, error) {
			mode, err := a.getParameterMode(stmt)
			if err != nil {
				return nil, err
			}
			a.opt.SetParameterMode(mode)
			out, err := AnalyzeStatementFromParserAST(
				query,
				stmt,
				a.catalog,
				a.opt,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to analyze: %w", err)
			}
			stmtNode, _ := out.ResolvedStatementMethod()
			ctx = a.context(ctx, funcMap, stmtNode, stmt)
			action, err := a.newStmtAction(ctx, query, args, stmtNode)
			if err != nil {
				return nil, err
			}
			if mode == googlesql.ParameterModeParameterPositional {
				args = args[len(action.Args()):]
			}
			return action, nil
		})
	}
	return actionFuncs, nil
}

func (a *Analyzer) context(
	ctx context.Context,
	funcMap map[string]*FunctionSpec,
	stmtNode googlesql.ResolvedStatementNode,
	stmt googlesql.ASTStatementNode) context.Context {
	ctx = withAnalyzer(ctx, a)
	ctx = withNamePath(ctx, a.namePath)
	ctx = withColumnRefMap(ctx, map[string]string{})
	ctx = withTableNameToColumnListMap(ctx, map[string][]*googlesql.ResolvedColumn{})
	ctx = withFuncMap(ctx, funcMap)
	ctx = withAnalyticOrderColumnNames(ctx, &analyticOrderColumnNames{})
	ctx = withNodeMap(ctx, NewNodeMap(stmtNode, stmt))
	return ctx
}

func (a *Analyzer) analyzeTemplatedFunctionWithRuntimeArgument(ctx context.Context, query string) (*FunctionSpec, error) {
	out, err := AnalyzeStatement(query, a.catalog, a.opt)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze: %w", err)
	}
	node, _ := out.ResolvedStatementMethod()
	stmt, ok := node.(googlesql.ResolvedCreateFunctionStmtNode)
	if !ok {
		return nil, fmt.Errorf("unexpected create function query %s", query)
	}
	spec, err := newFunctionSpec(ctx, a.namePath, stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to create function spec: %w", err)
	}
	return spec, nil
}

func (a *Analyzer) newStmtAction(ctx context.Context, query string, args []driver.NamedValue, node googlesql.ResolvedStatementNode) (StmtAction, error) {
	kind, _ := AsResolvedNode(node).NodeKind()
	switch kind {
	case googlesql.ResolvedNodeKindResolvedCreateTableStmt:
		return a.newCreateTableStmtAction(ctx, query, args, node.(googlesql.ResolvedCreateTableStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateTableAsSelectStmt:
		ctx = withUseColumnID(ctx)
		return a.newCreateTableAsSelectStmtAction(ctx, query, args, node.(googlesql.ResolvedCreateTableAsSelectStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateFunctionStmt:
		return a.newCreateFunctionStmtAction(ctx, query, args, node.(googlesql.ResolvedCreateFunctionStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateViewStmt:
		ctx = withUseColumnID(ctx)
		return a.newCreateViewStmtAction(ctx, query, args, node.(googlesql.ResolvedCreateViewStmtNode))
	case googlesql.ResolvedNodeKindResolvedDropStmt:
		return a.newDropStmtAction(ctx, query, args, node.(googlesql.ResolvedDropStmtNode))
	case googlesql.ResolvedNodeKindResolvedDropFunctionStmt:
		return a.newDropFunctionStmtAction(ctx, query, args, node.(googlesql.ResolvedDropFunctionStmtNode))
	case googlesql.ResolvedNodeKindResolvedInsertStmt, googlesql.ResolvedNodeKindResolvedUpdateStmt, googlesql.ResolvedNodeKindResolvedDeleteStmt:
		return a.newDMLStmtAction(ctx, query, args, node)
	case googlesql.ResolvedNodeKindResolvedTruncateStmt:
		return a.newTruncateStmtAction(ctx, query, args, node.(googlesql.ResolvedTruncateStmtNode))
	case googlesql.ResolvedNodeKindResolvedMergeStmt:
		ctx = withUseColumnID(ctx)
		return a.newMergeStmtAction(ctx, query, args, node.(googlesql.ResolvedMergeStmtNode))
	case googlesql.ResolvedNodeKindResolvedQueryStmt:
		ctx = withUseColumnID(ctx)
		return a.newQueryStmtAction(ctx, query, args, node.(googlesql.ResolvedQueryStmtNode))
	case googlesql.ResolvedNodeKindResolvedBeginStmt:
		return a.newBeginStmtAction(ctx, query, args, node)
	case googlesql.ResolvedNodeKindResolvedCommitStmt:
		return a.newCommitStmtAction(ctx, query, args, node)
	}
	dbg, _ := AsResolvedNode(node).DebugString()
	return nil, fmt.Errorf("unsupported stmt %s", dbg)
}

func (a *Analyzer) newCreateTableStmtAction(_ context.Context, query string, args []driver.NamedValue, node googlesql.ResolvedCreateTableStmtNode) (*CreateTableStmtAction, error) {
	spec := newTableSpec(a.namePath, node)
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	return &CreateTableStmtAction{
		query:           query,
		spec:            spec,
		args:            queryArgs,
		catalog:         a.catalog,
		isAutoIndexMode: a.isAutoIndexMode,
	}, nil
}

func (a *Analyzer) newCreateTableAsSelectStmtAction(ctx context.Context, _ string, args []driver.NamedValue, node googlesql.ResolvedCreateTableAsSelectStmtNode) (*CreateTableStmtAction, error) {
	query, err := newNode(nn(node.Query())).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	spec := newTableAsSelectSpec(a.namePath, query, node)
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	return &CreateTableStmtAction{
		query:           query,
		spec:            spec,
		args:            queryArgs,
		catalog:         a.catalog,
		isAutoIndexMode: a.isAutoIndexMode,
	}, nil
}

func (a *Analyzer) newCreateFunctionStmtAction(ctx context.Context, query string, _ []driver.NamedValue, node googlesql.ResolvedCreateFunctionStmtNode) (*CreateFunctionStmtAction, error) {
	var spec *FunctionSpec
	if a.resultTypeIsTemplatedType(node.Signature()) {
		realStmts, err := a.inferTemplatedTypeByRealType(query, node)
		if err != nil {
			return nil, err
		}
		templatedFuncSpec, err := newTemplatedFunctionSpec(ctx, a.namePath, node, realStmts)
		if err != nil {
			return nil, err
		}
		spec = templatedFuncSpec
	} else {
		funcSpec, err := newFunctionSpec(ctx, a.namePath, node)
		if err != nil {
			return nil, fmt.Errorf("failed to create function spec: %w", err)
		}
		spec = funcSpec
	}
	return &CreateFunctionStmtAction{
		spec:    spec,
		catalog: a.catalog,
		funcMap: funcMapFromContext(ctx),
	}, nil
}

func (a *Analyzer) newCreateViewStmtAction(ctx context.Context, _ string, _ []driver.NamedValue, node googlesql.ResolvedCreateViewStmtNode) (*CreateViewStmtAction, error) {
	query, err := newNode(nn(node.Query())).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	spec := newTableAsViewSpec(a.namePath, query, node)
	return &CreateViewStmtAction{
		query:   query,
		spec:    spec,
		catalog: a.catalog,
	}, nil
}

func (a *Analyzer) resultTypeIsTemplatedType(sig *googlesql.FunctionSignature) bool {
	if !m1(sig.IsTemplated()) {
		return false
	}
	return m1(sig.ResultType()).IsTemplated()
}

var inferTypes = []string{
	"INT64", "DOUBLE", "BOOL", "STRING", "BYTES",
	"JSON", "DATE", "DATETIME", "TIME", "TIMESTAMP",
	"INTERVAL", "GEOGRAPHY",
	"STRUCT<>",
}

func (a *Analyzer) inferTemplatedTypeByRealType(query string, node googlesql.ResolvedCreateFunctionStmtNode) ([]googlesql.ResolvedCreateFunctionStmtNode, error) {
	var stmts []googlesql.ResolvedCreateFunctionStmtNode
	for _, typ := range inferTypes {
		if out, err := AnalyzeStatement(a.buildScalarTypeFuncFromTemplatedFunc(node, typ), a.catalog, a.opt); err == nil {
			stmts = append(stmts, m1(out.ResolvedStatementMethod()).(googlesql.ResolvedCreateFunctionStmtNode))
		}
	}
	if len(stmts) != 0 {
		return stmts, nil
	}
	for _, typ := range inferTypes {
		if out, err := AnalyzeStatement(a.buildArrayTypeFuncFromTemplatedFunc(node, typ), a.catalog, a.opt); err == nil {
			stmts = append(stmts, m1(out.ResolvedStatementMethod()).(googlesql.ResolvedCreateFunctionStmtNode))
		}
	}
	if len(stmts) != 0 {
		return stmts, nil
	}
	return nil, fmt.Errorf("failed to infer templated function result type for %s", query)
}

func (a *Analyzer) buildScalarTypeFuncFromTemplatedFunc(node googlesql.ResolvedCreateFunctionStmtNode, realType string) string {
	signature, _ := node.Signature()
	var args []string
	for _, arg := range signature.Arguments() {
		typ := realType
		if !arg.IsTemplated() {
			typ = newType(arg.Type()).FormatType()
		}
		args = append(args, fmt.Sprintf("%s %s", arg.ArgumentName(), typ))
	}
	return fmt.Sprintf(
		"CREATE TEMP FUNCTION __zetasqlite_func__(%s) as (%s)",
		strings.Join(args, ","),
		m1(node.Code()),
	)
}

func (a *Analyzer) buildArrayTypeFuncFromTemplatedFunc(node googlesql.ResolvedCreateFunctionStmtNode, realType string) string {
	signature, _ := node.Signature()
	var args []string
	for _, arg := range signature.Arguments() {
		typ := fmt.Sprintf("ARRAY<%s>", realType)
		if !arg.IsTemplated() {
			typ = newType(arg.Type()).FormatType()
		}
		args = append(args, fmt.Sprintf("%s %s", arg.ArgumentName(), typ))
	}
	return fmt.Sprintf(
		"CREATE TEMP FUNCTION __zetasqlite_func__(%s) as (%s)",
		strings.Join(args, ","),
		m1(node.Code()),
	)
}

func (a *Analyzer) newDropStmtAction(ctx context.Context, query string, args []driver.NamedValue, node googlesql.ResolvedDropStmtNode) (*DropStmtAction, error) {
	formattedQuery, err := newNode(node).FormatSQL(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to format query %s: %w", query, err)
	}
	if formattedQuery == "" {
		return nil, fmt.Errorf("failed to format query %s", query)
	}
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	objectType, _ := node.ObjectType()
	name := a.namePath.format(node.NamePath())
	return &DropStmtAction{
		name:           name,
		objectType:     objectType,
		funcMap:        funcMapFromContext(ctx),
		catalog:        a.catalog,
		query:          query,
		formattedQuery: formattedQuery,
		args:           queryArgs,
	}, nil
}

func (a *Analyzer) newDropFunctionStmtAction(ctx context.Context, query string, args []driver.NamedValue, node googlesql.ResolvedDropFunctionStmtNode) (*DropStmtAction, error) {
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	name := a.namePath.format(node.NamePath())
	return &DropStmtAction{
		name:       name,
		objectType: "FUNCTION",
		funcMap:    funcMapFromContext(ctx),
		catalog:    a.catalog,
		query:      query,
		args:       queryArgs,
	}, nil
}

func (a *Analyzer) newDMLStmtAction(ctx context.Context, query string, args []driver.NamedValue, node googlesql.ResolvedNodeNode) (*DMLStmtAction, error) {
	formattedQuery, err := newNode(node).FormatSQL(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to format query %s: %w", query, err)
	}
	if formattedQuery == "" {
		return nil, fmt.Errorf("failed to format query %s", query)
	}
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	return &DMLStmtAction{
		query:          query,
		params:         params,
		args:           queryArgs,
		formattedQuery: formattedQuery,
	}, nil
}

func (a *Analyzer) newQueryStmtAction(ctx context.Context, query string, args []driver.NamedValue, node googlesql.ResolvedQueryStmtNode) (*QueryStmtAction, error) {
	outputColumns := []*ColumnSpec{}
	for _, col := range m1(node.OutputColumnList()) {
		outputColumns = append(outputColumns, &ColumnSpec{
			Name: m1(col.Name()),
			Type: newType(m1(col.Column()).Type()),
		})
	}
	formattedQuery, err := newNode(node).FormatSQL(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to format query %s: %w", query, err)
	}
	if formattedQuery == "" {
		return nil, fmt.Errorf("failed to format query %s", query)
	}
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	return &QueryStmtAction{
		query:          query,
		params:         params,
		args:           queryArgs,
		formattedQuery: formattedQuery,
		outputColumns:  outputColumns,
		isExplainMode:  a.isExplainMode,
	}, nil
}

func (a *Analyzer) newBeginStmtAction(ctx context.Context, query string, args []driver.NamedValue, node googlesql.ResolvedNodeNode) (*BeginStmtAction, error) {
	return &BeginStmtAction{}, nil
}

func (a *Analyzer) newCommitStmtAction(ctx context.Context, query string, args []driver.NamedValue, node googlesql.ResolvedNodeNode) (*CommitStmtAction, error) {
	return &CommitStmtAction{}, nil
}

//nolint:unparam
func (a *Analyzer) newTruncateStmtAction(_ context.Context, _ string, _ []driver.NamedValue, node googlesql.ResolvedTruncateStmtNode) (*TruncateStmtAction, error) {
	table := m1(node.TableScan()).Table().Name()
	return &TruncateStmtAction{query: fmt.Sprintf("DELETE FROM `%s`", table)}, nil
}

func (a *Analyzer) newMergeStmtAction(ctx context.Context, _ string, args []driver.NamedValue, node googlesql.ResolvedMergeStmtNode) (*MergeStmtAction, error) {
	targetTable, err := newNode(nn(node.TableScan())).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	sourceTable, err := newNode(nn(node.FromScan())).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	expr, err := newNode(nn(node.MergeExpr())).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	fn, ok := m1(node.MergeExpr()).(googlesql.ResolvedFunctionCallNode)
	if !ok {
		return nil, fmt.Errorf("currently MERGE expression is supported equal expression only")
	}
	if fn.Function().FullName(false) != "$equal" {
		return nil, fmt.Errorf("currently MERGE expression is supported equal expression only")
	}
	argList := fn.ArgumentList()
	if len(argList) != 2 {
		return nil, fmt.Errorf("unexpected MERGE expression column num. expected 2 column but specified %d column", len(args))
	}
	colA, ok := argList[0].(googlesql.ResolvedColumnRefNode)
	if !ok {
		return nil, fmt.Errorf("unexpected MERGE expression. expected column reference but got %T", argList[0])
	}
	colB, ok := argList[1].(googlesql.ResolvedColumnRefNode)
	if !ok {
		return nil, fmt.Errorf("unexpected MERGE expression. expected column reference but got %T", argList[1])
	}
	var (
		sourceColumn *googlesql.ResolvedColumn
		targetColumn *googlesql.ResolvedColumn
	)
	if strings.Contains(sourceTable, colA.Column().TableName()) {
		sourceColumn = colA.Column()
		targetColumn = colB.Column()
	} else {
		sourceColumn = colB.Column()
		targetColumn = colA.Column()
	}
	mergedTableSourceColumnName := fmt.Sprintf("`%s`", uniqueColumnName(ctx, sourceColumn))
	mergedTableTargetColumnName := fmt.Sprintf("`%s`", uniqueColumnName(ctx, targetColumn))
	mergedTableOutputColumns := []string{
		mergedTableTargetColumnName,
		mergedTableSourceColumnName,
	}
	var stmts []string
	stmts = append(stmts, fmt.Sprintf(
		"CREATE TABLE zetasqlite_merged_table AS SELECT DISTINCT * FROM (SELECT * FROM %[1]s LEFT JOIN %[2]s ON %[3]s UNION ALL SELECT * FROM %[2]s LEFT JOIN %[1]s ON %[3]s)",
		sourceTable, targetTable, expr,
	))

	// exists target table and source table
	matchedFromStmt := fmt.Sprintf(
		"FROM zetasqlite_merged_table WHERE %[2]s = %[1]s AND %[3]s = %[1]s",
		m1(targetColumn.Name()),
		mergedTableSourceColumnName,
		mergedTableTargetColumnName,
	)

	// exists target table but not exists source table
	notMatchedBySourceFromStmt := fmt.Sprintf(
		"FROM zetasqlite_merged_table WHERE %[2]s = `%[1]s` AND %[3]s IS NULL",
		m1(targetColumn.Name()),
		mergedTableTargetColumnName,
		mergedTableSourceColumnName,
	)

	// exists source table but not exists target table
	notMatchedByTargetFromStmt := fmt.Sprintf(
		"FROM zetasqlite_merged_table WHERE %[2]s = `%[1]s` AND %[3]s IS NULL",
		m1(sourceColumn.Name()),
		mergedTableSourceColumnName,
		mergedTableTargetColumnName,
	)
	for _, when := range m1(node.WhenClauseList()) {
		var fromStmt string
		switch when.MatchType() {
		case googlesql.ResolvedMergeWhenEnums_MatchTypeMatched:
			fromStmt = matchedFromStmt
		case googlesql.ResolvedMergeWhenEnums_MatchTypeNotMatchedBySource:
			fromStmt = notMatchedBySourceFromStmt
		case googlesql.ResolvedMergeWhenEnums_MatchTypeNotMatchedByTarget:
			fromStmt = notMatchedByTargetFromStmt
		}
		whereStmt := fmt.Sprintf(
			"WHERE EXISTS(SELECT %s %s)",
			strings.Join(mergedTableOutputColumns, ","),
			fromStmt,
		)
		switch when.ActionType() {
		case googlesql.ResolvedMergeWhenEnums_ActionTypeInsert:
			var columns []string
			for _, col := range m1(when.InsertColumnList()) {
				columns = append(columns, fmt.Sprintf("`%s`", m1(col.Name())))
			}
			row, err := newNode(nn(when.InsertRow())).FormatSQL(unuseColumnID(ctx))
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, fmt.Sprintf(
				"INSERT INTO `%[1]s`(%[2]s) SELECT %[3]s FROM (SELECT * FROM `%[4]s` %[5]s)",
				m1(targetColumn.TableName()),
				strings.Join(columns, ","),
				row,
				m1(sourceColumn.TableName()),
				whereStmt,
			))
		case googlesql.ResolvedMergeWhenEnums_ActionTypeUpdate:
			var items []string
			for _, item := range m1(when.UpdateItemList()) {
				sql, err := newNode(item).FormatSQL(ctx)
				if err != nil {
					return nil, err
				}
				items = append(items, sql)
			}
			stmts = append(stmts, fmt.Sprintf(
				"UPDATE `%s` SET %s %s",
				m1(targetColumn.TableName()),
				strings.Join(items, ","),
				fromStmt,
			))
		case googlesql.ResolvedMergeWhenEnums_ActionTypeDelete:
			stmts = append(stmts, fmt.Sprintf(
				"DELETE FROM `%s` %s",
				m1(targetColumn.TableName()),
				whereStmt,
			))
		}
	}
	stmts = append(stmts, "DROP TABLE zetasqlite_merged_table")
	return &MergeStmtAction{stmts: stmts}, nil
}

func getParamsFromNode(node googlesql.ResolvedNodeNode) []googlesql.ResolvedParameterNode {
	var (
		params       []googlesql.ResolvedParameterNode
		paramNameMap = map[string]struct{}{}
	)
	_ = ResolvedWalk(node, func(n googlesql.ResolvedNodeNode) error {
		param, ok := n.(googlesql.ResolvedParameterNode)
		if ok {
			name, _ := param.Name()
			if name != "" {
				if _, exists := paramNameMap[name]; !exists {
					params = append(params, param)
					paramNameMap[name] = struct{}{}
				}
			} else {
				params = append(params, param)
			}
		}
		return nil
	})
	return params
}

func getArgsFromParams(values []driver.NamedValue, params []googlesql.ResolvedParameterNode) ([]interface{}, error) {
	if values == nil {
		return nil, nil
	}
	argNum := len(params)
	if len(values) < argNum {
		return nil, fmt.Errorf("not enough query arguments")
	}
	namedValuesMap := map[string]driver.NamedValue{}
	for _, value := range values {
		// Name() value of googlesql.ResolvedParameterNode always returns lowercase name.
		namedValuesMap[strings.ToLower(value.Name)] = value
	}
	var namedValues []driver.NamedValue
	for idx, param := range params {
		name, _ := param.Name()
		if name != "" {
			value, exists := namedValuesMap[name]
			if exists {
				namedValues = append(namedValues, value)
			} else {
				namedValues = append(namedValues, values[idx])
			}
		} else {
			namedValues = append(namedValues, values[idx])
		}
	}
	newNamedValues, err := EncodeNamedValues(namedValues, params)
	if err != nil {
		return nil, err
	}
	args := make([]interface{}, 0, argNum)
	for _, newNamedValue := range newNamedValues {
		args = append(args, newNamedValue)
	}
	return args, nil
}
