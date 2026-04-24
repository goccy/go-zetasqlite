package internal

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
)

type Analyzer struct {
	namePath        *NamePath
	isAutoIndexMode bool
	isExplainMode   bool
	catalog         *Catalog
	opt             *googlesql.AnalyzerOptions
	parserOpts      *googlesql.ParserOptions
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
	if langOpt == nil {
		return nil, fmt.Errorf("failed to create LanguageOptions (wasm error)")
	}
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
	if opt == nil {
		return nil, fmt.Errorf("failed to create AnalyzerOptions (wasm error)")
	}
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

// parsedScript bundles parsed statements with the ParseResumeLocation and
// ParserOutputs they were produced from. The wasmify generator does not yet
// propagate parent-child ownership to the Go wrappers (child handles don't
// retain their owning parent), so callers must hold on to the parent handles
// for as long as they keep using any AST nodes — otherwise a Go GC cycle can
// free the parent's wasm-side tree while the children are still in use.
type parsedScript struct {
	loc     *googlesql.ParseResumeLocation
	outputs []*googlesql.ParserOutput
	stmts   []googlesql.ASTStatementNode
}

// Close releases the wasm-side C++ objects owned by this parsed script. It
// is idempotent — double-close is a no-op because the generated Close()
// methods guard on a zeroed ptr.
func (p *parsedScript) Close() {
	if p == nil {
		return
	}
	for _, out := range p.outputs {
		if out != nil {
			out.Close()
		}
	}
	p.outputs = nil
	if p.loc != nil {
		p.loc.Close()
		p.loc = nil
	}
	p.stmts = nil
}

func (a *Analyzer) parseScript(query string) (*parsedScript, error) {
	loc, locErr := googlesql.NewParseResumeLocationFromString(query)
	if loc == nil {
		return nil, fmt.Errorf("failed to create parse resume location for %q: %w", query, locErr)
	}
	// Cache the ParserOptions on the Analyzer. GetParserOptions returns a
	// fresh handle each time (the C++ side builds a copy) and, without
	// caching, every query adds one more ParserOptions to the wasm heap
	// until the module exhausts its 4 GiB linear memory.
	if a.parserOpts == nil {
		parserOpts, err := a.opt.GetParserOptions()
		if err != nil {
			return nil, fmt.Errorf("failed to get parser options: %w", err)
		}
		a.parserOpts = parserOpts
	}
	result := &parsedScript{loc: loc}
	input, _ := loc.Input()
	inputLen := int32(len(input))
	for {
		out, err := googlesql.ParseNextScriptStatement(loc, a.parserOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to parse statement: %w", err)
		}
		if out == nil {
			break
		}
		result.outputs = append(result.outputs, out)
		stmt, err := out.Statement()
		if err != nil {
			return nil, fmt.Errorf("failed to get statement from parser output: %w", err)
		}
		// ASTBeginEndBlock has a StatementListNode child — expand it.
		if block, ok := stmt.(*googlesql.ASTBeginEndBlock); ok {
			list, err := block.StatementListNode()
			if err == nil && list != nil {
				result.stmts = append(result.stmts, collectStatementsFromList(list)...)
			}
		} else {
			result.stmts = append(result.stmts, stmt)
		}
		pos, _ := loc.BytePosition()
		if pos >= inputLen {
			break
		}
		// Skip any trailing whitespace / semicolon noise that follows
		// the last statement. If only whitespace remains we're done.
		if strings.TrimSpace(input[pos:]) == "" {
			break
		}
	}
	return result, nil
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

// validateLiteralCasts walks the AST looking for CAST(<string-literal>
// AS <numeric-type>) expressions where the literal would fail at
// runtime. googlesql's analyzer currently does not fold such casts at
// analysis time; rejecting them here produces the
// "Could not cast literal" message users expect, and matches the
// historical zetasql behavior.
func validateLiteralCasts(stmt googlesql.ASTStatementNode, query string) error {
	var firstErr error
	_ = ASTWalk(stmt, func(node googlesql.ASTNodeNode) error {
		if firstErr != nil {
			return nil
		}
		cast, ok := node.(googlesql.ASTCastExpressionNode)
		if !ok {
			return nil
		}
		exprNode, _ := cast.Expr()
		strLit, ok := exprNode.(googlesql.ASTStringLiteralNode)
		if !ok {
			return nil
		}
		strVal, _ := strLit.StringValue()
		typeNode, _ := cast.Type()
		simple, ok := typeNode.(googlesql.ASTSimpleTypeNode)
		if !ok {
			return nil
		}
		path, _ := simple.TypeName()
		ids, _ := path.ToIdentifierVector()
		if len(ids) != 1 {
			return nil
		}
		typeName := strings.ToUpper(ids[0])
		switch typeName {
		case "INT32", "INT64", "UINT32", "UINT64":
			// Mirror StringValue.ToInt64: base 0 for hex/0x-prefixed.
			base := 10
			if strings.Contains(strings.ToLower(strVal), "0x") {
				base = 0
			}
			if _, perr := strconv.ParseInt(strVal, base, 64); perr == nil {
				return nil
			}
			if _, perr := strconv.ParseInt(strVal, 0, 64); perr == nil {
				return nil
			}
		case "FLOAT", "FLOAT64", "DOUBLE":
			if _, perr := strconv.ParseFloat(strVal, 64); perr == nil {
				return nil
			}
		default:
			return nil
		}
		if m1(cast.IsSafeCast()) {
			return nil
		}
		line, col := parseLocationLineCol(cast, query)
		firstErr = fmt.Errorf(
			"failed to analyze: INVALID_ARGUMENT: Could not cast literal %q to type %s [at %d:%d]",
			strVal, typeName, line, col,
		)
		return nil
	})
	return firstErr
}

// parseLocationLineCol converts the byte offset of an AST node's start
// location into a 1-indexed (line, column) pair relative to the query
// string. Falls back to (1, 1) if the location isn't available.
func parseLocationLineCol(node googlesql.ASTNodeNode, query string) (int, int) {
	sp, err := node.StartLocation()
	if err != nil || sp == nil {
		return 1, 1
	}
	off, err := sp.GetByteOffset()
	if err != nil {
		return 1, 1
	}
	line, col := 1, 1
	for i := int32(0); i < off && int(i) < len(query); i++ {
		if query[i] == '\n' {
			line++
			col = 1
		} else {
			col++
		}
	}
	return line, col
}

// preRegisterWildcardTables walks the AST looking for table references
// whose last path segment ends with `*` (e.g. `project.dataset.table_*`).
// For each such reference, build the corresponding wildcard table and
// add it to the wasm-side catalog so the analyzer resolves the
// reference. The googlesql SimpleCatalog has no native wildcard
// support, so we pre-register an equivalent SimpleTable here.
func (a *Analyzer) preRegisterWildcardTables(stmt googlesql.ASTStatementNode) {
	_ = ASTWalk(stmt, func(node googlesql.ASTNodeNode) error {
		tpath, ok := node.(googlesql.ASTTablePathExpressionNode)
		if !ok {
			return nil
		}
		pe, _ := tpath.PathExpr()
		if pe == nil {
			return nil
		}
		ids, _ := pe.ToIdentifierVector()
		if len(ids) == 0 {
			return nil
		}
		last := ids[len(ids)-1]
		if last == "" || last[len(last)-1] != '*' {
			return nil
		}
		a.catalog.registerWildcardTableByPath(ids)
		return nil
	})
}

// countPositionalParams walks the AST counting `?` occurrences. Used to
// split positional arguments across statements in a multi-statement
// script, where the resolved-tree walker's limited descent otherwise
// mis-reports per-statement parameter counts.
func countPositionalParams(stmt googlesql.ASTStatementNode) int {
	var n int
	_ = ASTWalk(stmt, func(node googlesql.ASTNodeNode) error {
		if p, ok := node.(googlesql.ASTParameterExprNode); ok {
			if m1(p.Position()) > 0 {
				n++
			}
		}
		return nil
	})
	return n
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
	parsed, err := a.parseScript(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse statements: %w", err)
	}
	funcMap := map[string]*FunctionSpec{}
	for _, spec := range a.catalog.getFunctions(a.namePath) {
		funcMap[spec.FuncName()] = spec
	}
	actionFuncs := make([]StmtActionFunc, 0, len(parsed.stmts))
	for idx, stmt := range parsed.stmts {
		stmt := stmt
		isLast := idx == len(parsed.stmts)-1
		actionFuncs = append(actionFuncs, func() (StmtAction, error) {
			// Keep the ParseResumeLocation / ParserOutputs alive so the
			// AST tree stmt points into is not freed out from under us
			// while this action runs. See parsedScript for context.
			defer runtime.KeepAlive(parsed)
			mode, err := a.getParameterMode(stmt)
			if err != nil {
				return nil, err
			}
			if err := validateLiteralCasts(stmt, query); err != nil {
				return nil, err
			}
			a.preRegisterWildcardTables(stmt)
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
				// Count positional parameters directly from the AST;
				// action.Args() reflects a later (possibly all-args)
				// fallback when the resolved-tree walker can't descend.
				consumed := countPositionalParams(stmt)
				if consumed > len(args) {
					consumed = len(args)
				}
				args = args[consumed:]
			}
			// Wrap with closingStmtAction so that Cleanup also releases
			// the AnalyzerOutput (which owns the huge resolved tree) and
			// — on the last stmt — the ParseResumeLocation + ParserOutputs.
			// Without this, running many queries against a single analyzer
			// accumulates wasm heap until wasm_alloc traps with "out of
			// bounds memory access". Finalizers are too late.
			wrapped := &closingStmtAction{StmtAction: action, analyzerOutput: out}
			if isLast {
				wrapped.parsed = parsed
			}
			return wrapped, nil
		})
	}
	if len(actionFuncs) == 0 {
		// No actions produced — still release the handles immediately.
		parsed.Close()
	}
	return actionFuncs, nil
}

// closingStmtAction wraps a StmtAction so that Cleanup also releases the
// wasm-side C++ handles owned by this statement (AnalyzerOutput; and the
// parsed-script handles on the last action of an Analyze batch).
type closingStmtAction struct {
	StmtAction
	analyzerOutput *googlesql.AnalyzerOutput
	parsed         *parsedScript // non-nil only for the last action
}

func (c *closingStmtAction) Cleanup(ctx context.Context, conn *Conn) error {
	err := c.StmtAction.Cleanup(ctx, conn)
	if c.analyzerOutput != nil {
		c.analyzerOutput.Close()
		c.analyzerOutput = nil
	}
	if c.parsed != nil {
		c.parsed.Close()
		c.parsed = nil
	}
	return err
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
	kind, _ := node.NodeKind()
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
	dbg, _ := node.DebugString()
	return nil, fmt.Errorf("unsupported stmt %s", dbg)
}

func (a *Analyzer) newCreateTableStmtAction(_ context.Context, query string, args []driver.NamedValue, node googlesql.ResolvedCreateTableStmtNode) (*CreateTableStmtAction, error) {
	spec := newTableSpecWithQuery(a.namePath, query, node)
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
	if a.resultTypeIsTemplatedType(m1(node.Signature())) {
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
	rt := m1(sig.ResultType())
	if rt == nil {
		return false
	}
	return m1(rt.IsTemplated())
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
	for _, arg := range compatSignatureArguments(signature) {
		typ := realType
		if !m1(arg.IsTemplated()) {
			typ = newType(m1(arg.Type())).FormatType()
		}
		args = append(args, fmt.Sprintf("%s %s", m1(arg.ArgumentName()), typ))
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
	for _, arg := range compatSignatureArguments(signature) {
		typ := fmt.Sprintf("ARRAY<%s>", realType)
		if !m1(arg.IsTemplated()) {
			typ = newType(m1(arg.Type())).FormatType()
		}
		args = append(args, fmt.Sprintf("%s %s", m1(arg.ArgumentName()), typ))
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
	name := a.namePath.format2(node.NamePath())
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
	name := a.namePath.format2(node.NamePath())
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
	// For INSERT statements, reshape struct-valued args against the
	// target InsertColumnList types so sparse Go maps expand to match
	// the declared STRUCT field order. See reshapeInsertArgs.
	args = reshapeInsertArgs(args, node)
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

// reshapeInsertArgs adjusts args for ResolvedInsertStmt so struct-typed
// columns receive fully-populated struct values (missing fields filled
// with nil, fields in declaration order). Go callers sometimes pass
// sparse maps that don't enumerate every declared field; without this,
// downstream STRUCT_FIELD(value, index) reads land out-of-range.
func reshapeInsertArgs(args []driver.NamedValue, node googlesql.ResolvedNodeNode) []driver.NamedValue {
	if len(args) == 0 {
		return args
	}
	insert, ok := node.(googlesql.ResolvedInsertStmtNode)
	if !ok {
		return args
	}
	cols, err := insert.InsertColumnList()
	if err != nil || len(cols) == 0 {
		return args
	}
	// Align args to columns positionally. Fewer args than columns is
	// fine (extra columns may receive defaults); extra args are passed
	// through unchanged (they may belong to WHERE clauses).
	out := make([]driver.NamedValue, len(args))
	copy(out, args)
	n := len(cols)
	if n > len(out) {
		n = len(out)
	}
	for i := 0; i < n; i++ {
		colType, err := cols[i].Type()
		if err != nil || colType == nil {
			continue
		}
		out[i].Value = reshapeArgToType(out[i].Value, colType)
	}
	return out
}

// reshapeArgToType takes a single arg value (possibly a
// zetasqlite-encoded base64 string or a raw Go value) and a declared
// googlesql type, and returns a value whose underlying structure
// matches the declared type. Only STRUCT reshape is interesting here;
// other shapes pass through unchanged.
func reshapeArgToType(v interface{}, t googlesql.Googlesql_TypeNode) interface{} {
	if v == nil || t == nil {
		return v
	}
	kind, _ := t.Kind()
	if kind != googlesql.TypeKindTypeStruct {
		return v
	}
	val, err := DecodeValue(v)
	if err != nil || val == nil {
		return v
	}
	reshaped, err := CastValue(t, val)
	if err != nil || reshaped == nil {
		return v
	}
	out, err := EncodeValue(reshaped)
	if err != nil {
		return v
	}
	return out
}

func (a *Analyzer) newQueryStmtAction(ctx context.Context, query string, args []driver.NamedValue, node googlesql.ResolvedQueryStmtNode) (*QueryStmtAction, error) {
	outputColumns := []*ColumnSpec{}
	for _, col := range m1(node.OutputColumnList()) {
		outputColumns = append(outputColumns, &ColumnSpec{
			Name: m1(col.Name()),
			Type: newType(m1(m1(col.Column()).Type())),
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
	table, _ := m1(m1(node.TableScan()).TableMethod()).Name()
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
	if m1(m1(fn.FunctionMethod()).FullName(false)) != "$equal" {
		return nil, fmt.Errorf("currently MERGE expression is supported equal expression only")
	}
	argList := m1(fn.ArgumentList())
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
	if strings.Contains(sourceTable, m1(m1(colA.Column()).TableName())) {
		sourceColumn = m1(colA.Column())
		targetColumn = m1(colB.Column())
	} else {
		sourceColumn = m1(colB.Column())
		targetColumn = m1(colA.Column())
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
		switch ResolvedMergeWhenMatchType(when) {
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
		switch ResolvedMergeWhenActionType(when) {
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
	// The resolved-tree walker is a placeholder while ResolvedNode
	// child iteration isn't yet bridged, so params may be empty even
	// when the user supplied real parameters. In that case pass the
	// named values straight through and let SQLite handle native
	// @name / ? binding.
	if len(params) == 0 && len(values) > 0 {
		out := make([]interface{}, 0, len(values))
		for _, v := range values {
			if v.Name != "" {
				out = append(out, sql.Named(v.Name, v.Value))
			} else {
				out = append(out, v.Value)
			}
		}
		return out, nil
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
