package internal

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/goccy/go-zetasql"
	parsed_ast "github.com/goccy/go-zetasql/ast"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
)

type Analyzer struct {
	namePath []string
	catalog  *Catalog
	opt      *zetasql.AnalyzerOptions
}

type AnalyzerOutput struct {
	node           ast.Node
	query          string
	formattedQuery string
	params         []*ast.ParameterNode
	isQuery        bool
	tableSpec      *TableSpec
	outputColumns  []*ColumnSpec
	Prepare        func(context.Context, *Conn) (driver.Stmt, error)
	ExecContext    func(context.Context, *Conn) (driver.Result, error)
	QueryContext   func(context.Context, *Conn) (driver.Rows, error)
}

func (o *AnalyzerOutput) Params() []*ast.ParameterNode {
	return o.params
}

func NewAnalyzer(catalog *Catalog) *Analyzer {
	return &Analyzer{
		catalog: catalog,
		opt:     newAnalyzerOptions(),
	}
}

func newAnalyzerOptions() *zetasql.AnalyzerOptions {
	langOpt := zetasql.NewLanguageOptions()
	langOpt.SetNameResolutionMode(zetasql.NameResolutionDefault)
	langOpt.SetProductMode(types.ProductInternal)
	langOpt.SetEnabledLanguageFeatures([]zetasql.LanguageFeature{
		zetasql.FeatureAnalyticFunctions,
		zetasql.FeatureNamedArguments,
		zetasql.FeatureNumericType,
		zetasql.FeatureCreateTableNotNull,
		zetasql.FeatureParameterizedTypes,
		zetasql.FeatureTablesample,
		zetasql.FeatureTimestampNanos,
		zetasql.FeatureV11HavingInAggregate,
		zetasql.FeatureV11NullHandlingModifierInAggregate,
		zetasql.FeatureV11OrderByCollate,
		zetasql.FeatureV11SelectStarExceptReplace,
		zetasql.FeatureV12SafeFunctionCall,
		zetasql.FeatureJsonType,
		zetasql.FeatureJsonArrayFunctions,
		zetasql.FeatureJsonStrictNumberParsing,
		zetasql.FeatureV13IsDistinct,
		zetasql.FeatureV13FormatInCast,
		zetasql.FeatureV13DateArithmetics,
		zetasql.FeatureV11OrderByInAggregate,
		zetasql.FeatureV11LimitInAggregate,
		zetasql.FeatureV13DateTimeConstructors,
		zetasql.FeatureV13ExtendedDateTimeSignatures,
		zetasql.FeatureV12CivilTime,
		zetasql.FeatureV12WeekWithWeekday,
		zetasql.FeatureIntervalType,
		zetasql.FeatureGroupByRollup,
		zetasql.FeatureV13NullsFirstLastInOrderBy,
		zetasql.FeatureV13Qualify,
	})
	langOpt.SetSupportedStatementKinds([]ast.Kind{
		ast.QueryStmt,
		ast.InsertStmt,
		ast.UpdateStmt,
		ast.DeleteStmt,
		ast.CreateTableStmt,
		ast.CreateProcedureStmt,
		ast.CreateFunctionStmt,
		ast.CreateTableFunctionStmt,
	})
	opt := zetasql.NewAnalyzerOptions()
	opt.SetAllowUndeclaredParameters(true)
	opt.SetLanguage(langOpt)
	return opt
}

func (a *Analyzer) NamePath() []string {
	return a.namePath
}

func (a *Analyzer) SetNamePath(path []string) {
	a.namePath = path
}

func (a *Analyzer) AddNamePath(path string) {
	a.namePath = append(a.namePath, path)
}

func (a *Analyzer) SetParameterMode(mode zetasql.ParameterMode) {
	a.opt.SetParameterMode(mode)
}

func (a *Analyzer) getFullNamePathMap(query string) (map[string][]string, error) {
	fullNamePathMap := map[string][]string{}
	loc := zetasql.NewParseResumeLocation(query)
	for {
		parsedAST, isEnd, err := zetasql.ParseNextStatement(loc, a.opt.ParserOptions())
		if err != nil {
			return nil, fmt.Errorf("failed to parse statement: %w", err)
		}
		parsed_ast.Walk(parsedAST, func(node parsed_ast.Node) error {
			switch n := node.(type) {
			case *parsed_ast.FunctionCallNode:
				path := []string{}
				for _, name := range n.Function().Names() {
					path = append(path, name.Name())
				}
				if len(path) == 0 {
					return fmt.Errorf("failed to find name path from function call node")
				}
				base := path[len(path)-1]
				fullNamePathMap[base] = path
			case *parsed_ast.TablePathExpressionNode:
				switch {
				case n.PathExpr() != nil:
					path := []string{}
					for _, name := range n.PathExpr().Names() {
						path = append(path, name.Name())
					}
					if len(path) == 0 {
						return fmt.Errorf("failed to find name path from table path expression node")
					}
					base := path[len(path)-1]
					fullNamePathMap[base] = path
				}
			case *parsed_ast.InsertStatementNode:
				path := []string{}
				for _, name := range n.TargetPath().(*parsed_ast.PathExpressionNode).Names() {
					path = append(path, name.Name())
				}
				if len(path) == 0 {
					return fmt.Errorf("failed to find name path from insert statement node")
				}
				base := path[len(path)-1]
				fullNamePathMap[base] = path
			case *parsed_ast.UpdateStatementNode:
				path := []string{}
				for _, name := range n.TargetPath().(*parsed_ast.PathExpressionNode).Names() {
					path = append(path, name.Name())
				}
				if len(path) == 0 {
					return fmt.Errorf("failed to find name path from update statement node")
				}
				base := path[len(path)-1]
				fullNamePathMap[base] = path
			case *parsed_ast.DeleteStatementNode:
				path := []string{}
				for _, name := range n.TargetPath().(*parsed_ast.PathExpressionNode).Names() {
					path = append(path, name.Name())
				}
				if len(path) == 0 {
					return fmt.Errorf("failed to find name path from delete statement node")
				}
				base := path[len(path)-1]
				fullNamePathMap[base] = path
			}
			return nil
		})
		if isEnd {
			break
		}
	}
	return fullNamePathMap, nil
}

func (a *Analyzer) AnalyzeIterator(ctx context.Context, conn *Conn, query string, args []driver.NamedValue) (*AnalyzerOutputIterator, error) {
	if err := a.catalog.Sync(ctx, conn); err != nil {
		return nil, fmt.Errorf("failed to sync catalog: %w", err)
	}
	fullNamePathMap, err := a.getFullNamePathMap(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get full name path map %s: %w", query, err)
	}
	funcMap := map[string]*FunctionSpec{}
	for _, spec := range a.catalog.functions {
		funcMap[spec.FuncName()] = spec
	}
	return &AnalyzerOutputIterator{
		query:           query,
		args:            args,
		analyzer:        a,
		loc:             zetasql.NewParseResumeLocation(query),
		funcMap:         funcMap,
		fullNamePathMap: fullNamePathMap,
	}, nil
}

type AnalyzerOutputIterator struct {
	query           string
	args            []driver.NamedValue
	analyzer        *Analyzer
	loc             *zetasql.ParseResumeLocation
	funcMap         map[string]*FunctionSpec
	fullNamePathMap map[string][]string
	out             *zetasql.AnalyzerOutput
	isEnd           bool
	err             error
}

func (it *AnalyzerOutputIterator) Next() bool {
	if it.isEnd {
		return false
	}
	out, isEnd, err := zetasql.AnalyzeNextStatement(
		it.loc,
		it.analyzer.catalog.catalog,
		it.analyzer.opt,
	)
	it.err = err
	it.out = out
	it.isEnd = isEnd
	if it.err != nil {
		return false
	}
	return true
}

func (it *AnalyzerOutputIterator) Err() error {
	return it.err
}

func (it *AnalyzerOutputIterator) Analyze(ctx context.Context) (*AnalyzerOutput, error) {
	ctx = withNamePath(ctx, it.analyzer.namePath)
	ctx = withColumnRefMap(ctx, map[string]string{})
	ctx = withTableNameToColumnListMap(ctx, map[string][]*ast.Column{})
	ctx = withFullNamePathMap(ctx, it.fullNamePathMap)
	ctx = withFuncMap(ctx, it.funcMap)
	ctx = withAnalyticOrderColumnNames(ctx, &analyticOrderColumnNames{})
	stmtNode := it.out.Statement()
	switch stmtNode.Kind() {
	case ast.CreateTableStmt:
		return it.analyzeCreateTableStmt(stmtNode.(*ast.CreateTableStmtNode))
	case ast.CreateFunctionStmt:
		return it.analyzeCreateFunctionStmt(ctx, stmtNode.(*ast.CreateFunctionStmtNode))
	case ast.InsertStmt, ast.UpdateStmt, ast.DeleteStmt:
		return it.analyzeDMLStmt(ctx, stmtNode)
	case ast.QueryStmt:
		ctx = withUseColumnID(ctx)
		return it.analyzeQueryStmt(ctx, stmtNode.(*ast.QueryStmtNode))
	}
	return nil, fmt.Errorf("unsupported stmt %s", stmtNode.DebugString())
}

func (it *AnalyzerOutputIterator) analyzeCreateTableStmt(node *ast.CreateTableStmtNode) (*AnalyzerOutput, error) {
	spec := newTableSpec(it.analyzer.namePath, node)
	params := it.getParamsFromNode(node)
	args, err := it.getArgsFromParams(params)
	if err != nil {
		return nil, err
	}
	return &AnalyzerOutput{
		node:      node,
		query:     it.query,
		params:    params,
		tableSpec: spec,
		Prepare: func(ctx context.Context, conn *Conn) (driver.Stmt, error) {
			if spec.CreateMode == ast.CreateOrReplaceMode {
				query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", spec.TableName())
				if _, err := conn.ExecContext(ctx, query); err != nil {
					return nil, err
				}
			}
			s, err := conn.PrepareContext(ctx, spec.SQLiteSchema())
			if err != nil {
				return nil, fmt.Errorf("failed to prepare %s: %w", it.query, err)
			}
			return newCreateTableStmt(s, conn, it.analyzer.catalog, spec), nil
		},
		ExecContext: func(ctx context.Context, conn *Conn) (driver.Result, error) {
			if spec.CreateMode == ast.CreateOrReplaceMode {
				dropTableQuery := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", spec.TableName())
				if _, err := conn.ExecContext(ctx, dropTableQuery); err != nil {
					return nil, err
				}
			}
			if _, err := conn.ExecContext(ctx, spec.SQLiteSchema(), args...); err != nil {
				return nil, fmt.Errorf("failed to exec %s: %w", it.query, err)
			}
			if err := it.analyzer.catalog.AddNewTableSpec(ctx, conn, spec); err != nil {
				return nil, fmt.Errorf("failed to add new table spec: %w", err)
			}
			return nil, nil
		},
	}, nil
}

func (it *AnalyzerOutputIterator) analyzeCreateFunctionStmt(ctx context.Context, node *ast.CreateFunctionStmtNode) (*AnalyzerOutput, error) {
	spec, err := newFunctionSpec(ctx, it.analyzer.namePath, node)
	if err != nil {
		return nil, fmt.Errorf("failed to create function spec: %w", err)
	}
	return &AnalyzerOutput{
		query: it.query,
		node:  node,
		Prepare: func(ctx context.Context, conn *Conn) (driver.Stmt, error) {
			return newCreateFunctionStmt(conn, it.analyzer.catalog, spec), nil
		},
		ExecContext: func(ctx context.Context, conn *Conn) (driver.Result, error) {
			if err := it.analyzer.catalog.AddNewFunctionSpec(ctx, conn, spec); err != nil {
				return nil, fmt.Errorf("failed to add new function spec: %w", err)
			}
			it.funcMap[spec.FuncName()] = spec
			return nil, nil
		},
		QueryContext: func(ctx context.Context, conn *Conn) (driver.Rows, error) {
			if err := it.analyzer.catalog.AddNewFunctionSpec(ctx, conn, spec); err != nil {
				return nil, fmt.Errorf("failed to add new function spec: %w", err)
			}
			it.funcMap[spec.FuncName()] = spec
			return &Rows{}, nil
		},
	}, nil
}

func (it *AnalyzerOutputIterator) analyzeDMLStmt(ctx context.Context, node ast.Node) (*AnalyzerOutput, error) {
	formattedQuery, err := newNode(node).FormatSQL(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to format query %s: %w", it.query, err)
	}
	if formattedQuery == "" {
		return nil, fmt.Errorf("failed to format query %s", it.query)
	}
	params := it.getParamsFromNode(node)
	args, err := it.getArgsFromParams(params)
	if err != nil {
		return nil, err
	}
	return &AnalyzerOutput{
		node:           node,
		query:          it.query,
		formattedQuery: formattedQuery,
		params:         params,
		Prepare: func(ctx context.Context, conn *Conn) (driver.Stmt, error) {
			s, err := conn.PrepareContext(ctx, formattedQuery)
			if err != nil {
				return nil, fmt.Errorf("failed to prepare %s: %w", it.query, err)
			}
			return newDMLStmt(s, params, formattedQuery), nil
		},
		ExecContext: func(ctx context.Context, conn *Conn) (driver.Result, error) {
			if _, err := conn.ExecContext(ctx, formattedQuery, args...); err != nil {
				return nil, fmt.Errorf("failed to exec %s: %w", formattedQuery, err)
			}
			return nil, nil
		},
	}, nil
}

func (it *AnalyzerOutputIterator) analyzeQueryStmt(ctx context.Context, node *ast.QueryStmtNode) (*AnalyzerOutput, error) {
	outputColumns := []*ColumnSpec{}
	for _, col := range node.OutputColumnList() {
		outputColumns = append(outputColumns, &ColumnSpec{
			Name: col.Name(),
			Type: newType(col.Column().Type()),
		})
	}
	formattedQuery, err := newNode(node).FormatSQL(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to format query %s: %w", it.query, err)
	}
	if formattedQuery == "" {
		return nil, fmt.Errorf("failed to format query %s", it.query)
	}
	params := it.getParamsFromNode(node)
	args, err := it.getArgsFromParams(params)
	if err != nil {
		return nil, err
	}
	return &AnalyzerOutput{
		node:           node,
		query:          it.query,
		formattedQuery: formattedQuery,
		params:         params,
		isQuery:        true,
		Prepare: func(ctx context.Context, conn *Conn) (driver.Stmt, error) {
			s, err := conn.PrepareContext(ctx, formattedQuery)
			if err != nil {
				return nil, fmt.Errorf("failed to prepare %s: %w", it.query, err)
			}
			return newQueryStmt(s, params, formattedQuery, outputColumns), nil
		},
		QueryContext: func(ctx context.Context, conn *Conn) (driver.Rows, error) {
			rows, err := conn.QueryContext(ctx, formattedQuery, args...)
			if err != nil {
				return nil, fmt.Errorf("failed to query %s: %w", formattedQuery, err)
			}
			return &Rows{rows: rows, columns: outputColumns}, nil
		},
	}, nil
}

func (it *AnalyzerOutputIterator) getParamsFromNode(node ast.Node) []*ast.ParameterNode {
	var params []*ast.ParameterNode
	ast.Walk(node, func(n ast.Node) error {
		param, ok := n.(*ast.ParameterNode)
		if ok {
			params = append(params, param)
		}
		return nil
	})
	return params
}

func (it *AnalyzerOutputIterator) getArgsFromParams(params []*ast.ParameterNode) ([]interface{}, error) {
	argNum := len(params)
	newNamedValues, err := EncodeNamedValues(it.args[:argNum], params)
	if err != nil {
		return nil, err
	}
	it.args = it.args[:argNum]
	args := make([]interface{}, 0, argNum)
	for _, newNamedValue := range newNamedValues {
		args = append(args, newNamedValue)
	}
	return args, nil
}
