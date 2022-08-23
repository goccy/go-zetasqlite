package internal

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

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
		zetasql.FeatureV13AllowDashesInTableName,
	})
	langOpt.SetSupportedStatementKinds([]ast.Kind{
		ast.BeginStmt,
		ast.CommitStmt,
		ast.MergeStmt,
		ast.QueryStmt,
		ast.InsertStmt,
		ast.UpdateStmt,
		ast.DeleteStmt,
		ast.DropStmt,
		ast.TruncateStmt,
		ast.CreateTableStmt,
		ast.CreateTableAsSelectStmt,
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

func (a *Analyzer) parseScript(query string) ([]parsed_ast.StatementNode, error) {
	loc := zetasql.NewParseResumeLocation(query)
	var stmts []parsed_ast.StatementNode
	for {
		stmt, isEnd, err := zetasql.ParseNextScriptStatement(loc, a.opt.ParserOptions())
		if err != nil {
			return nil, fmt.Errorf("failed to parse statement: %w", err)
		}
		switch s := stmt.(type) {
		case *parsed_ast.BeginEndBlockNode:
			stmts = append(stmts, s.StatementList()...)
		default:
			stmts = append(stmts, s)
		}
		if isEnd {
			break
		}
	}
	return stmts, nil
}

func (a *Analyzer) getFullNamePathMap(stmts []parsed_ast.StatementNode) (map[string][]string, error) {
	fullNamePathMap := map[string][]string{}
	for _, stmt := range stmts {
		parsed_ast.Walk(stmt, func(node parsed_ast.Node) error {
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
	}
	return fullNamePathMap, nil
}

func (a *Analyzer) AnalyzeIterator(ctx context.Context, conn *Conn, query string, args []driver.NamedValue) (*AnalyzerOutputIterator, error) {
	if err := a.catalog.Sync(ctx, conn); err != nil {
		return nil, fmt.Errorf("failed to sync catalog: %w", err)
	}
	stmts, err := a.parseScript(query)
	if err != nil {
		return nil, err
	}
	fullNamePathMap, err := a.getFullNamePathMap(stmts)
	if err != nil {
		return nil, fmt.Errorf("failed to get full name path map %s: %w", query, err)
	}
	funcMap := map[string]*FunctionSpec{}
	for _, spec := range a.catalog.getFunctions(a.namePath) {
		funcMap[spec.FuncName()] = spec
	}
	return &AnalyzerOutputIterator{
		query:           query,
		args:            args,
		stmts:           stmts,
		analyzer:        a,
		funcMap:         funcMap,
		fullNamePathMap: fullNamePathMap,
	}, nil
}

type AnalyzerOutputIterator struct {
	query           string
	args            []driver.NamedValue
	analyzer        *Analyzer
	stmts           []parsed_ast.StatementNode
	stmtIdx         int
	funcMap         map[string]*FunctionSpec
	fullNamePathMap map[string][]string
	out             *zetasql.AnalyzerOutput
	isEnd           bool
	err             error
}

func (it *AnalyzerOutputIterator) Next() bool {
	if it.stmtIdx >= len(it.stmts) {
		return false
	}
	out, err := zetasql.AnalyzeStatementFromParserAST(
		it.query,
		it.stmts[it.stmtIdx],
		it.analyzer.catalog.getCatalog(it.analyzer.namePath),
		it.analyzer.opt,
	)
	it.err = err
	it.out = out
	if it.err != nil {
		return false
	}
	it.stmtIdx++
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
		return it.analyzeCreateTableStmt(ctx, stmtNode.(*ast.CreateTableStmtNode))
	case ast.CreateTableAsSelectStmt:
		return it.analyzeCreateTableAsSelectStmt(ctx, stmtNode.(*ast.CreateTableAsSelectStmtNode))
	case ast.CreateFunctionStmt:
		return it.analyzeCreateFunctionStmt(ctx, stmtNode.(*ast.CreateFunctionStmtNode))
	case ast.DropStmt:
		return it.analyzeDropStmt(ctx, stmtNode.(*ast.DropStmtNode))
	case ast.InsertStmt, ast.UpdateStmt, ast.DeleteStmt:
		return it.analyzeDMLStmt(ctx, stmtNode)
	case ast.TruncateStmt:
		return it.analyzeTruncateStmt(ctx, stmtNode.(*ast.TruncateStmtNode))
	case ast.MergeStmt:
		ctx = withUseColumnID(ctx)
		return it.analyzeMergeStmt(ctx, stmtNode.(*ast.MergeStmtNode))
	case ast.QueryStmt:
		ctx = withUseColumnID(ctx)
		return it.analyzeQueryStmt(ctx, stmtNode.(*ast.QueryStmtNode))
	case ast.BeginStmt:
		return it.analyzeBeginStmt(ctx, stmtNode)
	case ast.CommitStmt:
		return it.analyzeCommitStmt(ctx, stmtNode)
	}
	return nil, fmt.Errorf("unsupported stmt %s", stmtNode.DebugString())
}

func (it *AnalyzerOutputIterator) analyzeCreateTableStmt(ctx context.Context, node *ast.CreateTableStmtNode) (*AnalyzerOutput, error) {
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
			dropTableQuery := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", spec.TableName())
			if spec.CreateMode == ast.CreateOrReplaceMode {
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
			if spec.IsTemp {
				stmt, err := zetasql.ParseStatement(dropTableQuery, nil)
				if err != nil {
					return nil, err
				}
				it.stmts = append(it.stmts, stmt)
			}
			return nil, nil
		},
	}, nil
}

func (it *AnalyzerOutputIterator) analyzeCreateTableAsSelectStmt(ctx context.Context, node *ast.CreateTableAsSelectStmtNode) (*AnalyzerOutput, error) {
	query, err := newNode(node.Query()).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	spec := newTableAsSelectSpec(it.analyzer.namePath, query, node)
	params := it.getParamsFromNode(node)
	args, err := it.getArgsFromParams(params)
	if err != nil {
		return nil, err
	}
	return &AnalyzerOutput{
		node:   node,
		query:  it.query,
		params: params,
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
			dropTableQuery := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", spec.TableName())
			if spec.CreateMode == ast.CreateOrReplaceMode {
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
			if spec.IsTemp {
				stmt, err := zetasql.ParseStatement(dropTableQuery, nil)
				if err != nil {
					return nil, err
				}
				it.stmts = append(it.stmts, stmt)
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

func (it *AnalyzerOutputIterator) analyzeDropStmt(ctx context.Context, node *ast.DropStmtNode) (*AnalyzerOutput, error) {
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
	objectType := node.ObjectType()
	name := FormatName(MergeNamePath(it.analyzer.namePath, node.NamePath()))
	return &AnalyzerOutput{
		node:           node,
		query:          it.query,
		formattedQuery: formattedQuery,
		params:         params,
		Prepare: func(ctx context.Context, conn *Conn) (driver.Stmt, error) {
			return nil, fmt.Errorf("currently unsupported prepared statement for DROP statment")
		},
		ExecContext: func(ctx context.Context, conn *Conn) (driver.Result, error) {
			switch objectType {
			case "TABLE":
				if _, err := conn.ExecContext(ctx, formattedQuery, args...); err != nil {
					return nil, fmt.Errorf("failed to exec %s: %w", formattedQuery, err)
				}
				if err := it.analyzer.catalog.DeleteTableSpec(ctx, conn, name); err != nil {
					return nil, fmt.Errorf("failed to delete table spec: %w", err)
				}
			case "FUNCTION":
				if err := it.analyzer.catalog.DeleteFunctionSpec(ctx, conn, name); err != nil {
					return nil, fmt.Errorf("failed to delete function spec: %w", err)
				}
				delete(it.funcMap, name)
			default:
				return nil, fmt.Errorf("currently unsupported DROP %s statement", objectType)
			}
			return nil, nil
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

func (it *AnalyzerOutputIterator) analyzeBeginStmt(ctx context.Context, node ast.Node) (*AnalyzerOutput, error) {
	return &AnalyzerOutput{
		node:           node,
		query:          it.query,
		formattedQuery: "",
		isQuery:        true,
		Prepare: func(ctx context.Context, conn *Conn) (driver.Stmt, error) {
			return nil, nil
		},
		QueryContext: func(ctx context.Context, conn *Conn) (driver.Rows, error) {
			return nil, nil
		},
		ExecContext: func(ctx context.Context, conn *Conn) (driver.Result, error) {
			return nil, nil
		},
	}, nil
}

func (it *AnalyzerOutputIterator) analyzeCommitStmt(ctx context.Context, node ast.Node) (*AnalyzerOutput, error) {
	return &AnalyzerOutput{
		node:           node,
		query:          it.query,
		formattedQuery: "",
		isQuery:        true,
		Prepare: func(ctx context.Context, conn *Conn) (driver.Stmt, error) {
			return nil, nil
		},
		QueryContext: func(ctx context.Context, conn *Conn) (driver.Rows, error) {
			return nil, nil
		},
		ExecContext: func(ctx context.Context, conn *Conn) (driver.Result, error) {
			return nil, nil
		},
	}, nil
}

func (it *AnalyzerOutputIterator) analyzeTruncateStmt(ctx context.Context, node *ast.TruncateStmtNode) (*AnalyzerOutput, error) {
	return &AnalyzerOutput{
		node:           node,
		query:          it.query,
		formattedQuery: "",
		isQuery:        true,
		Prepare: func(ctx context.Context, conn *Conn) (driver.Stmt, error) {
			return nil, nil
		},
		QueryContext: func(ctx context.Context, conn *Conn) (driver.Rows, error) {
			return nil, nil
		},
		ExecContext: func(ctx context.Context, conn *Conn) (driver.Result, error) {
			table := node.TableScan().Table().Name()
			query := fmt.Sprintf("DELETE FROM `%s`", table)
			if _, err := conn.ExecContext(ctx, query); err != nil {
				return nil, fmt.Errorf("failed to truncate %s: %w", query, err)
			}
			return nil, nil
		},
	}, nil
}

func (it *AnalyzerOutputIterator) analyzeMergeStmt(ctx context.Context, node *ast.MergeStmtNode) (*AnalyzerOutput, error) {
	targetTable, err := newNode(node.TableScan()).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	sourceTable, err := newNode(node.FromScan()).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	expr, err := newNode(node.MergeExpr()).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	fn, ok := node.MergeExpr().(*ast.FunctionCallNode)
	if !ok {
		return nil, fmt.Errorf("currently MERGE expression is supported equal expression only")
	}
	if fn.Function().FullName(false) != "$equal" {
		return nil, fmt.Errorf("currently MERGE expression is supported equal expression only")
	}
	args := fn.ArgumentList()
	if len(args) != 2 {
		return nil, fmt.Errorf("unexpected MERGE expression column num. expected 2 column but specified %d column", len(args))
	}
	colA, ok := args[0].(*ast.ColumnRefNode)
	if !ok {
		return nil, fmt.Errorf("unexpected MERGE expression. expected column reference but got %T", args[0])
	}
	colB, ok := args[1].(*ast.ColumnRefNode)
	if !ok {
		return nil, fmt.Errorf("unexpected MERGE expression. expected column reference but got %T", args[1])
	}
	var (
		sourceColumn *ast.Column
		targetColumn *ast.Column
	)
	if strings.Contains(sourceTable, colA.Column().TableName()) {
		sourceColumn = colA.Column()
		targetColumn = colB.Column()
	} else {
		sourceColumn = colB.Column()
		targetColumn = colA.Column()
	}
	mergedTableSourceColumnName := fmt.Sprintf("`%s`", string(uniqueColumnName(ctx, sourceColumn)))
	mergedTableTargetColumnName := fmt.Sprintf("`%s`", string(uniqueColumnName(ctx, targetColumn)))
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
		targetColumn.Name(),
		mergedTableSourceColumnName,
		mergedTableTargetColumnName,
	)

	// exists target table but not exists source table
	notMatchedBySourceFromStmt := fmt.Sprintf(
		"FROM zetasqlite_merged_table WHERE %[2]s = `%[1]s` AND %[3]s IS NULL",
		targetColumn.Name(),
		mergedTableTargetColumnName,
		mergedTableSourceColumnName,
	)

	// exists source table but not exists target table
	notMatchedByTargetFromStmt := fmt.Sprintf(
		"FROM zetasqlite_merged_table WHERE %[2]s = `%[1]s` AND %[3]s IS NULL",
		sourceColumn.Name(),
		mergedTableSourceColumnName,
		mergedTableTargetColumnName,
	)
	for _, when := range node.WhenClauseList() {
		var fromStmt string
		switch when.MatchType() {
		case ast.MatchTypeMatched:
			fromStmt = matchedFromStmt
		case ast.MatchTypeNotMatchedBySource:
			fromStmt = notMatchedBySourceFromStmt
		case ast.MatchTypeNotMatchedByTarget:
			fromStmt = notMatchedByTargetFromStmt
		}
		whereStmt := fmt.Sprintf(
			"WHERE EXISTS(SELECT %s %s)",
			strings.Join(mergedTableOutputColumns, ","),
			fromStmt,
		)
		switch when.ActionType() {
		case ast.ActionTypeInsert:
			var columns []string
			for _, col := range when.InsertColumnList() {
				columns = append(columns, fmt.Sprintf("`%s`", col.Name()))
			}
			row, err := newNode(when.InsertRow()).FormatSQL(unuseColumnID(ctx))
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, fmt.Sprintf(
				"INSERT INTO `%[1]s`(%[2]s) SELECT %[3]s FROM (SELECT * FROM `%[4]s` %[5]s)",
				targetColumn.TableName(),
				strings.Join(columns, ","),
				row,
				sourceColumn.TableName(),
				whereStmt,
			))
		case ast.ActionTypeUpdate:
			var items []string
			for _, item := range when.UpdateItemList() {
				sql, err := newNode(item).FormatSQL(ctx)
				if err != nil {
					return nil, err
				}
				items = append(items, sql)
			}
			stmts = append(stmts, fmt.Sprintf(
				"UPDATE `%s` SET %s %s",
				targetColumn.TableName(),
				strings.Join(items, ","),
				fromStmt,
			))
		case ast.ActionTypeDelete:
			stmts = append(stmts, fmt.Sprintf(
				"DELETE FROM `%s` %s",
				targetColumn.TableName(),
				whereStmt,
			))
		}
	}
	stmts = append(stmts, "DROP TABLE zetasqlite_merged_table")
	return &AnalyzerOutput{
		node:           node,
		query:          it.query,
		formattedQuery: "",
		isQuery:        true,
		Prepare: func(ctx context.Context, conn *Conn) (driver.Stmt, error) {
			return nil, nil
		},
		QueryContext: func(ctx context.Context, conn *Conn) (driver.Rows, error) {
			return nil, nil
		},
		ExecContext: func(ctx context.Context, conn *Conn) (driver.Result, error) {
			for _, stmt := range stmts {
				if _, err := conn.ExecContext(ctx, stmt); err != nil {
					return nil, fmt.Errorf("failed to exec merge statement %s: %w", stmt, err)
				}
			}
			return nil, nil
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
	if len(it.args) < argNum {
		return nil, fmt.Errorf("not enough query arguments")
	}
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
