package internal

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	googlesql "github.com/goccy/go-googlesql"
)

var (
	createCatalogTableQuery = `
CREATE TABLE IF NOT EXISTS zetasqlite_catalog(
  name STRING NOT NULL PRIMARY KEY,
  kind STRING NOT NULL,
  spec STRING NOT NULL,
  updatedAt TIMESTAMP NOT NULL,
  createdAt TIMESTAMP NOT NULL
)
`
	upsertCatalogQuery = `
INSERT INTO zetasqlite_catalog (
  name,
  kind,
  spec,
  updatedAt,
  createdAt
) VALUES (
  @name,
  @kind,
  @spec,
  @updatedAt,
  @createdAt
) ON CONFLICT(name) DO UPDATE SET
  spec = @spec,
  updatedAt = @updatedAt
`
	deleteCatalogQuery = `
DELETE FROM zetasqlite_catalog WHERE name = @name
`
)

type CatalogSpecKind string

const (
	TableSpecKind    CatalogSpecKind = "table"
	ViewSpecKind     CatalogSpecKind = "view"
	FunctionSpecKind CatalogSpecKind = "function"
	catalogName                      = "zetasqlite"
)

type Catalog struct {
	db           *sql.DB
	lastSyncedAt time.Time
	mu           sync.Mutex
	tables       []*TableSpec
	functions    []*FunctionSpec
	catalog      *googlesql.SimpleCatalog
	tableMap     map[string]*TableSpec
	funcMap      map[string]*FunctionSpec
	// subCatalogs maps a parent SimpleCatalog's wasm handle ptr to its
	// name→sub-catalog table, so every AddCatalog call happens at most
	// once per (parent, name) — the wasm side traps if you try to add
	// the same name twice.
	subCatalogs map[uint64]map[string]*googlesql.SimpleCatalog
}

func newSimpleCatalog(name string) *googlesql.SimpleCatalog {
	catalog := NewSimpleCatalog(name)
	_ = catalog.AddGoogleSQLFunctions()
	return catalog
}

func NewCatalog(db *sql.DB) *Catalog {
	return &Catalog{
		db:          db,
		catalog:     newSimpleCatalog(catalogName),
		tableMap:    map[string]*TableSpec{},
		funcMap:     map[string]*FunctionSpec{},
		subCatalogs: map[uint64]map[string]*googlesql.SimpleCatalog{},
	}
}

// getOrCreateSubCatalog returns the sub-catalog registered under
// parent+name, creating and attaching it on first use. AddCatalog is
// called exactly once per (parent ptr, sub-name) so wasm-side
// deduplication traps are impossible.
func (c *Catalog) getOrCreateSubCatalog(parent *googlesql.SimpleCatalog, name string) *googlesql.SimpleCatalog {
	parentPtr := handleRawPtr(parent)
	if parentPtr != 0 {
		subs := c.subCatalogs[parentPtr]
		if subs == nil {
			subs = map[string]*googlesql.SimpleCatalog{}
			c.subCatalogs[parentPtr] = subs
		}
		if existing, ok := subs[name]; ok {
			return existing
		}
		sub := newSimpleCatalog(name)
		_ = parent.AddCatalog(sub)
		subs[name] = sub
		return sub
	}
	// Parent has no recoverable ptr — fall back to a fresh catalog
	// (this only happens when reflection can't find the ptr field,
	// which shouldn't occur for a SimpleCatalog handle).
	sub := newSimpleCatalog(name)
	_ = parent.AddCatalog(sub)
	return sub
}

func (c *Catalog) FullName() string {
	s, _ := c.catalog.FullName()
	return s
}

// Find* methods are currently unsupported through the wasm bridge: the
// underlying googlesql::Catalog templated Find<T>(...) variants use
// output-pointer parameters the bridge cannot marshal today. Returning
// the stored SimpleCatalog handle via FindOptions would require a
// Go-side callback catalog wrapper, which is tracked separately.
func (c *Catalog) FindTable(path []string) (googlesql.TableNode, error) {
	if c.isWildcardTable(path) {
		return c.createWildcardTable(path)
	}
	return nil, fmt.Errorf("catalog: FindTable not yet supported via wasm bridge")
}

// registerWildcardTableByPath pre-creates a wildcard table for `path`
// and installs it into the googlesql catalog so the analyzer can
// resolve the reference. Called from the analyzer before a statement
// is analyzed (see preRegisterWildcardTables). Silent if the path
// does not match any registered tables.
func (c *Catalog) registerWildcardTableByPath(path []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.isWildcardTable(path) {
		return
	}
	wt, err := c.createWildcardTableImpl(path)
	if err != nil || wt == nil {
		return
	}
	simpleTable, err := c.createSimpleTable(strings.Join(path, "."), wt.spec)
	if err != nil {
		return
	}
	registerWildcardTable(simpleTable, wt)
	// Install into the wasm-side catalog tree. Walk the leading path
	// segments as sub-catalogs so the analyzer resolves the reference
	// in either form (fully-qualified or dataset-qualified).
	c.installWildcardIntoCatalog(c.catalog, path, simpleTable)
}

// installWildcardIntoCatalog adds `table` under each leading namespace
// prefix of `path`, so `project.dataset.table_*` resolves regardless
// of whether the query omits the project identifier.
func (c *Catalog) installWildcardIntoCatalog(cat *googlesql.SimpleCatalog, path []string, table *googlesql.SimpleTable) {
	if len(path) == 0 {
		return
	}
	if len(path) == 1 {
		if c.existsTable(cat, path[0]) {
			return
		}
		_ = cat.AddTable2(path[0], table)
		return
	}
	subName := path[0]
	sub := c.getOrCreateSubCatalog(cat, subName)
	c.installWildcardIntoCatalog(cat, path[1:], table)
	c.installWildcardIntoCatalog(sub, path[1:], table)
}

func (c *Catalog) normalizeTablePath(path []string) []string {
	result := []string{}
	for _, p := range path {
		parts := strings.Split(p, ".")
		result = append(result, parts...)
	}
	return result
}

func (c *Catalog) FindModel(path []string) (googlesql.ModelNode, error) {
	return nil, fmt.Errorf("catalog: FindModel not yet supported via wasm bridge")
}

func (c *Catalog) FindConnection(path []string) (googlesql.ConnectionNode, error) {
	return nil, fmt.Errorf("catalog: FindConnection not yet supported via wasm bridge")
}

func (c *Catalog) FindFunction(path []string) (*googlesql.Function, error) {
	return nil, fmt.Errorf("catalog: FindFunction not yet supported via wasm bridge")
}

func (c *Catalog) FindTableValuedFunction(path []string) (*googlesql.TableValuedFunction, error) {
	return nil, fmt.Errorf("catalog: FindTableValuedFunction not yet supported via wasm bridge")
}

func (c *Catalog) FindProcedure(path []string) (*googlesql.Procedure, error) {
	return nil, fmt.Errorf("catalog: FindProcedure not yet supported via wasm bridge")
}

func (c *Catalog) FindType(path []string) (googlesql.Googlesql_TypeNode, error) {
	return nil, fmt.Errorf("catalog: FindType not yet supported via wasm bridge")
}

func (c *Catalog) FindConstant(path []string) (googlesql.ConstantNode, int, error) {
	return nil, 0, fmt.Errorf("catalog: FindConstant not yet supported via wasm bridge")
}

func (c *Catalog) FindConversion(from, to googlesql.Googlesql_TypeNode) (*googlesql.Conversion, error) {
	return nil, fmt.Errorf("catalog: FindConversion not yet supported via wasm bridge")
}

// ExtendedTypeSuperTypes used to return *TypeListView but that alias was
// dropped when the clang-AST parser started expanding the FileScope
// typedef (TypeListView = absl::Span<const Type *const>) at parse time.
// Callers of this method only ever checked the error, so returning an
// empty slice keeps the contract without depending on the removed alias.
func (c *Catalog) ExtendedTypeSuperTypes(typ googlesql.Googlesql_TypeNode) ([]googlesql.Googlesql_TypeNode, error) {
	return nil, fmt.Errorf("catalog: ExtendedTypeSuperTypes not yet supported via wasm bridge")
}

func (c *Catalog) SuggestTable(mistypedPath []string) string {
	s, _ := c.catalog.SuggestTable(strings.Join(mistypedPath, "."))
	return s
}

func (c *Catalog) SuggestModel(mistypedPath []string) string {
	// SuggestModel isn't exposed on SimpleCatalog via the wasm bridge.
	return ""
}

func (c *Catalog) SuggestFunction(mistypedPath []string) string {
	s, _ := c.catalog.SuggestFunction(strings.Join(mistypedPath, "."))
	return s
}

func (c *Catalog) SuggestTableValuedFunction(mistypedPath []string) string {
	s, _ := c.catalog.SuggestTableValuedFunction(strings.Join(mistypedPath, "."))
	return s
}

func (c *Catalog) SuggestConstant(mistypedPath []string) string {
	s, _ := c.catalog.SuggestConstant(strings.Join(mistypedPath, "."))
	return s
}

func (c *Catalog) formatNamePath(path []string) string {
	return strings.Join(path, "_")
}

func (c *Catalog) getFunctions(namePath *NamePath) []*FunctionSpec {
	if namePath.empty() {
		return c.functions
	}
	key := c.formatNamePath(namePath.path)
	specs := make([]*FunctionSpec, 0, len(c.functions))
	for _, fn := range c.functions {
		if len(fn.NamePath) == 1 {
			// function name only
			specs = append(specs, fn)
			continue
		}
		pathPrefixKey := c.formatNamePath(c.trimmedLastPath(fn.NamePath))
		if strings.Contains(pathPrefixKey, key) {
			specs = append(specs, fn)
		}
	}
	return specs
}

func (c *Catalog) Sync(ctx context.Context, conn *Conn) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.createCatalogTablesIfNotExists(ctx, conn); err != nil {
		return fmt.Errorf("failed to create catalog tables: %w", err)
	}
	now := time.Now()
	rows, err := conn.QueryContext(
		ctx,
		`SELECT name, kind, spec FROM zetasqlite_catalog WHERE updatedAt >= @lastUpdatedAt`,
		c.lastSyncedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to query load catalog: %w", err)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			name string
			kind CatalogSpecKind
			spec string
		)
		if err := rows.Scan(&name, &kind, &spec); err != nil {
			return fmt.Errorf("failed to scan catalog values: %w", err)
		}
		switch kind {
		case TableSpecKind, ViewSpecKind:
			if err := c.loadTableSpec(spec); err != nil {
				return fmt.Errorf("failed to load table spec: %w", err)
			}
		case FunctionSpecKind:
			if err := c.loadFunctionSpec(spec); err != nil {
				return fmt.Errorf("failed to load function spec: %w", err)
			}
		default:
			return fmt.Errorf("unknown catalog spec kind %s", kind)
		}
	}
	c.lastSyncedAt = now
	return nil
}

func (c *Catalog) AddNewTableSpec(ctx context.Context, conn *Conn, spec *TableSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.addTableSpec(spec); err != nil {
		return err
	}
	if !spec.IsTemp {
		if err := c.saveTableSpec(ctx, conn, spec); err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) AddNewFunctionSpec(ctx context.Context, conn *Conn, spec *FunctionSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.addFunctionSpec(spec); err != nil {
		return err
	}
	if !spec.IsTemp {
		if err := c.saveFunctionSpec(ctx, conn, spec); err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) DeleteTableSpec(ctx context.Context, conn *Conn, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.deleteTableSpecByName(name); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, deleteCatalogQuery, sql.Named("name", name)); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) DeleteFunctionSpec(ctx context.Context, conn *Conn, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.deleteFunctionSpecByName(name); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, deleteCatalogQuery, sql.Named("name", name)); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) deleteTableSpecByName(name string) error {
	spec, exists := c.tableMap[name]
	if !exists {
		return fmt.Errorf("failed to find table spec from map by %s", name)
	}
	tables := make([]*TableSpec, 0, len(c.tables))
	specName := c.formatNamePath(spec.NamePath)
	for _, table := range c.tables {
		if specName == c.formatNamePath(table.NamePath) {
			continue
		}
		tables = append(tables, table)
	}
	if err := c.resetCatalog(tables, c.functions); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) deleteFunctionSpecByName(name string) error {
	spec, exists := c.funcMap[name]
	if !exists {
		return fmt.Errorf("failed to find function spec from map by %s", name)
	}
	functions := make([]*FunctionSpec, 0, len(c.functions))
	specName := c.formatNamePath(spec.NamePath)
	for _, function := range c.functions {
		if specName == c.formatNamePath(function.NamePath) {
			continue
		}
		functions = append(functions, function)
	}
	if err := c.resetCatalog(c.tables, functions); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) resetCatalog(tables []*TableSpec, functions []*FunctionSpec) error {
	c.catalog = newSimpleCatalog(catalogName)
	c.tables = []*TableSpec{}
	c.functions = []*FunctionSpec{}
	c.tableMap = map[string]*TableSpec{}
	c.funcMap = map[string]*FunctionSpec{}
	for _, spec := range tables {
		if err := c.addTableSpec(spec); err != nil {
			return err
		}
	}
	for _, spec := range functions {
		if err := c.addFunctionSpec(spec); err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) saveTableSpec(ctx context.Context, conn *Conn, spec *TableSpec) error {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("failed to encode table spec: %w", err)
	}
	now := time.Now()
	kind := string(TableSpecKind)
	if spec.IsView {
		kind = string(ViewSpecKind)
	}
	if _, err := conn.ExecContext(
		ctx,
		upsertCatalogQuery,
		sql.Named("name", spec.TableName()),
		sql.Named("kind", kind),
		sql.Named("spec", string(encoded)),
		sql.Named("updatedAt", now),
		sql.Named("createdAt", now),
	); err != nil {
		return fmt.Errorf("failed to save a new table spec: %w", err)
	}
	return nil
}

func (c *Catalog) saveFunctionSpec(ctx context.Context, conn *Conn, spec *FunctionSpec) error {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("failed to encode function spec: %w", err)
	}
	now := time.Now()
	if _, err := conn.ExecContext(
		ctx,
		upsertCatalogQuery,
		sql.Named("name", spec.FuncName()),
		sql.Named("kind", string(FunctionSpecKind)),
		sql.Named("spec", string(encoded)),
		sql.Named("updatedAt", now),
		sql.Named("createdAt", now),
	); err != nil {
		return fmt.Errorf("failed to save a new function spec: %w", err)
	}
	return nil
}

func (c *Catalog) createCatalogTablesIfNotExists(ctx context.Context, conn *Conn) error {
	if _, err := conn.ExecContext(ctx, createCatalogTableQuery); err != nil {
		return fmt.Errorf("failed to create catalog table: %w", err)
	}
	return nil
}

func (c *Catalog) loadTableSpec(spec string) error {
	var v TableSpec
	if err := json.Unmarshal([]byte(spec), &v); err != nil {
		return fmt.Errorf("failed to decode table spec: %w", err)
	}
	if err := c.addTableSpec(&v); err != nil {
		return fmt.Errorf("failed to add table spec to catalog: %w", err)
	}
	return nil
}

func (c *Catalog) loadFunctionSpec(spec string) error {
	var v FunctionSpec
	if err := json.Unmarshal([]byte(spec), &v); err != nil {
		return fmt.Errorf("failed to decode function spec: %w", err)
	}
	if err := c.addFunctionSpec(&v); err != nil {
		return fmt.Errorf("failed to add function spec to catalog: %w", err)
	}
	return nil
}

func (c *Catalog) trimmedLastPath(path []string) []string {
	if len(path) == 0 {
		return path
	}
	return path[:len(path)-1]
}

func (c *Catalog) addFunctionSpec(spec *FunctionSpec) error {
	funcName := spec.FuncName()
	if _, exists := c.funcMap[funcName]; exists {
		c.funcMap[funcName] = spec // update current spec
		return nil
	}
	c.functions = append(c.functions, spec)
	c.funcMap[funcName] = spec
	if err := c.addFunctionSpecRecursive(c.catalog, spec); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) addTableSpec(spec *TableSpec) error {
	tableName := spec.TableName()
	if _, exists := c.tableMap[tableName]; exists {
		c.tableMap[tableName] = spec // update current spec
		return nil
	}
	c.tables = append(c.tables, spec)
	c.tableMap[tableName] = spec
	if err := c.addTableSpecRecursive(c.catalog, spec); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) addTableSpecRecursive(cat *googlesql.SimpleCatalog, spec *TableSpec) error {
	if len(spec.NamePath) > 1 {
		subCatalogName := spec.NamePath[0]
		// Reuse the previously-added sub-catalog when the same name
		// reappears (e.g. "project" on the path of every table). Adding
		// a new SimpleCatalog handle under an already-registered name
		// traps the wasm runtime because SimpleCatalog::AddCatalog
		// rejects duplicates via an CHECK.
		subCatalog := c.getOrCreateSubCatalog(cat, subCatalogName)
		fullTableName := strings.Join(spec.NamePath, ".")
		if !c.existsTable(cat, fullTableName) {
			table, err := c.createSimpleTable(fullTableName, spec)
			if err != nil {
				return err
			}
			if err := cat.AddTable(table); err != nil {
				return fmt.Errorf("SimpleCatalog.AddTable(%q): %w", fullTableName, err)
			}
		}
		newNamePath := spec.NamePath[1:]
		// add sub catalog to root catalog
		if err := c.addTableSpecRecursive(cat, c.copyTableSpec(spec, newNamePath)); err != nil {
			return fmt.Errorf("failed to add table spec to root catalog: %w", err)
		}
		// add sub catalog to parent catalog
		if err := c.addTableSpecRecursive(subCatalog, c.copyTableSpec(spec, newNamePath)); err != nil {
			return fmt.Errorf("failed to add table spec to parent catalog: %w", err)
		}
		return nil
	}
	if len(spec.NamePath) == 0 {
		return fmt.Errorf("table name is not found")
	}

	tableName := spec.NamePath[0]
	if c.existsTable(cat, tableName) {
		return nil
	}
	table, err := c.createSimpleTable(tableName, spec)
	if err != nil {
		return err
	}
	if err := cat.AddTable(table); err != nil {
		return fmt.Errorf("SimpleCatalog.AddTable(%q): %w", tableName, err)
	}
	return nil
}

func (c *Catalog) createSimpleTable(tableName string, spec *TableSpec) (*googlesql.SimpleTable, error) {
	columns := []*googlesql.SimpleColumn{}
	for _, column := range spec.Columns {
		typ, err := column.Type.ToZetaSQLType()
		if err != nil {
			return nil, err
		}
		columns = append(columns, NewSimpleColumn(
			tableName, column.Name, typ,
		))
	}
	return NewSimpleTable(tableName, columns), nil
}

func (c *Catalog) addFunctionSpecRecursive(cat *googlesql.SimpleCatalog, spec *FunctionSpec) error {
	if len(spec.NamePath) > 1 {
		subCatalogName := spec.NamePath[0]
		subCatalog := c.getOrCreateSubCatalog(cat, subCatalogName)
		newNamePath := spec.NamePath[1:]
		// add sub catalog to root catalog
		if err := c.addFunctionSpecRecursive(cat, c.copyFunctionSpec(spec, newNamePath)); err != nil {
			return fmt.Errorf("failed to add function spec to root catalog: %w", err)
		}
		// add sub catalog to parent catalog
		if err := c.addFunctionSpecRecursive(subCatalog, c.copyFunctionSpec(spec, newNamePath)); err != nil {
			return fmt.Errorf("failed to add function spec to parent catalog: %w", err)
		}
		return nil
	}
	if len(spec.NamePath) == 0 {
		return fmt.Errorf("function name is not found")
	}

	funcName := spec.NamePath[0]
	if c.existsFunction(cat, funcName) {
		return nil
	}
	argTypes := []*googlesql.FunctionArgumentType{}
	for _, arg := range spec.Args {
		argType, err := arg.FunctionArgumentType()
		if err != nil {
			return err
		}
		argTypes = append(argTypes, argType)
	}
	retType, err := spec.Return.FunctionArgumentType()
	if err != nil {
		return err
	}
	sig := NewFunctionSignature(retType, argTypes)
	newFunc, err := NewFunction([]string{funcName}, "", int(ScalarMode(0)), []*googlesql.FunctionSignature{sig}, nil)
	if err != nil {
		return err
	}
	_ = cat.AddFunction(newFunc)
	return nil
}

func (c *Catalog) existsTable(cat *googlesql.SimpleCatalog, name string) bool {
	// Checks presence on the *wasm-side* SimpleCatalog via TableNames().
	// The earlier implementation short-circuited against c.tableMap,
	// which caused AddTable to be skipped for tables that were already
	// recorded on the Go side but never propagated into the wasm
	// catalog — the analyzer then couldn't find them.
	names, err := cat.TableNames()
	if err != nil {
		return false
	}
	target := strings.ToLower(name)
	for _, n := range names {
		if strings.ToLower(n) == target {
			return true
		}
	}
	return false
}

func (c *Catalog) existsFunction(cat *googlesql.SimpleCatalog, name string) bool {
	names, err := cat.FunctionNames()
	if err != nil {
		return false
	}
	target := strings.ToLower(name)
	for _, n := range names {
		if strings.ToLower(n) == target {
			return true
		}
	}
	return false
}

func (c *Catalog) isNilTable(t googlesql.TableNode) bool {
	v := reflect.ValueOf(t)
	if !v.IsValid() {
		return true
	}
	return v.IsNil()
}

func (c *Catalog) copyTableSpec(spec *TableSpec, newNamePath []string) *TableSpec {
	return &TableSpec{
		NamePath:   newNamePath,
		Columns:    spec.Columns,
		CreateMode: spec.CreateMode,
	}
}

func (c *Catalog) copyFunctionSpec(spec *FunctionSpec, newNamePath []string) *FunctionSpec {
	return &FunctionSpec{
		NamePath: newNamePath,
		Language: spec.Language,
		Args:     spec.Args,
		Return:   spec.Return,
		Code:     spec.Code,
		Body:     spec.Body,
	}
}
