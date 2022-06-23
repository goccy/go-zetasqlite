package zetasqlite

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/goccy/go-zetasql/types"
)

var (
	createCatalogTableQuery = `
CREATE TABLE IF NOT EXISTS zetasqlite_catalog(
  name STRING NOT NULL PRIMARY KEY,
  kind STRING NOT NULL,
  spec STRING NOT NULL
)
`
	loadCatalogQuery   = `SELECT name, kind, spec FROM zetasqlite_catalog`
	upsertCatalogQuery = `INSERT INTO zetasqlite_catalog (name, kind, spec) VALUES (?, ?, ?) ON CONFLICT(name) DO UPDATE SET spec = ?`
)

type CatalogSpecKind string

const (
	TableSpecKind    CatalogSpecKind = "table"
	FunctionSpecKind CatalogSpecKind = "function"
)

type Catalog struct {
	conn      *ZetaSQLiteConn
	catalog   *types.SimpleCatalog
	tables    []*TableSpec
	functions []*FunctionSpec
	tableMap  map[string]*TableSpec
	funcMap   map[string]*FunctionSpec
	mu        sync.Mutex
}

func newCatalog(conn *ZetaSQLiteConn) *Catalog {
	catalog := types.NewSimpleCatalog("zetasqlite")
	catalog.AddZetaSQLBuiltinFunctions()
	return &Catalog{
		conn:     conn,
		catalog:  catalog,
		tableMap: map[string]*TableSpec{},
		funcMap:  map[string]*FunctionSpec{},
	}
}

func (c *Catalog) Sync() error {
	if err := c.createCatalogTablesIfNotExists(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	stmt, err := c.conn.sqliteConn.Prepare(loadCatalogQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare load catalog: %w", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(nil)
	if err != nil {
		return fmt.Errorf("failed to query load catalog: %w", err)
	}
	defer rows.Close()
	for {
		var (
			name string
			kind CatalogSpecKind
			spec string
		)
		values := []driver.Value{&name, &kind, &spec}
		if err := rows.Next(values); err != nil {
			if err != io.EOF {
				return fmt.Errorf("failed to load catalog values: %w", err)
			}
			break
		}
		spec = values[2].(string)
		switch CatalogSpecKind(values[1].(string)) {
		case TableSpecKind:
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
	return nil
}

func (c *Catalog) AddNewTableSpec(spec *TableSpec) error {
	if err := c.addTableSpec(spec); err != nil {
		return err
	}
	if err := c.saveTableSpec(spec); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) AddNewFunctionSpec(spec *FunctionSpec) error {
	if err := c.addFunctionSpec(spec); err != nil {
		return err
	}
	if err := c.saveFunctionSpec(spec); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) saveTableSpec(spec *TableSpec) error {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("failed to encode table spec: %w", err)
	}

	if err := c.exec(upsertCatalogQuery, []driver.Value{
		spec.TableName(),
		string(TableSpecKind),
		string(encoded),
		string(encoded),
	}); err != nil {
		return fmt.Errorf("failed to save a new table spec: %w", err)
	}
	return nil
}

func (c *Catalog) saveFunctionSpec(spec *FunctionSpec) error {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("failed to encode function spec: %w", err)
	}
	if err := c.exec(upsertCatalogQuery, []driver.Value{
		spec.FuncName(),
		string(FunctionSpecKind),
		string(encoded),
		string(encoded),
	}); err != nil {
		return fmt.Errorf("failed to save a new function spec: %w", err)
	}
	return nil
}

func (c *Catalog) createCatalogTablesIfNotExists() error {
	if err := c.exec(createCatalogTableQuery, nil); err != nil {
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

func (c *Catalog) addFunctionSpec(spec *FunctionSpec) error {
	funcName := spec.FuncName()
	if _, exists := c.funcMap[funcName]; exists {
		return nil
	}
	if err := c.conn.sqliteConn.RegisterFunc(funcName, spec.FuncBody(c.conn), true); err != nil {
		return fmt.Errorf("failed to register user defined function: %w", err)
	}
	c.functions = append(c.functions, spec)
	c.funcMap[funcName] = spec
	return c.addFunctionSpecRecursive(c.catalog, spec)
}

func (c *Catalog) addTableSpec(spec *TableSpec) error {
	tableName := spec.TableName()
	if _, exists := c.tableMap[tableName]; exists {
		return nil
	}
	c.tables = append(c.tables, spec)
	c.tableMap[tableName] = spec
	return c.addTableSpecRecursive(c.catalog, spec)
}

func (c *Catalog) addTableSpecRecursive(cat *types.SimpleCatalog, spec *TableSpec) error {
	if len(spec.NamePath) > 1 {
		subCatalogName := spec.NamePath[0]
		subCatalog := types.NewSimpleCatalog(subCatalogName)
		if !c.existsCatalog(cat, subCatalogName) {
			cat.AddCatalog(subCatalog)
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
	columns := []types.Column{}
	for _, column := range spec.Columns {
		typ, err := column.Type.ToZetaSQLType()
		if err != nil {
			return err
		}
		columns = append(columns, types.NewSimpleColumn(
			tableName, column.Name, typ,
		))
	}
	cat.AddTable(types.NewSimpleTable(tableName, columns))
	return nil
}

func (c *Catalog) addFunctionSpecRecursive(cat *types.SimpleCatalog, spec *FunctionSpec) error {
	if len(spec.NamePath) > 1 {
		subCatalogName := spec.NamePath[0]
		subCatalog := types.NewSimpleCatalog(subCatalogName)
		if !c.existsCatalog(cat, subCatalogName) {
			cat.AddCatalog(subCatalog)
		}
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
	argTypes := []*types.FunctionArgumentType{}
	for _, arg := range spec.Args {
		t, err := arg.Type.ToZetaSQLType()
		if err != nil {
			return err
		}
		argTypes = append(argTypes, types.NewFunctionArgumentType(arg.Name, t))
	}
	retType, err := spec.Return.ToZetaSQLType()
	if err != nil {
		return err
	}
	returnType := types.NewFunctionArgumentType(spec.Return.Name, retType)
	sig := types.NewFunctionSignature(returnType, argTypes)
	newFunc := types.NewFunction([]string{funcName}, "", types.ScalarMode, []*types.FunctionSignature{sig})
	cat.AddFunction(newFunc)
	return nil
}

func (c *Catalog) existsCatalog(cat *types.SimpleCatalog, name string) bool {
	foundCatalog, _ := cat.Catalog(name)
	return !c.isNilCatalog(foundCatalog)
}

func (c *Catalog) existsTable(cat *types.SimpleCatalog, name string) bool {
	foundTable, _ := cat.FindTable([]string{name})
	return !c.isNilTable(foundTable)
}

func (c *Catalog) existsFunction(cat *types.SimpleCatalog, name string) bool {
	foundFunc, _ := cat.FindFunction([]string{name})
	return foundFunc != nil
}

func (c *Catalog) isNilCatalog(cat types.Catalog) bool {
	v := reflect.ValueOf(cat)
	if !v.IsValid() {
		return true
	}
	return v.IsNil()
}

func (c *Catalog) isNilTable(t types.Table) bool {
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

func (c *Catalog) exec(query string, args []driver.Value) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	stmt, err := c.conn.sqliteConn.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare %s: %w", query, err)
	}
	defer stmt.Close()
	if _, err := stmt.Exec(args); err != nil {
		return fmt.Errorf("failed to exec %s: %w", query, err)
	}
	return nil
}

func (c *Catalog) query(query string, args []driver.Value) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stmt, err := c.conn.sqliteConn.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare %s: %w", query, err)
	}
	rows, err := stmt.Query(args)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", query, err)
	}
	return rows, nil
}
