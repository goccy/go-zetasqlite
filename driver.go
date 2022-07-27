package zetasqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/goccy/go-zetasql"
	internal "github.com/goccy/go-zetasqlite/internal"
	"github.com/mattn/go-sqlite3"
)

var (
	_ driver.Driver = &ZetaSQLiteDriver{}
	_ driver.Conn   = &ZetaSQLiteConn{}
	_ driver.Tx     = &ZetaSQLiteTx{}
)

var (
	nameToCatalogMap = map[string]*internal.Catalog{}
	nameToDBMap      = map[string]*sql.DB{}
	nameToValueMapMu sync.Mutex
)

func init() {
	sql.Register("zetasqlite", &ZetaSQLiteDriver{})
	sql.Register("zetasqlite_sqlite3", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if err := internal.RegisterFunctions(conn); err != nil {
				return err
			}
			return nil
		},
	})
}

func newDBAndCatalog(name string) (*sql.DB, *internal.Catalog, error) {
	nameToValueMapMu.Lock()
	defer nameToValueMapMu.Unlock()
	db, exists := nameToDBMap[name]
	if exists {
		return db, nameToCatalogMap[name], nil
	}
	db, err := sql.Open("zetasqlite_sqlite3", name)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open database by %s: %w", name, err)
	}
	catalog := internal.NewCatalog(db)
	nameToDBMap[name] = db
	nameToCatalogMap[name] = catalog
	return db, catalog, nil
}

type ZetaSQLiteDriver struct {
	ConnectHook func(*ZetaSQLiteConn) error
}

func (d *ZetaSQLiteDriver) Open(name string) (driver.Conn, error) {
	db, catalog, err := newDBAndCatalog(name)
	if err != nil {
		return nil, err
	}
	conn, err := newZetaSQLiteConn(db, catalog)
	if err != nil {
		return nil, err
	}
	if d.ConnectHook != nil {
		if err := d.ConnectHook(conn); err != nil {
			return nil, err
		}
	}
	return conn, nil
}

type ZetaSQLiteConn struct {
	conn     *sql.Conn
	tx       *sql.Tx
	analyzer *internal.Analyzer
}

func newZetaSQLiteConn(db *sql.DB, catalog *internal.Catalog) (*ZetaSQLiteConn, error) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get sqlite3 connection: %w", err)
	}
	return &ZetaSQLiteConn{
		conn:     conn,
		analyzer: internal.NewAnalyzer(catalog),
	}, nil
}

func (c *ZetaSQLiteConn) NamePath() []string {
	return c.analyzer.NamePath()
}

func (c *ZetaSQLiteConn) SetNamePath(path []string) {
	c.analyzer.SetNamePath(path)
}

func (c *ZetaSQLiteConn) AddNamePath(path string) {
	c.analyzer.AddNamePath(path)
}

func (c *ZetaSQLiteConn) SetParameterMode(mode zetasql.ParameterMode) {
	c.analyzer.SetParameterMode(mode)
}

func (s *ZetaSQLiteConn) CheckNamedValue(value *driver.NamedValue) error {
	return nil
}

func (c *ZetaSQLiteConn) Prepare(query string) (driver.Stmt, error) {
	conn := internal.NewConn(c.conn, c.tx)
	out, err := c.analyzer.Analyze(context.Background(), conn, query)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze query: %w", err)
	}
	return out.Prepare(context.Background(), conn)
}

func (c *ZetaSQLiteConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	conn := internal.NewConn(c.conn, c.tx)
	out, err := c.analyzer.Analyze(ctx, conn, query)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze query: %w", err)
	}
	newNamedValues, err := internal.EncodeNamedValues(args, out.Params())
	if err != nil {
		return nil, err
	}
	newArgs := make([]interface{}, 0, len(args))
	for _, newNamedValue := range newNamedValues {
		newArgs = append(newArgs, newNamedValue)
	}
	return out.ExecContext(ctx, conn, newArgs...)
}

func (c *ZetaSQLiteConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	conn := internal.NewConn(c.conn, c.tx)
	out, err := c.analyzer.Analyze(ctx, conn, query)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze query: %w", err)
	}
	newNamedValues, err := internal.EncodeNamedValues(args, out.Params())
	if err != nil {
		return nil, err
	}
	newArgs := make([]interface{}, 0, len(args))
	for _, newNamedValue := range newNamedValues {
		newArgs = append(newArgs, newNamedValue)
	}
	return out.QueryContext(ctx, conn, newArgs...)
}

func (c *ZetaSQLiteConn) Close() error {
	return c.conn.Close()
}

func (c *ZetaSQLiteConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(opts.Isolation),
		ReadOnly:  opts.ReadOnly,
	})
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return &ZetaSQLiteTx{
		tx:   tx,
		conn: c,
	}, nil
}

func (c *ZetaSQLiteConn) Begin() (driver.Tx, error) {
	tx, err := c.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return &ZetaSQLiteTx{
		tx:   tx,
		conn: c,
	}, nil
}

type ZetaSQLiteTx struct {
	tx   *sql.Tx
	conn *ZetaSQLiteConn
}

func (tx *ZetaSQLiteTx) Commit() error {
	defer func() {
		tx.conn.tx = nil
	}()
	return tx.tx.Commit()
}

func (tx *ZetaSQLiteTx) Rollback() error {
	defer func() {
		tx.conn.tx = nil
	}()
	return tx.tx.Rollback()
}
