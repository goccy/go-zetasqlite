package zetasqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/mattn/go-sqlite3"
)

func init() {
	sql.Register("zetasqlite", &ZetaSQLiteDriver{})
}

var (
	_ driver.Driver = &ZetaSQLiteDriver{}
	_ driver.Conn   = &ZetaSQLiteConn{}
	_ driver.Tx     = &ZetaSQLiteTx{}
)

type ZetaSQLiteDriver struct {
	ConnectHook func(*ZetaSQLiteConn) error
}

func (d *ZetaSQLiteDriver) Open(name string) (driver.Conn, error) {
	conn, err := newZetaSQLiteConn(name)
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
	sqliteConn *sqlite3.SQLiteConn
	conn       driver.Conn
	analyzer   *Analyzer
}

func newZetaSQLiteConn(name string) (*ZetaSQLiteConn, error) {
	var sqliteConn *sqlite3.SQLiteConn
	sqliteDriver := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if err := registerBuiltinFunctions(conn); err != nil {
				return err
			}
			sqliteConn = conn
			return nil
		},
	}
	conn, err := sqliteDriver.Open(name)
	if err != nil {
		return nil, fmt.Errorf("zetasqlite: failed to open database: %w", err)
	}
	c := &ZetaSQLiteConn{
		sqliteConn: sqliteConn,
		conn:       conn,
	}
	c.analyzer = newAnalyzer(newCatalog(c))
	return c, nil
}

func (c *ZetaSQLiteConn) NamePath() []string {
	return c.analyzer.namePath
}

func (c *ZetaSQLiteConn) SetNamePath(path []string) {
	c.analyzer.namePath = path
}

func (c *ZetaSQLiteConn) AddNamePath(path string) {
	c.analyzer.namePath = append(c.analyzer.namePath, path)
}

func (s *ZetaSQLiteConn) CheckNamedValue(value *driver.NamedValue) error {
	return nil
}

func (c *ZetaSQLiteConn) Prepare(query string) (driver.Stmt, error) {
	out, err := c.analyzer.Analyze(query)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze query: %w", err)
	}
	return out.prepare(c.conn)
}

func (c *ZetaSQLiteConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	out, err := c.analyzer.Analyze(query)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze query: %w", err)
	}
	newArgs, err := convertNamedValues(args)
	if err != nil {
		return nil, err
	}
	return out.execContext(ctx, c.conn.(driver.ExecerContext), newArgs)
}

func (c *ZetaSQLiteConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	out, err := c.analyzer.Analyze(query)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze query: %w", err)
	}
	newArgs, err := convertNamedValues(args)
	if err != nil {
		return nil, err
	}
	return out.queryContext(ctx, c.conn.(driver.QueryerContext), newArgs)
}

func (c *ZetaSQLiteConn) Close() error {
	return c.conn.Close()
}

func (c *ZetaSQLiteConn) Begin() (driver.Tx, error) {
	tx, err := c.conn.Begin()
	if err != nil {
		return nil, err
	}
	return &ZetaSQLiteTx{
		tx:   tx,
		conn: c,
	}, nil
}

type ZetaSQLiteTx struct {
	tx   driver.Tx
	conn *ZetaSQLiteConn
}

func (tx *ZetaSQLiteTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *ZetaSQLiteTx) Rollback() error {
	return tx.tx.Rollback()
}
