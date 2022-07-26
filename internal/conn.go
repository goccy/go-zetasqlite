package internal

import (
	"context"
	"database/sql"
)

type Conn struct {
	conn *sql.Conn
	tx   *sql.Tx
}

func NewConn(conn *sql.Conn, tx *sql.Tx) *Conn {
	return &Conn{conn: conn, tx: tx}
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if c.tx != nil {
		return c.tx.PrepareContext(ctx, query)
	}
	return c.conn.PrepareContext(ctx, query)
}

func (c *Conn) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if c.tx != nil {
		return c.tx.ExecContext(ctx, query, args...)
	}
	return c.conn.ExecContext(ctx, query, args...)
}

func (c *Conn) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if c.tx != nil {
		return c.tx.QueryContext(ctx, query, args...)
	}
	return c.conn.QueryContext(ctx, query, args...)
}
