package zetasqlite_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/goccy/go-zetasqlite"
)

func TestExec(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	ctx = zetasqlite.WithCurrentTime(ctx, now)
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, test := range []struct {
		name        string
		query       string
		args        []interface{}
		expectedErr bool
	}{
		{
			name:  "create table with all types",
			query: `CREATE TABLE _table_a ( doubleValue DOUBLE, floatValue FLOAT )`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.ExecContext(ctx, test.query); err != nil {
				t.Fatal(err)
			}
		})
	}
}
