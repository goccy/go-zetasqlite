package zetasqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/goccy/go-zetasqlite"
	"github.com/google/go-cmp/cmp"
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
			name: "create table with all types",
			query: `
CREATE TABLE _table_a (
 intValue        INT64,
 boolValue       BOOL,
 doubleValue     DOUBLE,
 floatValue      FLOAT,
 stringValue     STRING,
 bytesValue      BYTES,
 numericValue    NUMERIC,
 bignumericValue BIGNUMERIC,
 intervalValue   INTERVAL,
 dateValue       DATE,
 datetimeValue   DATETIME,
 timeValue       TIME,
 timestampValue  TIMESTAMP
)`,
		},
		{
			name: "create table as select",
			query: `
CREATE TABLE foo ( id STRING, name STRING );
CREATE TABLE bar ( id STRING, name STRING );
CREATE OR REPLACE TABLE new_table_as_select AS (
  SELECT t1.id, t2.name FROM foo t1 JOIN bar t2 ON t1.id = t2.id
);
`,
		},
		{
			name: "recreate table",
			query: `
CREATE OR REPLACE TABLE recreate_table ( a string );
DROP TABLE recreate_table;
CREATE TABLE recreate_table ( b string );
INSERT recreate_table (b) VALUES ('hello');
`,
		},
		{
			name: "transaction",
			query: `
CREATE OR REPLACE TABLE Inventory
(
 product string,
 quantity int64,
 supply_constrained bool
);

CREATE OR REPLACE TABLE NewArrivals
(
 product string,
 quantity int64,
 warehouse string
);

INSERT Inventory (product, quantity)
VALUES('top load washer', 10),
     ('front load washer', 20),
     ('dryer', 30),
     ('refrigerator', 10),
     ('microwave', 20),
     ('dishwasher', 30);

INSERT NewArrivals (product, quantity, warehouse)
VALUES('top load washer', 100, 'warehouse #1'),
     ('dryer', 200, 'warehouse #2'),
     ('oven', 300, 'warehouse #1');

BEGIN TRANSACTION;

CREATE TEMP TABLE tmp AS SELECT * FROM NewArrivals WHERE warehouse = 'warehouse #1';
DELETE NewArrivals WHERE warehouse = 'warehouse #1';
MERGE Inventory AS I
USING tmp AS T
ON I.product = T.product
WHEN NOT MATCHED THEN
 INSERT(product, quantity, supply_constrained)
 VALUES(product, quantity, false)
WHEN MATCHED THEN
 UPDATE SET quantity = I.quantity + T.quantity;

TRUNCATE TABLE tmp;

COMMIT TRANSACTION;
`,
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

func TestCreateTempTable(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	ctx = zetasqlite.WithCurrentTime(ctx, now)
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "CREATE TEMP TABLE tmp_table (id INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TEMP TABLE tmp_table (id INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE tmp_table (id INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE tmp_table (id INT64)"); err == nil {
		t.Fatal("expected error")
	}
}

func TestWildcardTable(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.table_a` AS SELECT specialName FROM UNNEST (['alice_a', 'bob_a']) as specialName",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.table_b` AS SELECT name FROM UNNEST(['alice_b', 'bob_b']) as name",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.table_c` AS SELECT name FROM UNNEST(['alice_c', 'bob_c']) as name",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.other_d` AS SELECT name FROM UNNEST(['alice_d', 'bob_d']) as name",
	); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, "SELECT name, _TABLE_SUFFIX FROM `project.dataset.table_*` WHERE name LIKE 'alice%' OR name IS NULL")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type queryRow struct {
		Name   *string
		Suffix string
	}
	var results []*queryRow
	for rows.Next() {
		var (
			name   *string
			suffix string
		)
		if err := rows.Scan(&name, &suffix); err != nil {
			t.Fatal(err)
		}
		results = append(results, &queryRow{
			Name:   name,
			Suffix: suffix,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	stringPtr := func(v string) *string { return &v }
	if diff := cmp.Diff(results, []*queryRow{
		{Name: stringPtr("alice_c"), Suffix: "c"},
		{Name: stringPtr("alice_b"), Suffix: "b"},
		{Name: nil, Suffix: "a"},
		{Name: nil, Suffix: "a"},
	}); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}

func TestTemplatedArgFunc(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(
		ctx,
		`CREATE FUNCTION MAX_FROM_ARRAY(arr ANY TYPE) as (( SELECT MAX(x) FROM UNNEST(arr) as x ))`,
	); err != nil {
		t.Fatal(err)
	}
	t.Run("int64", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "SELECT MAX_FROM_ARRAY([1, 4, 2, 3])")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var num int64
			if err := rows.Scan(&num); err != nil {
				t.Fatal(err)
			}
			if num != 4 {
				t.Fatalf("failed to get max number. got %d", num)
			}
			break
		}
		if rows.Err() != nil {
			t.Fatal(err)
		}
	})
	t.Run("float64", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "SELECT MAX_FROM_ARRAY([1.234, 3.456, 4.567, 2.345])")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var num float64
			if err := rows.Scan(&num); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(num) != "4.567" {
				t.Fatalf("failed to get max number. got %f", num)
			}
			break
		}
		if rows.Err() != nil {
			t.Fatal(err)
		}
	})

}
