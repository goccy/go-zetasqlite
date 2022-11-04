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
