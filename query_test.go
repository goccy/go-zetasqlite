package zetasqlite_test

import (
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/goccy/go-zetasqlite"
	"github.com/google/go-cmp/cmp"
)

func TestQuery(t *testing.T) {
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, test := range []struct {
		name         string
		query        string
		args         []interface{}
		expectedRows [][]interface{}
		expectedErr  bool
	}{
		// priority 2 operator
		{
			name:         "unary plus operator",
			query:        "SELECT +1",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "unary minus operator",
			query:        "SELECT -2",
			expectedRows: [][]interface{}{{int64(-2)}},
		},
		{
			name:         "bit not operator",
			query:        "SELECT ~1",
			expectedRows: [][]interface{}{{int64(-2)}},
		},
		// priority 3 operator
		{
			name:         "mul operator",
			query:        "SELECT 2 * 3",
			expectedRows: [][]interface{}{{int64(6)}},
		},
		{
			name:         "div operator",
			query:        "SELECT 10 / 2",
			expectedRows: [][]interface{}{{float64(5)}},
		},
		{
			name:         "concat operator",
			query:        `SELECT "a" || "b"`,
			expectedRows: [][]interface{}{{"ab"}},
		},
		// priority 4 operator
		{
			name:         "add operator",
			query:        "SELECT 1 + 1",
			expectedRows: [][]interface{}{{int64(2)}},
		},
		{
			name:         "sub operator",
			query:        "SELECT 1 - 2",
			expectedRows: [][]interface{}{{int64(-1)}},
		},
		// priority 5 operator
		{
			name:         "left shift operator",
			query:        "SELECT 1 << 2",
			expectedRows: [][]interface{}{{int64(4)}},
		},
		{
			name:         "right shift operator",
			query:        "SELECT 4 >> 1",
			expectedRows: [][]interface{}{{int64(2)}},
		},
		// priority 6 operator
		{
			name:         "bit and operator",
			query:        "SELECT 3 & 1",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		// priority 7 operator
		{
			name:         "bit xor operator",
			query:        "SELECT 10 ^ 12",
			expectedRows: [][]interface{}{{int64(6)}},
		},
		// priority 8 operator
		{
			name:         "bit or operator",
			query:        "SELECT 1 | 2",
			expectedRows: [][]interface{}{{int64(3)}},
		},
		// priority 9 operator
		{
			name:         "eq operator",
			query:        "SELECT 100 = 100",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "lt operator",
			query:        "SELECT 10 < 100",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "gt operator",
			query:        "SELECT 100 > 10",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "lte operator",
			query:        "SELECT 10 <= 10",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "gte operator",
			query:        "SELECT 10 >= 10",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "ne operator",
			query:        "SELECT 100 != 10",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "like operator",
			query:        `SELECT "abcd" LIKE "a%d"`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "not like operator",
			query:        `SELECT "abcd" NOT LIKE "a%d"`,
			expectedRows: [][]interface{}{{int64(0)}},
		},
		{
			name:         "between operator",
			query:        `SELECT "2022-09-10" BETWEEN "2022-09-01" and "2022-10-01"`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "not between operator",
			query:        `SELECT "2020-09-10" NOT BETWEEN "2022-09-01" and "2022-10-01"`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "in operator",
			query:        `SELECT 3 IN (1, 2, 3, 4)`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "not in operator",
			query:        `SELECT 5 NOT IN (1, 2, 3, 4)`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "is null operator",
			query:        `SELECT NULL IS NULL`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "is not null operator",
			query:        `SELECT 1 IS NOT NULL`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "is true operator",
			query:        `SELECT true IS TRUE`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "is not true operator",
			query:        `SELECT false IS NOT TRUE`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "is false operator",
			query:        `SELECT false IS FALSE`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "is not false operator",
			query:        `SELECT true IS NOT FALSE`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		// priority 10 operator
		{
			name:         "not operator",
			query:        `SELECT NOT 1 = 2`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		// priority 11 operator
		{
			name:         "and operator",
			query:        `SELECT 1 = 1 AND 2 = 2`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		// priority 12 operator
		{
			name:         "or operator",
			query:        `SELECT 1 = 2 OR 1 = 1`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "exists",
			query:        `SELECT EXISTS ( SELECT val FROM UNNEST([1, 2, 3]) AS val WHERE val = 1 )`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "not exists",
			query:        `SELECT EXISTS ( SELECT val FROM UNNEST([1, 2, 3]) AS val WHERE val = 4 )`,
			expectedRows: [][]interface{}{{int64(0)}},
		},
		// not supported `IS DISTINCT FROM` by zetasql
		//{
		//	name:         "is distinct from",
		//	query:        `SELECT 1 IS DISTINCT FROM 2`,
		//	expectedRows: [][]interface{}{{int64(1)}},
		//},
		{
			name: "case-when",
			query: `
SELECT
  val,
  CASE val
    WHEN 1 THEN 'one'
    WHEN 2 THEN 'two'
    WHEN 3 THEN 'three'
    ELSE 'four'
    END
FROM UNNEST([1, 2, 3, 4]) AS val`,
			expectedRows: [][]interface{}{
				{int64(1), "one"},
				{int64(2), "two"},
				{int64(3), "three"},
				{int64(4), "four"},
			},
		},
		{
			name: "case-when with compare",
			query: `
SELECT
  val,
  CASE
    WHEN val > 3 THEN 'four'
    WHEN val > 2 THEN 'three'
    WHEN val > 1 THEN 'two'
    ELSE 'one'
    END
FROM UNNEST([1, 2, 3, 4]) AS val`,
			expectedRows: [][]interface{}{
				{int64(1), "one"},
				{int64(2), "two"},
				{int64(3), "three"},
				{int64(4), "four"},
			},
		},
		{
			name:         "coalesce",
			query:        `SELECT COALESCE('A', 'B', 'C')`,
			expectedRows: [][]interface{}{{"A"}},
		},
		{
			name:         "coalesce with null",
			query:        `SELECT COALESCE(NULL, 'B', 'C')`,
			expectedRows: [][]interface{}{{"B"}},
		},
		{
			name:         "if return int64",
			query:        `SELECT IF("a" = "b", 1, 2)`,
			expectedRows: [][]interface{}{{int64(2)}},
		},
		{
			name:         "if return string",
			query:        `SELECT IF("a" = "a", "true", "false")`,
			expectedRows: [][]interface{}{{"true"}},
		},
		{
			name:         "ifnull",
			query:        `SELECT IFNULL(10, 0)`,
			expectedRows: [][]interface{}{{int64(10)}},
		},
		{
			name:         "ifnull with null",
			query:        `SELECT IFNULL(NULL, 0)`,
			expectedRows: [][]interface{}{{int64(0)}},
		},
		{
			name:         "nullif true",
			query:        `SELECT NULLIF(0, 0)`,
			expectedRows: [][]interface{}{{int64(0)}},
		},
		{
			name:         "nullif false",
			query:        `SELECT NULLIF(10, 0)`,
			expectedRows: [][]interface{}{{int64(10)}},
		},
		{
			name: "with clause",
			query: `
WITH sub1 AS (SELECT ["a", "b"]),
     sub2 AS (SELECT ["c", "d"])
SELECT * FROM sub1
UNION ALL
SELECT * FROM sub2`,
			expectedRows: [][]interface{}{
				{[]string{"a", "b"}},
				{[]string{"c", "d"}},
			},
		},
		{
			name: "field access operator",
			query: `
WITH orders AS (
  SELECT STRUCT(STRUCT('Yonge Street' AS street, 'Canada' AS country) AS address) AS customer
)
SELECT t.customer.address.country FROM orders AS t`,
			expectedRows: [][]interface{}{{"Canada"}},
		},
		{
			name: "array index access operator",
			query: `
WITH Items AS (SELECT ["coffee", "tea", "milk"] AS item_array)
SELECT
  item_array,
  item_array[OFFSET(1)] AS item_offset,
  item_array[ORDINAL(1)] AS item_ordinal,
  item_array[SAFE_OFFSET(6)] AS item_safe_offset,
FROM Items`,
			expectedRows: [][]interface{}{{
				[]string{"coffee", "tea", "milk"},
				"tea",
				"coffee",
				nil,
			}},
		},
		{
			name: "create function",
			query: `
CREATE FUNCTION customfunc(
  arr ARRAY<STRUCT<name STRING, val INT64>>
) AS (
  (SELECT SUM(IF(elem.name = "foo",elem.val,null)) FROM UNNEST(arr) AS elem)
)`,
			expectedRows: [][]interface{}{},
		},
		{
			name: "use function",
			query: `
SELECT customfunc([
  STRUCT<name STRING, val INT64>("foo", 10),
  STRUCT<name STRING, val INT64>("bar", 40),
  STRUCT<name STRING, val INT64>("foo", 20)
])`,
			expectedRows: [][]interface{}{{int64(30)}},
		},
		{
			name: "out of range error",
			query: `
WITH Items AS (SELECT ["coffee", "tea", "milk"] AS item_array)
SELECT
  item_array[OFFSET(6)] AS item_offset
FROM Items`,
			expectedRows: [][]interface{}{},
			expectedErr:  true,
		},
		// INVALID_ARGUMENT: Subscript access using [INT64] is not supported on values of type JSON [at 2:34]
		//{
		//	name: "json",
		//	query: `
		//	SELECT json_value.class.students[0]['name'] AS first_student
		//	FROM
		//	  UNNEST(
		//	    [
		//	      JSON '{"class" : {"students" : [{"name" : "Jane"}]}}',
		//	      JSON '{"class" : {"students" : []}}',
		//	      JSON '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'])
		//	    AS json_value`,
		//	expectedRows: [][]interface{}{
		//		{"Jane"},
		//		{nil},
		//		{"John"},
		//	},
		//},
		{
			name:         "date operator",
			query:        `SELECT DATE "2020-09-22" + 1 AS day_later, DATE "2020-09-22" - 7 AS week_ago`,
			expectedRows: [][]interface{}{{"2020-09-23", "2020-09-15"}},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			rows, err := db.Query(test.query, test.args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if len(test.expectedRows) == 0 {
				return
			}
			columnNum := len(test.expectedRows[0])
			args := []interface{}{}
			for i := 0; i < columnNum; i++ {
				var v interface{}
				args = append(args, &v)
			}
			rowNum := 0
			for rows.Next() {
				if err := rows.Scan(args...); err != nil {
					t.Fatal(err)
				}
				expectedRow := test.expectedRows[rowNum]
				if len(args) != len(expectedRow) {
					t.Fatalf("failed to get columns. expected %d but got %d", len(expectedRow), len(args))
				}
				for i := 0; i < len(args); i++ {
					value := reflect.ValueOf(args[i]).Elem().Interface()
					if diff := cmp.Diff(expectedRow[i], value); diff != "" {
						t.Errorf("(-want +got):\n%s", diff)
					}
				}
				rowNum++
			}
			rowsErr := rows.Err()
			if test.expectedErr {
				if rowsErr == nil {
					t.Fatal("expected error")
				}
			} else {
				if rowsErr != nil {
					t.Fatal(rowsErr)
				}
			}
			if len(test.expectedRows) != rowNum {
				t.Fatalf("failed to get rows. expected %d but got %d", len(test.expectedRows), rowNum)
			}
		})
	}
}
