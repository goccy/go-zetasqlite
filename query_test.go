package zetasqlite_test

import (
	"context"
	"database/sql"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/goccy/go-zetasqlite"
	"github.com/google/go-cmp/cmp"
)

func TestQuery(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	ctx = zetasqlite.WithCurrentTime(ctx, now)
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	floatCmpOpt := cmp.Comparer(func(x, y float64) bool {
		delta := math.Abs(x - y)
		mean := math.Abs(x+y) / 2.0
		return delta/mean < 0.00001
	})
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
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "lt operator",
			query:        "SELECT 10 < 100",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "gt operator",
			query:        "SELECT 100 > 10",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "lte operator",
			query:        "SELECT 10 <= 10",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "gte operator",
			query:        "SELECT 10 >= 10",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "ne operator",
			query:        "SELECT 100 != 10",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "like operator",
			query:        `SELECT "abcd" LIKE "a%d"`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "not like operator",
			query:        `SELECT "abcd" NOT LIKE "a%d"`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "between operator",
			query:        `SELECT DATE "2022-09-10" BETWEEN "2022-09-01" and "2022-10-01"`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "not between operator",
			query:        `SELECT DATE "2020-09-10" NOT BETWEEN "2022-09-01" and "2022-10-01"`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "in operator",
			query:        `SELECT 3 IN (1, 2, 3, 4)`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "not in operator",
			query:        `SELECT 5 NOT IN (1, 2, 3, 4)`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is null operator",
			query:        `SELECT NULL IS NULL`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is not null operator",
			query:        `SELECT 1 IS NOT NULL`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is true operator",
			query:        `SELECT true IS TRUE`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is not true operator",
			query:        `SELECT false IS NOT TRUE`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is false operator",
			query:        `SELECT false IS FALSE`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is not false operator",
			query:        `SELECT true IS NOT FALSE`,
			expectedRows: [][]interface{}{{true}},
		},
		// priority 10 operator
		{
			name:         "not operator",
			query:        `SELECT NOT 1 = 2`,
			expectedRows: [][]interface{}{{true}},
		},
		// priority 11 operator
		{
			name:         "and operator",
			query:        `SELECT 1 = 1 AND 2 = 2`,
			expectedRows: [][]interface{}{{true}},
		},
		// priority 12 operator
		{
			name:         "or operator",
			query:        `SELECT 1 = 2 OR 1 = 1`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "exists",
			query:        `SELECT EXISTS ( SELECT val FROM UNNEST([1, 2, 3]) AS val WHERE val = 1 )`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "not exists",
			query:        `SELECT EXISTS ( SELECT val FROM UNNEST([1, 2, 3]) AS val WHERE val = 4 )`,
			expectedRows: [][]interface{}{{false}},
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
			expectedRows: [][]interface{}{{nil}},
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
		{
			name:  "array_agg",
			query: `SELECT ARRAY_AGG(x) AS array_agg FROM UNNEST([2, 1,-2, 3, -2, 1, 2]) AS x`,
			expectedRows: [][]interface{}{{
				[]int64{2, 1, -2, 3, -2, 1, 2},
			}},
		},
		{
			name:  "array_agg with distinct",
			query: `SELECT ARRAY_AGG(DISTINCT x) AS array_agg FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x`,
			expectedRows: [][]interface{}{{
				[]int64{2, 1, -2, 3},
			}},
		},
		{
			name:  "array_agg with limit",
			query: `SELECT ARRAY_AGG(x LIMIT 5) AS array_agg FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x`,
			expectedRows: [][]interface{}{{
				[]int64{2, 1, -2, 3, -2},
			}},
		},
		{
			name:  "array_agg with ignore nulls",
			query: `SELECT ARRAY_AGG(x IGNORE NULLS) AS array_agg FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x`,
			expectedRows: [][]interface{}{{
				[]int64{1, -2, 3, -2, 1},
			}},
		},
		{
			name:  "array_agg with abs",
			query: `SELECT ARRAY_AGG(x ORDER BY ABS(x)) AS array_agg FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x`,
			expectedRows: [][]interface{}{{
				[]int64{1, 1, 2, -2, -2, 2, 3},
			}},
		},
		{
			name: "array_concat_agg",
			query: `
SELECT ARRAY_CONCAT_AGG(x) AS array_concat_agg FROM (
  SELECT [NULL, 1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
)`,
			expectedRows: [][]interface{}{{
				[]int64{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8), int64(9)},
			}},
		},
		{
			name: "array_concat_agg with format",
			query: `SELECT FORMAT("%T", ARRAY_CONCAT_AGG(x)) AS array_concat_agg FROM (
  SELECT [NULL, 1, 2, 3, 4] AS x
  UNION ALL SELECT NULL
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
)`,
			expectedRows: [][]interface{}{
				{"[NULL, 1, 2, 3, 4, 5, 6, 7, 8, 9]"},
			},
		},
		{
			name:         "avg",
			query:        `SELECT AVG(x) as avg FROM UNNEST([0, 2, 4, 4, 5]) as x`,
			expectedRows: [][]interface{}{{float64(3)}},
		},
		{
			name:         "avg with distinct",
			query:        `SELECT AVG(DISTINCT x) AS avg FROM UNNEST([0, 2, 4, 4, 5]) AS x`,
			expectedRows: [][]interface{}{{float64(2.75)}},
		},
		{
			name:         "bit_and",
			query:        `SELECT BIT_AND(x) as bit_and FROM UNNEST([0xF001, 0x00A1]) as x`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "bit_or",
			query:        `SELECT BIT_OR(x) as bit_or FROM UNNEST([0xF001, 0x00A1]) as x`,
			expectedRows: [][]interface{}{{int64(61601)}},
		},
		{
			name:         "bit_xor",
			query:        `SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([5678, 1234]) AS x`,
			expectedRows: [][]interface{}{{int64(4860)}},
		},
		{
			name:         "bit_xor 2",
			query:        `SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x`,
			expectedRows: [][]interface{}{{int64(5678)}},
		},
		{
			name:         "bit_xor 3",
			query:        `SELECT BIT_XOR(DISTINCT x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x`,
			expectedRows: [][]interface{}{{int64(4860)}},
		},
		{
			name:         "count",
			query:        `SELECT COUNT(*) AS count_star, COUNT(DISTINCT x) AS count_dist_x FROM UNNEST([1, 4, 4, 5]) AS x`,
			expectedRows: [][]interface{}{{int64(4), int64(3)}},
		},
		{
			name:         "count with if",
			query:        `SELECT COUNT(DISTINCT IF(x > 0, x, NULL)) AS distinct_positive FROM UNNEST([1, -2, 4, 1, -5, 4, 1, 3, -6, 1]) AS x`,
			expectedRows: [][]interface{}{{int64(3)}},
		},
		{
			name:         "countif",
			query:        `SELECT COUNTIF(x<0) AS num_negative, COUNTIF(x>0) AS num_positive FROM UNNEST([5, -2, 3, 6, -10, -7, 4, 0]) AS x`,
			expectedRows: [][]interface{}{{int64(3), int64(4)}},
		},
		{
			name:         "logical_and",
			query:        `SELECT LOGICAL_AND(x) AS logical_and FROM UNNEST([true, false, true]) AS x`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "logical_or",
			query:        `SELECT LOGICAL_OR(x) AS logical_or FROM UNNEST([true, false, true]) AS x`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "max",
			query:        `SELECT MAX(x) AS max FROM UNNEST([8, 37, 4, 55]) AS x`,
			expectedRows: [][]interface{}{{int64(55)}},
		},
		{
			name:         "min",
			query:        `SELECT MIN(x) AS min FROM UNNEST([8, 37, 4, 55]) AS x`,
			expectedRows: [][]interface{}{{int64(4)}},
		},
		{
			name:         "string_agg",
			query:        `SELECT STRING_AGG(fruit) AS string_agg FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"apple,pear,banana,pear"}},
		},
		{
			name:         "string_agg with delimiter",
			query:        `SELECT STRING_AGG(fruit, " & ") AS string_agg FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"apple & pear & banana & pear"}},
		},
		{
			name:         "string_agg with distinct",
			query:        `SELECT STRING_AGG(DISTINCT fruit, " & ") AS string_agg FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"apple & pear & banana"}},
		},
		{
			name:         "string_agg with order by",
			query:        `SELECT STRING_AGG(fruit, " & " ORDER BY LENGTH(fruit)) AS string_agg FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"pear & pear & apple & banana"}},
		},
		{
			name:         "string_agg with limit",
			query:        `SELECT STRING_AGG(fruit, " & " LIMIT 2) AS string_agg FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"apple & pear"}},
		},
		{
			name:         "string_agg with distinct and order by and limit",
			query:        `SELECT STRING_AGG(DISTINCT fruit, " & " ORDER BY fruit DESC LIMIT 2) AS string_agg FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"pear & banana"}},
		},
		{
			name:         "sum",
			query:        `SELECT SUM(x) AS sum FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x`,
			expectedRows: [][]interface{}{{int64(25)}},
		},
		{
			name:         "sum with distinct",
			query:        `SELECT SUM(DISTINCT x) AS sum FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x`,
			expectedRows: [][]interface{}{{int64(15)}},
		},
		{
			name:         "sum null",
			query:        `SELECT SUM(x) AS sum FROM UNNEST([]) AS x`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "null",
			query:        `SELECT NULL`,
			expectedRows: [][]interface{}{{nil}},
		},

		// window function
		{
			name: `window total`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, SUM(purchases)
  OVER () AS total_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", int64(54)},
				{"leek", int64(2), "vegetable", int64(54)},
				{"apple", int64(8), "fruit", int64(54)},
				{"cabbage", int64(9), "vegetable", int64(54)},
				{"lettuce", int64(10), "vegetable", int64(54)},
				{"kale", int64(23), "vegetable", int64(54)},
			},
		},
		{
			name: `window subtotal`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, SUM(purchases)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS total_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", int64(10)},
				{"apple", int64(8), "fruit", int64(10)},
				{"leek", int64(2), "vegetable", int64(44)},
				{"cabbage", int64(9), "vegetable", int64(44)},
				{"lettuce", int64(10), "vegetable", int64(44)},
				{"kale", int64(23), "vegetable", int64(44)},
			},
		},
		{
			name: `window cumulative`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, SUM(purchases)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS total_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", int64(2)},
				{"apple", int64(8), "fruit", int64(10)},
				{"leek", int64(2), "vegetable", int64(2)},
				{"cabbage", int64(9), "vegetable", int64(11)},
				{"lettuce", int64(10), "vegetable", int64(21)},
				{"kale", int64(23), "vegetable", int64(44)},
			},
		},
		{
			name: `window cumulative omit current row`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, SUM(purchases)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS UNBOUNDED PRECEDING
  ) AS total_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", int64(2)},
				{"apple", int64(8), "fruit", int64(10)},
				{"leek", int64(2), "vegetable", int64(2)},
				{"cabbage", int64(9), "vegetable", int64(11)},
				{"lettuce", int64(10), "vegetable", int64(21)},
				{"kale", int64(23), "vegetable", int64(44)},
			},
		},

		{
			name: `window offset`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, SUM(purchases)
  OVER (
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING
  ) AS total_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", nil},
				{"leek", int64(2), "vegetable", nil},
				{"apple", int64(8), "fruit", int64(2)},
				{"cabbage", int64(9), "vegetable", int64(4)},
				{"lettuce", int64(10), "vegetable", int64(12)},
				{"kale", int64(23), "vegetable", int64(21)},
			},
		},
		{
			name: `window avg`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, AVG(purchases)
  OVER (
    ORDER BY purchases
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS avg_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", float64(2)},
				{"leek", int64(2), "vegetable", float64(4)},
				{"apple", int64(8), "fruit", float64(6.333333333333333)},
				{"cabbage", int64(9), "vegetable", float64(9)},
				{"lettuce", int64(10), "vegetable", float64(14)},
				{"kale", int64(23), "vegetable", float64(16.5)},
			},
		},
		{
			name: `window last_value`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS most_popular
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", "apple"},
				{"apple", int64(8), "fruit", "apple"},
				{"leek", int64(2), "vegetable", "kale"},
				{"cabbage", int64(9), "vegetable", "kale"},
				{"lettuce", int64(10), "vegetable", "kale"},
				{"kale", int64(23), "vegetable", "kale"},
			},
		},
		{
			name: `window last_value with offset`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS most_popular
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", "apple"},
				{"apple", int64(8), "fruit", "apple"},
				{"leek", int64(2), "vegetable", "cabbage"},
				{"cabbage", int64(9), "vegetable", "lettuce"},
				{"lettuce", int64(10), "vegetable", "kale"},
				{"kale", int64(23), "vegetable", "kale"},
			},
		},
		{
			name: `window last_value with named window`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (
    item_window
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS most_popular
FROM Produce
WINDOW item_window AS (
  PARTITION BY category
  ORDER BY purchases)`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", "apple"},
				{"apple", int64(8), "fruit", "apple"},
				{"leek", int64(2), "vegetable", "cabbage"},
				{"cabbage", int64(9), "vegetable", "lettuce"},
				{"lettuce", int64(10), "vegetable", "kale"},
				{"kale", int64(23), "vegetable", "kale"},
			},
		},
		{
			name: "window range",
			query: `
WITH Farm AS
 (SELECT 'cat' as animal, 23 as population, 'mammal' as category
  UNION ALL SELECT 'duck', 3, 'bird'
  UNION ALL SELECT 'dog', 2, 'mammal'
  UNION ALL SELECT 'goose', 1, 'bird'
  UNION ALL SELECT 'ox', 2, 'mammal'
  UNION ALL SELECT 'goat', 2, 'mammal')
SELECT animal, population, category, COUNT(*)
  OVER (
    ORDER BY population
    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS similar_population
FROM Farm`,
			expectedRows: [][]interface{}{
				{"goose", int64(1), "bird", int64(4)},
				{"dog", int64(2), "mammal", int64(5)},
				{"ox", int64(2), "mammal", int64(5)},
				{"goat", int64(2), "mammal", int64(5)},
				{"duck", int64(3), "bird", int64(4)},
				{"cat", int64(23), "mammal", int64(1)},
			},
		},
		{
			name: "date type",
			query: `
WITH Employees AS
 (SELECT 'Isabella' as name, 2 as department, DATE(1997, 09, 28) as start_date
  UNION ALL SELECT 'Anthony', 1, DATE(1995, 11, 29)
  UNION ALL SELECT 'Daniel', 2, DATE(2004, 06, 24)
  UNION ALL SELECT 'Andrew', 1, DATE(1999, 01, 23)
  UNION ALL SELECT 'Jacob', 1, DATE(1990, 07, 11)
  UNION ALL SELECT 'Jose', 2, DATE(2013, 03, 17))
SELECT * FROM Employees`,
			expectedRows: [][]interface{}{
				{"Isabella", int64(2), "1997-09-28"},
				{"Anthony", int64(1), "1995-11-29"},
				{"Daniel", int64(2), "2004-06-24"},
				{"Andrew", int64(1), "1999-01-23"},
				{"Jacob", int64(1), "1990-07-11"},
				{"Jose", int64(2), "2013-03-17"},
			},
		},
		{
			name: "window rank",
			query: `
WITH Employees AS
 (SELECT 'Isabella' as name, 2 as department, DATE(1997, 09, 28) as start_date
  UNION ALL SELECT 'Anthony', 1, DATE(1995, 11, 29)
  UNION ALL SELECT 'Daniel', 2, DATE(2004, 06, 24)
  UNION ALL SELECT 'Andrew', 1, DATE(1999, 01, 23)
  UNION ALL SELECT 'Jacob', 1, DATE(1990, 07, 11)
  UNION ALL SELECT 'Jose', 2, DATE(2013, 03, 17))
SELECT name, department, start_date,
  RANK() OVER (PARTITION BY department ORDER BY start_date) AS rank
FROM Employees`,
			expectedRows: [][]interface{}{
				{"Jacob", int64(1), "1990-07-11", int64(1)},
				{"Anthony", int64(1), "1995-11-29", int64(2)},
				{"Andrew", int64(1), "1999-01-23", int64(3)},
				{"Isabella", int64(2), "1997-09-28", int64(1)},
				{"Daniel", int64(2), "2004-06-24", int64(2)},
				{"Jose", int64(2), "2013-03-17", int64(3)},
			},
		},
		{
			name: "rank with same order",
			query: `
WITH Numbers AS
 (SELECT 1 as x
  UNION ALL SELECT 2
  UNION ALL SELECT 2
  UNION ALL SELECT 5
  UNION ALL SELECT 8
  UNION ALL SELECT 10
  UNION ALL SELECT 10
)
SELECT x,
  RANK() OVER (ORDER BY x ASC) AS rank
FROM Numbers`,
			expectedRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(2), int64(2)},
				{int64(2), int64(2)},
				{int64(5), int64(4)},
				{int64(8), int64(5)},
				{int64(10), int64(6)},
				{int64(10), int64(6)},
			},
		},
		{
			name: "window dense_rank",
			query: `
WITH Numbers AS
 (SELECT 1 as x
  UNION ALL SELECT 2
  UNION ALL SELECT 2
  UNION ALL SELECT 5
  UNION ALL SELECT 8
  UNION ALL SELECT 10
  UNION ALL SELECT 10
)
SELECT x,
  DENSE_RANK() OVER (ORDER BY x ASC) AS dense_rank
FROM Numbers`,
			expectedRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(2), int64(2)},
				{int64(2), int64(2)},
				{int64(5), int64(3)},
				{int64(8), int64(4)},
				{int64(10), int64(5)},
				{int64(10), int64(5)},
			},
		},
		{
			name: "window dense_rank with group",
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  DENSE_RANK() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers
`,
			expectedRows: [][]interface{}{
				{"Sophia Liu", createTimeFromString("2016-10-18 09:51:45+00"), "F30-34", int64(1)},
				{"Nikki Leith", createTimeFromString("2016-10-18 09:59:01+00"), "F30-34", int64(2)},
				{"Meghan Lederer", createTimeFromString("2016-10-18 09:59:01+00"), "F30-34", int64(2)},
				{"Jen Edwards", createTimeFromString("2016-10-18 10:06:36+00"), "F30-34", int64(3)},
				{"Lisa Stelzner", createTimeFromString("2016-10-18 09:54:11+00"), "F35-39", int64(1)},
				{"Lauren Matthews", createTimeFromString("2016-10-18 10:01:17+00"), "F35-39", int64(2)},
				{"Desiree Berry", createTimeFromString("2016-10-18 10:05:42+00"), "F35-39", int64(3)},
				{"Suzy Slane", createTimeFromString("2016-10-18 10:06:24+00"), "F35-39", int64(4)},
			},
		},
		{
			name: "window lag",
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41+00', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58+00', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14+00', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS preceding_runner
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Carly Forte", createTimeFromString("2016-10-18 03:08:58+00"), "F25-29", nil},
				{"Sophia Liu", createTimeFromString("2016-10-18 02:51:45+00"), "F30-34", nil},
				{"Nikki Leith", createTimeFromString("2016-10-18 02:59:01+00"), "F30-34", "Sophia Liu"},
				{"Jen Edwards", createTimeFromString("2016-10-18 03:06:36+00"), "F30-34", "Nikki Leith"},
				{"Meghan Lederer", createTimeFromString("2016-10-18 03:07:41+00"), "F30-34", "Jen Edwards"},
				{"Lauren Reasoner", createTimeFromString("2016-10-18 03:10:14+00"), "F30-34", "Meghan Lederer"},
				{"Lisa Stelzner", createTimeFromString("2016-10-18 02:54:11+00"), "F35-39", nil},
				{"Lauren Matthews", createTimeFromString("2016-10-18 03:01:17+00"), "F35-39", "Lisa Stelzner"},
				{"Desiree Berry", createTimeFromString("2016-10-18 03:05:42+00"), "F35-39", "Lauren Matthews"},
				{"Suzy Slane", createTimeFromString("2016-10-18 03:06:24+00"), "F35-39", "Desiree Berry"},
			},
		},
		{
			name: "window lag with offset",
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41+00', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58+00', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14+00', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name, 2)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_ahead
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Carly Forte", createTimeFromString("2016-10-18 03:08:58+00"), "F25-29", nil},
				{"Sophia Liu", createTimeFromString("2016-10-18 02:51:45+00"), "F30-34", nil},
				{"Nikki Leith", createTimeFromString("2016-10-18 02:59:01+00"), "F30-34", nil},
				{"Jen Edwards", createTimeFromString("2016-10-18 03:06:36+00"), "F30-34", "Sophia Liu"},
				{"Meghan Lederer", createTimeFromString("2016-10-18 03:07:41+00"), "F30-34", "Nikki Leith"},
				{"Lauren Reasoner", createTimeFromString("2016-10-18 03:10:14+00"), "F30-34", "Jen Edwards"},
				{"Lisa Stelzner", createTimeFromString("2016-10-18 02:54:11+00"), "F35-39", nil},
				{"Lauren Matthews", createTimeFromString("2016-10-18 03:01:17+00"), "F35-39", nil},
				{"Desiree Berry", createTimeFromString("2016-10-18 03:05:42+00"), "F35-39", "Lisa Stelzner"},
				{"Suzy Slane", createTimeFromString("2016-10-18 03:06:24+00"), "F35-39", "Lauren Matthews"},
			},
		},
		{
			name: "window lag with offset and default value",
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41+00', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58+00', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14+00', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name, 2, 'NoBody')
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_ahead
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Carly Forte", createTimeFromString("2016-10-18 03:08:58+00"), "F25-29", "NoBody"},
				{"Sophia Liu", createTimeFromString("2016-10-18 02:51:45+00"), "F30-34", "NoBody"},
				{"Nikki Leith", createTimeFromString("2016-10-18 02:59:01+00"), "F30-34", "NoBody"},
				{"Jen Edwards", createTimeFromString("2016-10-18 03:06:36+00"), "F30-34", "Sophia Liu"},
				{"Meghan Lederer", createTimeFromString("2016-10-18 03:07:41+00"), "F30-34", "Nikki Leith"},
				{"Lauren Reasoner", createTimeFromString("2016-10-18 03:10:14+00"), "F30-34", "Jen Edwards"},
				{"Lisa Stelzner", createTimeFromString("2016-10-18 02:54:11+00"), "F35-39", "NoBody"},
				{"Lauren Matthews", createTimeFromString("2016-10-18 03:01:17+00"), "F35-39", "NoBody"},
				{"Desiree Berry", createTimeFromString("2016-10-18 03:05:42+00"), "F35-39", "Lisa Stelzner"},
				{"Suzy Slane", createTimeFromString("2016-10-18 03:06:24+00"), "F35-39", "Lauren Matthews"},
			},
		},
		{
			name:  "sign",
			query: `SELECT SIGN(25) UNION ALL SELECT SIGN(0) UNION ALL SELECT SIGN(-25)`,
			expectedRows: [][]interface{}{
				{int64(1)}, {int64(0)}, {int64(-1)},
			},
		},
		{
			name:  "current_date",
			query: `SELECT CURRENT_DATE()`,
			expectedRows: [][]interface{}{
				{now.Format("2006-01-02")},
			},
		},
		{
			name:  "current_datetime",
			query: `SELECT CURRENT_DATETIME()`,
			expectedRows: [][]interface{}{
				{now.Format("2006-01-02T15:04:05")},
			},
		},
		{
			name:  "current_time",
			query: `SELECT CURRENT_TIME()`,
			expectedRows: [][]interface{}{
				{now.Format("15:04:05")},
			},
		},
		{
			name:  "current_timestamp",
			query: `SELECT CURRENT_TIMESTAMP()`,
			expectedRows: [][]interface{}{
				{now.UTC()},
			},
		},
		// INVALID_ARGUMENT: No matching signature for operator - for argument types: TIMESTAMP, TIMESTAMP. Supported signatures: INT64 - INT64; NUMERIC - NUMERIC; FLOAT64 - FLOAT64; DATE - INT64 [at 1:8]
		//{
		//	name:  "interval",
		//	query: `SELECT TIMESTAMP "2021-06-01 12:34:56.789" - TIMESTAMP "2021-05-31 00:00:00" AS time_diff`,
		//	expectedRows: [][]interface{}{
		//		{"0-0 396 0:0:0", "0-0 0 36:34:56.789"},
		//	},
		//},

		// array functions
		{
			name:  "array function",
			query: `SELECT ARRAY (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS new_array`,
			expectedRows: [][]interface{}{
				{[]int64{1, 2, 3}},
			},
		},
		{
			name:  "array function with struct",
			query: `SELECT ARRAY (SELECT AS STRUCT 1, 2, 3 UNION ALL SELECT AS STRUCT 4, 5, 6) AS new_array`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						map[string]interface{}{
							"_field_1": float64(1),
							"_field_2": float64(2),
							"_field_3": float64(3),
						},
						map[string]interface{}{
							"_field_1": float64(4),
							"_field_2": float64(5),
							"_field_3": float64(6),
						},
					},
				},
			},
		},
		{
			name:  "array function with multiple array",
			query: `SELECT ARRAY (SELECT AS STRUCT [1, 2, 3] UNION ALL SELECT AS STRUCT [4, 5, 6]) AS new_array`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						map[string]interface{}{
							"_field_1": []interface{}{
								float64(1),
								float64(2),
								float64(3),
							},
						},
						map[string]interface{}{
							"_field_1": []interface{}{
								float64(4),
								float64(5),
								float64(6),
							},
						},
					},
				},
			},
		},
		{
			name:  "array_concat function",
			query: `SELECT ARRAY_CONCAT([1, 2], [3, 4], [5, 6]) as count_to_six`,
			expectedRows: [][]interface{}{
				{
					[]int64{1, 2, 3, 4, 5, 6},
				},
			},
		},
		{
			name:         "array_length function",
			query:        `SELECT ARRAY_LENGTH([1, 2, 3, 4]) as length`,
			expectedRows: [][]interface{}{{int64(4)}},
		},
		{
			name: "array_to_string function",
			query: `
WITH items AS
  (SELECT ['coffee', 'tea', 'milk' ] as list
  UNION ALL
  SELECT ['cake', 'pie', NULL] as list)
SELECT ARRAY_TO_STRING(list, '--') AS text FROM items`,
			expectedRows: [][]interface{}{
				{"coffee--tea--milk"},
				{"cake--pie"},
			},
		},
		{
			name: "array_to_string function with null text",
			query: `
WITH items AS
  (SELECT ['coffee', 'tea', 'milk' ] as list
  UNION ALL
  SELECT ['cake', 'pie', NULL] as list)

SELECT ARRAY_TO_STRING(list, '--', 'MISSING') AS text FROM items`,
			expectedRows: [][]interface{}{
				{"coffee--tea--milk"},
				{"cake--pie--MISSING"},
			},
		},
		{
			name:         "generate_array function",
			query:        `SELECT GENERATE_ARRAY(1, 5) AS example_array`,
			expectedRows: [][]interface{}{{[]int64{1, 2, 3, 4, 5}}},
		},
		{
			name:         "generate_array function with step",
			query:        `SELECT GENERATE_ARRAY(0, 10, 3) AS example_array`,
			expectedRows: [][]interface{}{{[]int64{0, 3, 6, 9}}},
		},
		{
			name:         "generate_array function with negative step value",
			query:        `SELECT GENERATE_ARRAY(10, 0, -3) AS example_array`,
			expectedRows: [][]interface{}{{[]int64{10, 7, 4, 1}}},
		},
		{
			name:         "generate_array function with large step value",
			query:        `SELECT GENERATE_ARRAY(4, 4, 10) AS example_array`,
			expectedRows: [][]interface{}{{[]int64{4}}},
		},
		{
			name:         "generate_array function with over step value",
			query:        `SELECT GENERATE_ARRAY(10, 0, 3) AS example_array`,
			expectedRows: [][]interface{}{{[]int64{}}},
		},
		{
			name:         "generate_array function with null",
			query:        `SELECT GENERATE_ARRAY(5, NULL, 1) AS example_array`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:  "generate_array function for generate multiple array",
			query: `SELECT GENERATE_ARRAY(start, 5) AS example_array FROM UNNEST([3, 4, 5]) AS start`,
			expectedRows: [][]interface{}{
				{[]int64{3, 4, 5}},
				{[]int64{4, 5}},
				{[]int64{5}},
			},
		},
		{
			name:  "generate_date_array function",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08') AS example`,
			expectedRows: [][]interface{}{
				{[]string{"2016-10-05", "2016-10-06", "2016-10-07", "2016-10-08"}},
			},
		},
		{
			name:  "generate_date_array function with step",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-09', INTERVAL 2 DAY) AS example`,
			expectedRows: [][]interface{}{
				{[]string{"2016-10-05", "2016-10-07", "2016-10-09"}},
			},
		},
		{
			name:  "generate_date_array function with negative step",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-01', INTERVAL -3 DAY) AS example`,
			expectedRows: [][]interface{}{
				{[]string{"2016-10-05", "2016-10-02"}},
			},
		},
		{
			name:  "generate_date_array function with same value",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-05', INTERVAL 8 DAY) AS example`,
			expectedRows: [][]interface{}{
				{[]string{"2016-10-05"}},
			},
		},
		{
			name:  "generate_date_array function with over step",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-01', INTERVAL 1 DAY) AS example`,
			expectedRows: [][]interface{}{
				{[]string{}},
			},
		},
		{
			name:  "generate_date_array function with null",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', NULL) AS example`,
			expectedRows: [][]interface{}{
				{nil},
			},
		},
		{
			name:  "generate_date_array function with month",
			query: `SELECT GENERATE_DATE_ARRAY('2016-01-01', '2016-12-31', INTERVAL 2 MONTH) AS example`,
			expectedRows: [][]interface{}{
				{[]string{"2016-01-01", "2016-03-01", "2016-05-01", "2016-07-01", "2016-09-01", "2016-11-01"}},
			},
		},
		{
			name: "generate_date_array function with variable",
			query: `
SELECT GENERATE_DATE_ARRAY(date_start, date_end, INTERVAL 1 WEEK) AS date_range
FROM (
  SELECT DATE '2016-01-01' AS date_start, DATE '2016-01-31' AS date_end
  UNION ALL SELECT DATE "2016-04-01", DATE "2016-04-30"
  UNION ALL SELECT DATE "2016-07-01", DATE "2016-07-31"
  UNION ALL SELECT DATE "2016-10-01", DATE "2016-10-31"
) AS items`,
			expectedRows: [][]interface{}{
				{[]string{"2016-01-01", "2016-01-08", "2016-01-15", "2016-01-22", "2016-01-29"}},
				{[]string{"2016-04-01", "2016-04-08", "2016-04-15", "2016-04-22", "2016-04-29"}},
				{[]string{"2016-07-01", "2016-07-08", "2016-07-15", "2016-07-22", "2016-07-29"}},
				{[]string{"2016-10-01", "2016-10-08", "2016-10-15", "2016-10-22", "2016-10-29"}},
			},
		},
		{
			name:  "generate_timestamp_array function",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY(TIMESTAMP '2016-10-05 00:00:00+00', '2016-10-07 00:00:00+00', INTERVAL 1 DAY) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{
					[]time.Time{
						createTimeFromString("2016-10-05 00:00:00+00"),
						createTimeFromString("2016-10-06 00:00:00+00"),
						createTimeFromString("2016-10-07 00:00:00+00"),
					},
				},
			},
		},
		{
			name:  "generate_timestamp_array function interval 1 second",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00+00', '2016-10-05 00:00:02+00', INTERVAL 1 SECOND) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{
					[]time.Time{
						createTimeFromString("2016-10-05 00:00:00+00"),
						createTimeFromString("2016-10-05 00:00:01+00"),
						createTimeFromString("2016-10-05 00:00:02+00"),
					},
				},
			},
		},
		{
			name:  "generate_timestamp_array function negative interval",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00+00', '2016-10-01 00:00:00+00', INTERVAL -2 DAY) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{
					[]time.Time{
						createTimeFromString("2016-10-06 00:00:00+00"),
						createTimeFromString("2016-10-04 00:00:00+00"),
						createTimeFromString("2016-10-02 00:00:00+00"),
					},
				},
			},
		},
		{
			name:  "generate_timestamp_array function same value",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00+00', '2016-10-05 00:00:00+00', INTERVAL 1 HOUR) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{
					[]time.Time{
						createTimeFromString("2016-10-05 00:00:00+00"),
					},
				},
			},
		},
		{
			name:  "generate_timestamp_array function over step",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00+00', '2016-10-05 00:00:00+00', INTERVAL 1 HOUR) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{[]time.Time{}},
			},
		},
		{
			name:  "generate_timestamp_array function with null",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00+00', NULL, INTERVAL 1 HOUR) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{nil},
			},
		},
		{
			name: "generate_timestamp_array function with variable",
			query: `
SELECT GENERATE_TIMESTAMP_ARRAY(start_timestamp, end_timestamp, INTERVAL 1 HOUR)
  AS timestamp_array
FROM
  (SELECT
    TIMESTAMP '2016-10-05 00:00:00+00' AS start_timestamp,
    TIMESTAMP '2016-10-05 02:00:00+00' AS end_timestamp
   UNION ALL
   SELECT
    TIMESTAMP '2016-10-05 12:00:00+00' AS start_timestamp,
    TIMESTAMP '2016-10-05 14:00:00+00' AS end_timestamp
   UNION ALL
   SELECT
    TIMESTAMP '2016-10-05 23:59:00+00' AS start_timestamp,
    TIMESTAMP '2016-10-06 01:59:00+00' AS end_timestamp)`,
			expectedRows: [][]interface{}{
				{
					[]time.Time{
						createTimeFromString("2016-10-05 00:00:00+00"),
						createTimeFromString("2016-10-05 01:00:00+00"),
						createTimeFromString("2016-10-05 02:00:00+00"),
					},
				},
				{
					[]time.Time{
						createTimeFromString("2016-10-05 12:00:00+00"),
						createTimeFromString("2016-10-05 13:00:00+00"),
						createTimeFromString("2016-10-05 14:00:00+00"),
					},
				},
				{
					[]time.Time{
						createTimeFromString("2016-10-05 23:59:00+00"),
						createTimeFromString("2016-10-06 00:59:00+00"),
						createTimeFromString("2016-10-06 01:59:00+00"),
					},
				},
			},
		},
		{
			name: "array_reverse function",
			query: `
WITH example AS (
  SELECT [1, 2, 3] AS arr UNION ALL
  SELECT [4, 5] AS arr UNION ALL
  SELECT [] AS arr
) SELECT ARRAY_REVERSE(arr) AS reverse_arr FROM example`,
			expectedRows: [][]interface{}{
				{[]int64{3, 2, 1}},
				{[]int64{5, 4}},
				{[]int64{}},
			},
		},
		{
			name: "group by",
			query: `
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  day,
  SUM(price) AS total
FROM Sales
GROUP BY day`,
			expectedRows: [][]interface{}{
				{int64(1), float64(23.54)},
				{int64(2), float64(9.99)},
				{int64(3), float64(6.24)},
			},
		},
		{
			name: "group by rollup with one column",
			query: `
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  day,
  SUM(price) AS total
FROM Sales
GROUP BY ROLLUP(day)`,
			expectedRows: [][]interface{}{
				{nil, float64(39.77)},
				{int64(1), float64(23.54)},
				{int64(2), float64(9.99)},
				{int64(3), float64(6.24)},
			},
		},
		{
			name: "group by rollup with two columns",
			query: `
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  sku,
  day,
  SUM(price) AS total
FROM Sales
GROUP BY ROLLUP(sku, day)
ORDER BY sku, day`,
			expectedRows: [][]interface{}{
				{nil, nil, float64(39.77)},
				{int64(123), nil, float64(28.97)},
				{int64(123), int64(1), float64(18.98)},
				{int64(123), int64(2), float64(9.99)},
				{int64(456), nil, float64(8.81)},
				{int64(456), int64(1), float64(4.56)},
				{int64(456), int64(3), float64(4.25)},
				{int64(789), nil, float64(1.99)},
				{int64(789), int64(3), float64(1.99)},
			},
		},
		{
			name: "group by having",
			query: `
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 2, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  day,
  SUM(price) AS total
FROM Sales
GROUP BY day HAVING SUM(price) > 10`,
			expectedRows: [][]interface{}{
				{int64(1), float64(23.54)},
				{int64(2), float64(10.99)},
			},
		},
		{
			name:  "order by",
			query: `SELECT x, y FROM (SELECT 1 AS x, true AS y UNION ALL SELECT 9, true UNION ALL SELECT NULL, false) ORDER BY x`,
			expectedRows: [][]interface{}{
				{nil, false},
				{int64(1), true},
				{int64(9), true},
			},
		},
		{
			name:  "order by with nulls last",
			query: `SELECT x, y FROM (SELECT 1 AS x, true AS y UNION ALL SELECT 9, true UNION ALL SELECT NULL, false) ORDER BY x NULLS LAST`,
			expectedRows: [][]interface{}{
				{int64(1), true},
				{int64(9), true},
				{nil, false},
			},
		},
		{
			name:  "order by desc",
			query: `SELECT x, y FROM (SELECT 1 AS x, true AS y UNION ALL SELECT 9, true UNION ALL SELECT NULL, false) ORDER BY x DESC`,
			expectedRows: [][]interface{}{
				{int64(9), true},
				{int64(1), true},
				{nil, false},
			},
		},
		{
			name:  "order by nulls first",
			query: `SELECT x, y FROM (SELECT 1 AS x, true AS y UNION ALL SELECT 9, true UNION ALL SELECT NULL, false) ORDER BY x DESC NULLS FIRST`,
			expectedRows: [][]interface{}{
				{nil, false},
				{int64(9), true},
				{int64(1), true},
			},
		},
		{
			name: "inner join with using",
			query: `
WITH Roster AS
 (SELECT 'Adams' as LastName, 50 as SchoolID UNION ALL
  SELECT 'Buchanan', 52 UNION ALL
  SELECT 'Coolidge', 52 UNION ALL
  SELECT 'Davis', 51 UNION ALL
  SELECT 'Eisenhower', 77),
 TeamMascot AS
 (SELECT 50 as SchoolID, 'Jaguars' as Mascot UNION ALL
  SELECT 51, 'Knights' UNION ALL
  SELECT 52, 'Lakers' UNION ALL
  SELECT 53, 'Mustangs')
SELECT * FROM Roster INNER JOIN TeamMascot USING (SchoolID)
`,
			expectedRows: [][]interface{}{
				{int64(50), "Adams", "Jaguars"},
				{int64(52), "Buchanan", "Lakers"},
				{int64(52), "Coolidge", "Lakers"},
				{int64(51), "Davis", "Knights"},
			},
		},
		{
			name: "left join",
			query: `
WITH Roster AS
 (SELECT 'Adams' as LastName, 50 as SchoolID UNION ALL
  SELECT 'Buchanan', 52 UNION ALL
  SELECT 'Coolidge', 52 UNION ALL
  SELECT 'Davis', 51 UNION ALL
  SELECT 'Eisenhower', 77),
 TeamMascot AS
 (SELECT 50 as SchoolID, 'Jaguars' as Mascot UNION ALL
  SELECT 51, 'Knights' UNION ALL
  SELECT 52, 'Lakers' UNION ALL
  SELECT 53, 'Mustangs')
SELECT Roster.LastName, TeamMascot.Mascot FROM Roster LEFT JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID
`,
			expectedRows: [][]interface{}{
				{"Adams", "Jaguars"},
				{"Buchanan", "Lakers"},
				{"Coolidge", "Lakers"},
				{"Davis", "Knights"},
				{"Eisenhower", nil},
			},
		},
		{
			name: "right join",
			query: `
WITH Roster AS
 (SELECT 'Adams' as LastName, 50 as SchoolID UNION ALL
  SELECT 'Buchanan', 52 UNION ALL
  SELECT 'Coolidge', 52 UNION ALL
  SELECT 'Davis', 51 UNION ALL
  SELECT 'Eisenhower', 77),
 TeamMascot AS
 (SELECT 50 as SchoolID, 'Jaguars' as Mascot UNION ALL
  SELECT 51, 'Knights' UNION ALL
  SELECT 52, 'Lakers' UNION ALL
  SELECT 53, 'Mustangs')
SELECT Roster.LastName, TeamMascot.Mascot FROM Roster RIGHT JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID
`,
			expectedRows: [][]interface{}{
				{"Adams", "Jaguars"},
				{"Buchanan", "Lakers"},
				{"Coolidge", "Lakers"},
				{"Davis", "Knights"},
				{nil, "Mustangs"},
			},
		},
		{
			name: "full join",
			query: `
WITH Roster AS
 (SELECT 'Adams' as LastName, 50 as SchoolID UNION ALL
  SELECT 'Buchanan', 52 UNION ALL
  SELECT 'Coolidge', 52 UNION ALL
  SELECT 'Davis', 51 UNION ALL
  SELECT 'Eisenhower', 77),
 TeamMascot AS
 (SELECT 50 as SchoolID, 'Jaguars' as Mascot UNION ALL
  SELECT 51, 'Knights' UNION ALL
  SELECT 52, 'Lakers' UNION ALL
  SELECT 53, 'Mustangs')
SELECT Roster.LastName, TeamMascot.Mascot FROM Roster FULL JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID
`,
			expectedRows: [][]interface{}{
				{"Adams", "Jaguars"},
				{"Buchanan", "Lakers"},
				{"Coolidge", "Lakers"},
				{"Davis", "Knights"},
				{"Eisenhower", nil},
				{nil, "Mustangs"},
			},
		},
		{
			name: "qualify",
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT
  item,
  RANK() OVER (PARTITION BY category ORDER BY purchases DESC) as rank
FROM Produce WHERE Produce.category = 'vegetable' QUALIFY rank <= 3`,
			expectedRows: [][]interface{}{
				{"kale", int64(1)},
				{"lettuce", int64(2)},
				{"cabbage", int64(3)},
			},
		},
		{
			name: "qualify direct",
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item FROM Produce WHERE Produce.category = 'vegetable' QUALIFY RANK() OVER (PARTITION BY category ORDER BY purchases DESC) <= 3`,
			expectedRows: [][]interface{}{
				{"kale"},
				{"lettuce"},
				{"cabbage"},
			},
		},
		{
			name:        "invalid cast",
			query:       `SELECT CAST("apple" AS INT64) AS not_a_number`,
			expectedErr: true,
		},
		{
			name:         "safe cast for invalid cast",
			query:        `SELECT SAFE_CAST("apple" AS INT64) AS not_a_number`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "parse date with %A %b %e %Y",
			query:        `SELECT PARSE_DATE("%A %b %e %Y", "Thursday Dec 25 2008")`,
			expectedRows: [][]interface{}{{"2008-12-25"}},
		},
		{
			name:         "parse date with %Y%m%d",
			query:        `SELECT PARSE_DATE("%Y%m%d", "20081225") AS parsed`,
			expectedRows: [][]interface{}{{"2008-12-25"}},
		},
		{
			name:         "parse date with %F",
			query:        `SELECT PARSE_DATE("%F", "2008-12-25") AS parsed`,
			expectedRows: [][]interface{}{{"2008-12-25"}},
		},
		{
			name:         "parse date with %x",
			query:        `SELECT PARSE_DATE("%x", "12/25/08") AS parsed`,
			expectedRows: [][]interface{}{{"2008-12-25"}},
		},
		{
			name:        "parse date ( the year element is in different locations )",
			query:       `SELECT PARSE_DATE("%Y %A %b %e", "Thursday Dec 25 2008")`,
			expectedErr: true,
		},
		{
			name:        "parse date ( one of the year elements is missing )",
			query:       `SELECT PARSE_DATE("%A %b %e", "Thursday Dec 25 2008")`,
			expectedErr: true,
		},
		{
			name:         "parse datetime",
			query:        `SELECT PARSE_DATETIME("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008")`,
			expectedRows: [][]interface{}{{"2008-12-25T07:30:00"}},
		},
		{
			name:         "parse datetime with %c",
			query:        `SELECT PARSE_DATETIME("%c", "Thu Dec 25 07:30:00 2008")`,
			expectedRows: [][]interface{}{{"2008-12-25T07:30:00"}},
		},
		{
			name:        "parse datetime ( the year element is in different locations )",
			query:       `SELECT PARSE_DATETIME("%a %b %e %Y %I:%M:%S", "Thu Dec 25 07:30:00 2008")`,
			expectedErr: true,
		},
		{
			name:        "parse datetime ( one of the year elements is missing )",
			query:       `SELECT PARSE_DATETIME("%a %b %e %I:%M:%S", "Thu Dec 25 07:30:00 2008")`,
			expectedErr: true,
		},
		{
			name:         "parse time with %I:%M:%S",
			query:        `SELECT PARSE_TIME("%I:%M:%S", "07:30:00")`,
			expectedRows: [][]interface{}{{"07:30:00"}},
		},
		{
			name:         "parse time with %T",
			query:        `SELECT PARSE_TIME("%T", "07:30:00")`,
			expectedRows: [][]interface{}{{"07:30:00"}},
		},
		{
			name:        "parse time ( the seconds element is in different locations )",
			query:       `SELECT PARSE_TIME("%S:%I:%M", "07:30:00")`,
			expectedErr: true,
		},
		{
			name:        "parse time ( one of the seconds elements is missing )",
			query:       `SELECT PARSE_TIME("%I:%M", "07:30:00")`,
			expectedErr: true,
		},
		{
			name:         "parse timestamp with %a %b %e %I:%M:%S %Y",
			query:        `SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008")`,
			expectedRows: [][]interface{}{{createTimeFromString("2008-12-25 07:30:00+00")}},
		},
		{
			name:         "parse timestamp with %c",
			query:        `SELECT PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008")`,
			expectedRows: [][]interface{}{{createTimeFromString("2008-12-25 07:30:00+00")}},
		},
		{
			name:        "parse timestamp ( the year element is in different locations )",
			query:       `SELECT PARSE_TIMESTAMP("%a %b %e %Y %I:%M:%S", "Thu Dec 25 07:30:00 2008")`,
			expectedErr: true,
		},
		{
			name:        "parse timestamp ( one of the year elements is missing )",
			query:       `SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S", "Thu Dec 25 07:30:00 2008")`,
			expectedErr: true,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			rows, err := db.QueryContext(ctx, test.query, test.args...)
			if err != nil {
				if !test.expectedErr {
					t.Fatal(err)
				} else {
					return
				}
			}
			defer rows.Close()
			columns, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			columnNum := len(columns)
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
				derefArgs := []interface{}{}
				for i := 0; i < len(args); i++ {
					value := reflect.ValueOf(args[i]).Elem().Interface()
					derefArgs = append(derefArgs, value)
				}
				if len(test.expectedRows) <= rowNum {
					t.Fatalf("unexpected row %v", derefArgs)
				}
				expectedRow := test.expectedRows[rowNum]
				if len(derefArgs) != len(expectedRow) {
					t.Fatalf("failed to get columns. expected %d but got %d", len(expectedRow), len(derefArgs))
				}
				for i := 0; i < len(derefArgs); i++ {
					if diff := cmp.Diff(expectedRow[i], derefArgs[i], floatCmpOpt); diff != "" {
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

func createTimeFromString(v string) time.Time {
	t, _ := time.Parse("2006-01-02 15:04:05+00", v)
	return t
}
