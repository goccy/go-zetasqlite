# go-zetasqlite

![Go](https://github.com/goccy/go-zetasqlite/workflows/Go/badge.svg)
[![GoDoc](https://godoc.org/github.com/goccy/go-zetasqlite?status.svg)](https://pkg.go.dev/github.com/goccy/go-zetasqlite?tab=doc)

A database driver library that interprets ZetaSQL queries and runs them using SQLite3

# Features

`go-zetasqlite` supports `database/sql` driver interface.
So, you can use ZetaSQL queries just by importing `github.com/goccy/go-zetasqlite`.
Also, go-zetasqlite uses SQLite3 as the database engine.
Since we are using [go-sqlite3](https://github.com/mattn/go-sqlite3), we can use the options ( like `:memory:` ) supported by `go-sqlite3` ( see [details](https://pkg.go.dev/github.com/mattn/go-sqlite3#readme-connection-string) ).
ZetaSQL functionality is provided by [go-zetasql](https://github.com/goccy/go-zetasql)

# Installation

```
go get github.com/goccy/go-zetasqlite
```

## **NOTE**

Since this library uses go-zetasql, the following environment variables must be enabled in order to build. See [here](https://github.com/goccy/go-zetasql#prerequisites) for details.

```
CGO_ENABLED=1
CXX=clang++
```

# Synopsis

You can pass ZetaSQL queries to Query/Exec function of database/sql package.

```go
package main

import (
  "database/sql"
  "fmt"

  _ "github.com/goccy/go-zetasqlite"
)

func main() {
  db, err := sql.Open("zetasqlite", ":memory:")
  if err != nil {
    panic(err)
  }
  defer db.Close()

  rows, err := db.Query(`SELECT * FROM UNNEST([?, ?, ?])`, 1, 2, 3)
  if err != nil {
    panic(err)
  }
  var ids []int64
  for rows.Next() {
    var id int64
    if err := rows.Scan(&id); err != nil {
      panic(err)
    }
    ids = append(ids, id)
  }
  fmt.Println(ids) // [1 2 3]
}
```

# Status

A list of ZetaSQL specifications and features supported by go-zetasqlite.

## Types

- [x] INT64 ( `INT`, `SMALLINT`, `INTEGER`, `BIGINT`, `TINYINT`, `BYTEINT` )
- [ ] NUMERIC ( `DECIMAL` )
- [ ] BIGNUMERIC ( `BIGDECIMAL` )
- [x] FLOAT64 ( `FLOAT` )
- [x] BOOL ( `BOOLEAN` )
- [x] STRING
- [x] BYTES
- [x] DATE
- [x] TIME
- [x] DATETIME
- [x] TIMESTAMP
- [ ] INTERVAL
- [x] ARRAY
- [x] STRUCT
- [x] JSON
- [x] RECORD
- [ ] GEOGRAPHY

## Statements

- [x] SELECT
- [x] UPDATE
- [x] INSERT
- [x] DELETE
- [x] DROP
- [x] TRUNCATE
- [x] MERGE
- [x] BEGIN-END
- [x] BEGIN TRANSACTION
- [x] COMMIT TRANSACTION
- [x] CREATE TABLE
- [x] CREATE FUNCTION
- [x] CREATE TEMPORARY TABLE
- [x] CREATE TEMPORARY FUNCTION

## Standard SQL Features

### Operator precedence

- [x] Field access operator
- [x] Array subscript operator
- [ ] JSON subscript operator
- [x] Unary operators ( `+`, `-`, `~` )
- [x] Multiplication ( `*` )
- [x] Division ( `/` )
- [x] Concatenation operator ( `||` )
- [x] Addition ( `+` )
- [x] Subtraction ( `-` )
- [x] Bitwise operators ( `<<`, `>>`, `&`, `|` )
- [x] Comparison operators ( `=`, `<`, `>`, `<=`, `>=`, `!=`, `<>`)
- [x] [NOT] LIKE
- [x] [NOT] BETWEEN
- [x] [NOT] IN
- [x] IS [NOT] NULL
- [x] IS [NOT] TRUE
- [x] IS [NOT] FALSE
- [x] NOT
- [x] AND
- [x] OR
- [x] [NOT] EXISTS
- [x] IS [NOT] DISTINCT FROM

### Conditional expressions

- [x] CASE-WHEN
- [x] COALESCE
- [x] IF
- [x] IFNULL
- [x] NULLIF

### Other clauses

- [x] OVER
- [x] WINDOW
- [x] WITH
- [x] UNION
- [X] HAVING
- [x] ORDER BY
- [X] GROUP BY - ROLLUP
- [X] INNER/LEFT/RIGHT/FULL/CROSS JOIN
- [x] QUALIFY
- [x] EXCEPT
- [x] REPLACE

### Aggregate functions

- [x] ANY_VALUE
- [x] ARRAY_AGG
- [x] ARRAY_CONCAT_AGG
- [x] AVG
- [x] BIT_AND
- [x] BIT_OR
- [x] BIT_XOR
- [x] COUNT
- [x] COUNTIF
- [x] LOGICAL_AND
- [x] LOGICAL_OR
- [x] MAX
- [x] MIN
- [x] STRING_AGG
- [x] SUM

### Statistical aggregate functions

- [x] CORR
- [x] COVAR_POP
- [x] COVAR_SAMP
- [x] STDDEV_POP
- [x] STDDEV_SAMP
- [x] STDDEV
- [x] VAR_POP
- [x] VAR_SAMP
- [x] VARIANCE

### Approximate aggregate functions

- [ ] APPROX_COUNT_DISTINCT
- [ ] APPROX_QUANTILES
- [ ] APPROX_TOP_COUNT
- [ ] APPROX_TOP_SUM

### HyperLogLog++ functions

- [ ] HLL_COUNT.INIT
- [ ] HLL_COUNT.MERGE
- [ ] HLL_COUNT.MERGE_PARTIAL
- [ ] HLL_COUNT.EXTRACT

### Numbering functions

- [x] RANK
- [x] DENSE_RANK
- [ ] PERCENT_RANK
- [ ] CUME_DIST
- [ ] NTILE
- [x] ROW_NUMBER

### Bit functions

- [ ] BIT_COUNT

### Conversion functions

- [x] CAST AS ARRAY
- [ ] CAST AS BIGNUMERIC
- [x] CAST AS BOOL
- [x] CAST AS BYTES
- [x] CAST AS DATE
- [x] CAST AS DATETIME
- [x] CAST AS FLOAT64
- [x] CAST AS INT64
- [ ] CAST AS INTERVAL
- [ ] CAST AS NUMERIC
- [x] CAST AS STRING
- [x] CAST AS STRUCT
- [x] CAST AS TIME
- [x] CAST AS TIMESTAMP
- [ ] PARSE_BIGNUMERIC
- [ ] PARSE_NUMERIC
- [x] SAFE_CAST
- [ ] Format clause for CAST

### Mathematical functions

- [x] ABS
- [x] SIGN
- [x] IS_INF
- [x] IS_NAN
- [x] IEEE_DIVIDE
- [x] RAND
- [x] SQRT
- [x] POW
- [x] POWER
- [x] EXP
- [x] LN
- [x] LOG
- [x] LOG10
- [x] GREATEST
- [x] LEAST
- [x] DIV
- [x] SAFE_DIVIDE
- [x] SAFE_MULTIPLY
- [x] SAFE_NEGATE
- [x] SAFE_ADD
- [x] SAFE_SUBTRACT
- [x] MOD
- [x] ROUND
- [x] TRUNC
- [x] CEIL
- [x] CEILING
- [x] FLOOR
- [x] COS
- [x] COSH
- [x] ACOS
- [x] ACOSH
- [x] SIN
- [x] SINH
- [x] ASIN
- [x] ASINH
- [x] TAN
- [x] TANH
- [x] ATAN
- [x] ATANH
- [x] ATAN2
- [x] RANGE_BUCKET

### Navigation functions

- [x] FIRST_VALUE
- [x] LAST_VALUE
- [ ] NTH_VALUE
- [ ] LEAD
- [x] LAG
- [ ] PERCENTILE_CONT
- [ ] PERCENTILE_DISC

### Hash functions

- [x] FARM_FINGERPRINT
- [x] MD5
- [x] SHA1
- [x] SHA256
- [x] SHA512

### String functions

- [x] ASCII
- [x] BYTE_LENGTH
- [x] CHAR_LENGTH
- [x] CHARACTER_LENGTH
- [x] CHR
- [x] CODE_POINTS_TO_BYTES
- [x] CODE_POINTS_TO_STRING
- [ ] COLLATE
- [x] CONCAT
- [ ] CONTAINS_SUBSTR
- [x] ENDS_WITH
- [x] FORMAT
- [x] FROM_BASE32
- [x] FROM_BASE64
- [x] FROM_HEX
- [x] INITCAP
- [x] INSTR
- [x] LEFT
- [x] LENGTH
- [x] LPAD
- [x] LOWER
- [x] LTRIM
- [x] NORMALIZE
- [x] NORMALIZE_AND_CASEFOLD
- [x] OCTET_LENGTH
- [x] REGEXP_CONTAINS
- [x] REGEXP_EXTRACT
- [ ] REGEXP_EXTRACT_ALL
- [ ] REGEXP_INSTR
- [ ] REGEXP_REPLACE
- [ ] REGEXP_SUBSTR
- [ ] REPLACE
- [ ] REPEAT
- [ ] REVERSE
- [ ] RIGHT
- [ ] RPAD
- [ ] RTRIM
- [ ] SAFE_CONVERT_BYTES_TO_STRING
- [ ] SOUNDEX
- [ ] SPLIT
- [x] STARTS_WITH
- [x] STRPOS
- [x] SUBSTR
- [x] SUBSTRING
- [x] TO_BASE32
- [x] TO_BASE64
- [x] TO_CODE_POINTS
- [x] TO_HEX
- [x] TRANSALTE
- [x] TRIM
- [x] UNICODE
- [x] UPPER

### JSON functions

- [ ] JSON_EXTRACT
- [ ] JSON_QUERY
- [ ] JSON_EXTRACT_SCALAR
- [ ] JSON_VALUE
- [ ] JSON_EXTRACT_ARRAY
- [ ] JSON_QUERY_ARRAY
- [ ] JSON_EXTRACT_STRING_ARRAY
- [ ] JSON_VALUE_ARRAY
- [ ] PARSE_JSON
- [x] TO_JSON
- [x] TO_JSON_STRING
- [x] STRING
- [x] BOOL
- [x] INT64
- [x] FLOAT64
- [x] JSON_TYPE

### Array functions

- [x] ARRAY
- [x] ARRAY_CONCAT
- [x] ARRAY_LENGTH
- [x] ARRAY_TO_STRING
- [x] GENERATE_ARRAY
- [x] GENERATE_DATE_ARRAY
- [x] GENERATE_TIMESTAMP_ARRAY
- [x] ARRAY_REVERSE

### Date functions

- [x] CURRENT_DATE
- [x] EXTRACT
- [x] DATE
- [x] DATE_ADD
- [x] DATE_SUB
- [x] DATE_DIFF
- [x] DATE_TRUNC
- [x] DATE_FROM_UNIX_DATE
- [x] FORMAT_DATE
- [x] LAST_DAY
- [x] PARSE_DATE
- [x] UNIX_DATE

### Datetime functions

- [x] CURRENT_DATETIME
- [x] DATETIME
- [x] EXTRACT
- [x] DATETIME_ADD
- [x] DATETIME_SUB
- [x] DATETIME_DIFF
- [x] DATETIME_TRUNC
- [x] FORMAT_DATETIME
- [x] LAST_DAY
- [x] PARSE_DATETIME

### Time functions

- [x] CURRENT_TIME
- [x] TIME
- [x] EXTRACT
- [x] TIME_ADD
- [x] TIME_SUB
- [x] TIME_DIFF
- [x] TIME_TRUNC
- [x] FORMAT_TIME
- [x] PARSE_TIME

### Timestamp functions

- [x] CURRENT_TIMESTAMP
- [x] EXTRACT
- [x] STRING
- [x] TIMESTAMP
- [x] TIMESTAMP_ADD
- [x] TIMESTAMP_SUB
- [x] TIMESTAMP_DIFF
- [x] TIMESTAMP_TRUNC
- [x] FORMAT_TIMESTAMP
- [x] PARSE_TIMESTAMP
- [x] TIMESTAMP_SECONDS
- [x] TIMESTAMP_MILLIS
- [x] TIMEATAMP_MICROS
- [x] UNIX_SECONDS
- [x] UNIX_MILLIS
- [x] UNIX_MICROS

### Interval functions

- [ ] MAKE_INTERVAL
- [ ] EXTRACT
- [ ] JUSTIFY_DAYS
- [ ] JUSTIFY_HOURS
- [ ] JUSTIFY_INTERVAL

### Geography functions

Not suported yet

### Security functions

- [ ] SESSION_USER

### UUID functions

- [x] GENERATE_UUID

### Net functions

- [ ] NET.IP_FROM_STRING
- [ ] NET.SAFE_IP_FROM_STRING
- [ ] NET.IP_TO_STRING
- [ ] NET.IP_NET_MASK
- [ ] NET.IP_TRUNC
- [ ] NET.IPV4_FROM_INT64
- [ ] NET.IPV4_TO_INT64
- [ ] NET.HOST
- [ ] NET.PUBLIC_SUFFIX
- [ ] NET.REG_DOMAIN

### Debugging functions

- [ ] ERROR

### AEAD encryption functions

- [ ] KEYS.NEW_KEYSET
- [ ] KEYS.ADD_KEY_FROM_RAW_BYTES
- [ ] AEAD.DECRYPT_BYTES
- [ ] AEAD.DECRYPT_STRING
- [ ] AEAD.ENCRYPT
- [ ] DETERMINISTIC_DECRYPT_BYTES
- [ ] DETERMINISTIC_DECRYPT_STRING
- [ ] DETERMINISTIC_ENCRYPT
- [ ] KEYS.KEYSET_CHAIN
- [ ] KEYS.KEYSET_FROM_JSON
- [ ] KEYS.KEYSET_TO_JSON
- [ ] KEYS.ROTATE_KEYSET
- [ ] KEYS.KEYSET_LENGTH

# License

MIT
