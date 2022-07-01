# go-zetasqlite

![Go](https://github.com/goccy/go-zetasqlite/workflows/Go/badge.svg)
[![GoDoc](https://godoc.org/github.com/goccy/go-zetasqlite?status.svg)](https://pkg.go.dev/github.com/goccy/go-zetasqlite?tab=doc)

A database driver library that interprets ZetaSQL queries and runs them using SQLite3

# Features

`go-zetasqlite` supports `database/sql` driver interface.
So, you can use ZetaSQL queries just by importing `github.com/goccy/go-zetasqlite`.
Also, go-zetasqlite uses SQLite3 as the database engine.
Since we are using [go-sqlite3](https://github.com/mattn/go-sqlite3), we can use the options ( like `:memory:` ) supported by `go-sqlite3` ( see [details](https://pkg.go.dev/github.com/mattn/go-sqlite3#readme-connection-string) ).

# Installation

```
go get github.com/goccy/go-zetasqlite
```

# Status

## Types

- [x] INT64 ( `INT`, `SMALLINT`, `INTEGER`, `BIGINT`, `TINYINT`, `BYTEINT` )
- [ ] NUMERIC ( `DECIMAL` )
- [ ] BIGNUMERIC ( `BIGDECIMAL` )
- [x] FLOAT64 ( `FLOAT` )
- [x] BOOL ( `BOOLEAN` )
- [x] STRING
- [ ] BYTES
- [x] DATE
- [ ] TIME
- [ ] DATETIME
- [ ] TIMESTAMP
- [ ] INTERVAL
- [x] ARRAY
- [x] STRUCT
- [ ] GEOGRAPHY
- [ ] JSON
- [ ] RECORD

## Statements

- [x] SELECT
- [x] UPDATE
- [x] INSERT
- [x] DELETE
- [x] CREATE TABLE
- [x] CREATE FUNCTION

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
- [x] OVER

### Aggregate functions

- [x] ANY_VALUE
- [ ] ARRAY_AGG
- [ ] ARRAY_CONCAT_AGG
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

- [ ] CORR
- [ ] COVAR_POP
- [ ] COVAR_SAMP
- [ ] STDDEV_POP
- [ ] STDDEV_SAMP
- [ ] STDDEV
- [ ] VAR_POP
- [ ] VAR_SAMP
- [ ] VARIANCE

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
- [ ] DENSE_RANK
- [ ] PERCENT_RANK
- [ ] CUME_DIST
- [ ] NTILE
- [ ] ROW_NUMBER

### Bit functions

- [ ] BIT_COUNT

### Conversion functions

- [ ] CAST AS ARRAY
- [ ] CAST AS BIGNUMERIC
- [ ] CAST AS BOOL
- [ ] CAST AS BYTES
- [ ] CAST AS DATE
- [ ] CAST AS DATETIME
- [ ] CAST AS FLOAT64
- [ ] CAST AS INT64
- [ ] CAST AS INTERVAL
- [ ] CAST AS NUMERIC
- [ ] CAST AS STRING
- [ ] CAST AS STRUCT
- [ ] CAST AS TIME
- [ ] CAST AS TIMESTAMP
- [ ] PARSE_BIGNUMERIC
- [ ] PARSE_NUMERIC
- [ ] SAFE_CAST
- [ ] Format clause for CAST

### Mathematical functions

- [ ] ABS
- [ ] SIGN
- [ ] IS_INF
- [ ] IS_NAN
- [ ] IEEE_DIVIDE
- [ ] RAND
- [ ] SQRT
- [ ] POW
- [ ] POWER
- [ ] EXP
- [ ] LN
- [ ] LOG
- [ ] LOG10
- [ ] GREATEST
- [ ] LEAST
- [ ] DIV
- [ ] SAFE_DIVIDE
- [ ] SAFE_MULTIPLY
- [ ] SAFE_NEGATE
- [ ] SAFE_ADD
- [ ] SAFE_SUBTRACT
- [ ] MOD
- [ ] ROUND
- [ ] TRUNC
- [ ] CEIL
- [ ] CEILING
- [ ] FLOOR
- [ ] COS
- [ ] COSH
- [ ] ACOS
- [ ] ACOSH
- [ ] SIN
- [ ] SINH
- [ ] ASIN
- [ ] ASINH
- [ ] TAN
- [ ] TANH
- [ ] ATAN
- [ ] ATANH
- [ ] ATAN2
- [ ] RANGE_BUCKET

### Navigation functions

- [ ] FIRST_VALUE
- [x] LAST_VALUE
- [ ] NTH_VALUE
- [ ] LEAD
- [ ] LAG
- [ ] PERCENTILE_CONT
- [ ] PERCENTILE_DISC

### Hash functions

- [ ] FARM_FINGERPRINT
- [ ] MD5
- [ ] SHA1
- [ ] SHA256
- [ ] SHA512

### String functions

- [ ] ASCII
- [ ] BYTE_LENGTH
- [ ] CHAR_LENGTH
- [ ] CHARACTER_LENGTH
- [ ] CHR
- [ ] CODE_POINTS_TO_BYTES
- [ ] CODE_POINTS_TO_STRING
- [ ] COLLATE
- [ ] CONCAT
- [ ] CONTAINS_SUBSTR
- [ ] ENDS_WITH
- [ ] FORMAT
- [ ] FROM_BASE32
- [ ] FROM_BASE64
- [ ] FROM_HEX
- [ ] INITCAP
- [ ] INSTR
- [ ] LEFT
- [ ] LENGTH
- [ ] LPAD
- [ ] LOWER
- [ ] LTRIM
- [ ] NORMALIZE
- [ ] NORMALIZE_AND_CASEFOLD
- [ ] OCTET_LENGTH
- [ ] REGEXP_CONTAINS
- [ ] REGEXP_EXTRACT
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
- [ ] STARTS_WITH
- [ ] STRPOS
- [ ] SUBSTR
- [ ] SUBSTRING
- [ ] TO_BASE32
- [ ] TO_BASE64
- [ ] TO_CODE_POINTS
- [ ] TO_HEX
- [ ] TRANSALTE
- [ ] TRIM
- [ ] UNICODE
- [ ] UPPER

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
- [ ] TO_JSON
- [ ] TO_JSON_STRING

### Array functions

- [ ] ARRAY
- [ ] ARRAY_CONCAT
- [ ] ARRAY_LENGTH
- [ ] ARRAY_TO_STRING
- [ ] GENERATE_ARRAY
- [ ] GENERATE_DATE_ARRAY
- [ ] GENERATE_TIMESTAMP_ARRAY
- [ ] ARRAY_REVERSE

### Date functions

- [ ] CURRENT_DATE
- [ ] EXTRACT
- [x] DATE
- [x] DATE_ADD
- [x] DATE_SUB
- [ ] DATE_DIFF
- [ ] DATE_TRUNC
- [ ] DATE_FROM_UNIX_DATE
- [ ] FORMAT_DATE
- [ ] LAST_DAY
- [ ] PARSE_DATE
- [ ] UNIX_DATE

### Datetime functions

- [ ] CURRENT_DATETIME
- [ ] DATETIME
- [ ] EXTRACT
- [ ] DATETIME_ADD
- [ ] DATETIME_SUB
- [ ] DATETIME_DIFF
- [ ] DATETIME_TRUNC
- [ ] FORMAT_DATETIME
- [ ] LAST_DAY
- [ ] PARSE_DATETIME

### Time functions

- [ ] CURRENT_TIME
- [ ] TIME
- [ ] EXTRACT
- [ ] TIME_ADD
- [ ] TIME_SUB
- [ ] TIME_DIFF
- [ ] TIME_TRUNC
- [ ] FORMAT_TIME
- [ ] PARSE_TIME

### Timestamp functions

- [ ] CURRENT_TIMESTAMP
- [ ] EXTRACT
- [ ] STRING
- [ ] TIMESTAMP
- [ ] TIMESTAMP_ADD
- [ ] TIMESTAMP_SUB
- [ ] TIMESTAMP_DIFF
- [ ] TIMESTAMP_TRUNC
- [ ] FORMAT_TIMESTAMP
- [ ] PARSE_TIMESTAMP
- [ ] TIMESTAMP_SECONDS
- [ ] TIMESTAMP_MILLIS
- [ ] TIMEATAMP_MICROS
- [ ] UNIX_SECONDS
- [ ] UNIX_MILLIS
- [ ] UNIX_MICROS

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

- [ ] GENERATE_UUID

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

# Synopsis

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

  rows, err := db.Query(`
  SELECT
  val,
  CASE val
    WHEN 1 THEN 'one'
    WHEN 2 THEN 'two'
    WHEN 3 THEN 'three'
    ELSE 'four'
    END
  FROM UNNEST([1, 2, 3, 4]) AS val`)
  if err != nil {
    panic(err)
  }
  for rows.Next() {
    var (
      num int64
      text string    
    )
    if err := rows.Scan(&num, &text); err != nil {
	  panic(err)
    }
    fmt.Println("num = ", num, "text = ", text)
  }
}
```

# License

MIT
