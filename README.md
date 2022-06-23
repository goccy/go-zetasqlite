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