module example

go 1.24.0

toolchain go1.24.2

require github.com/goccy/go-zetasqlite v0.6.6

require (
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/goccy/go-json v0.10.0 // indirect
	github.com/goccy/go-zetasql v0.5.5 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-sqlite3 v1.14.16 // indirect
	gonum.org/v1/gonum v0.11.0 // indirect
)

replace github.com/goccy/go-zetasqlite => ../
