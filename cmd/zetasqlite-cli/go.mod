module github.com/goccy/go-zetasqlite/cmd/zetasqlite-cli

go 1.18

require (
	github.com/chzyer/readline v1.5.1
	github.com/goccy/go-zetasqlite v0.4.0
)

require (
	github.com/goccy/go-json v0.9.10 // indirect
	github.com/goccy/go-zetasql v0.2.9 // indirect
	github.com/mattn/go-sqlite3 v1.14.14 // indirect
	golang.org/x/sys v0.0.0-20220310020820-b874c991c1a5 // indirect
)

replace github.com/goccy/go-zetasqlite => ../../
