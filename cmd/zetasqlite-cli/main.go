package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"

	"github.com/chzyer/readline"
	_ "github.com/goccy/go-zetasqlite"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatalf("%+v", err)
	}
}

func run(ctx context.Context) error {
	db, err := sql.Open("zetasqlite_sqlite3", ":memory:")
	if err != nil {
		return err
	}
	rl, err := readline.NewEx(&readline.Config{
		Prompt:      ">> ",
		HistoryFile: "./zetasqlite.history",
	})
	if err != nil {
		return err
	}
	defer rl.Close()
	for {
		line, err := rl.Readline()
		if err == io.EOF || err == readline.ErrInterrupt {
			break
		}
		line = strings.TrimSpace(line)
		rows, err := db.QueryContext(ctx, line)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			continue
		}
		columns, err := rows.Columns()
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			continue
		}
		columnNum := len(columns)
		args := make([]interface{}, columnNum)
		for i := 0; i < columnNum; i++ {
			var v interface{}
			args[i] = &v
		}
		header := strings.Join(columns, "|")
		fmt.Printf("%s\n", header)
		fmt.Printf("%s\n", strings.Repeat("-", len(header)))
		for rows.Next() {
			if err := rows.Scan(args...); err != nil {
				fmt.Printf("ERROR: %v", err)
				break
			}
			values := make([]string, 0, len(args))
			for _, arg := range args {
				v := reflect.ValueOf(arg).Elem().Interface()
				values = append(values, fmt.Sprint(v))
			}
			fmt.Printf("%s\n", strings.Join(values, "|"))
		}
	}
	return nil
}
