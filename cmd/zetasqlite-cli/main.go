package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/chzyer/readline"
	_ "github.com/goccy/go-zetasqlite"
	"github.com/goccy/go-zetasqlite/internal"
	"github.com/jessevdk/go-flags"
	"golang.org/x/crypto/ssh/terminal"
)

type option struct {
	RawMode     bool   `description:"specify the raw query mode. write sqlite3 query directly. this is a debug mode for developers" long:"raw"`
	HistoryFile string `description:"specify the history file for used queries" long:"history" default:".zetasqlite_history"`
}

type exitCode int

const (
	exitOK    exitCode = 0
	exitError exitCode = 1
)

const (
	zetasqliteRawDriver = "zetasqlite_sqlite3"
	zetasqliteDriver    = "zetasqlite"
)

var (
	errQuit    = errors.New("exit normally")
	commandMap = map[string]func(context.Context, []string, option, *sql.DB) error{
		".quit":      stopCommand,
		".exit":      stopCommand,
		".tables":    showTablesCommand,
		".functions": showFunctionsCommand,
	}
)

func parseOpt() ([]string, option, error) {
	var opt option
	parser := flags.NewParser(&opt, flags.Default)
	args, err := parser.Parse()
	return args, opt, err
}

func main() {
	os.Exit(int(run(context.Background())))
}

func run(ctx context.Context) exitCode {
	args, opt, err := parseOpt()
	if err != nil {
		flagsErr, ok := err.(*flags.Error)
		if !ok {
			fmt.Fprintf(os.Stderr, "[zetasqlite] unknown parsed option error: %[1]T %[1]v\n", err)
			return exitError
		}
		if flagsErr.Type == flags.ErrHelp {
			return exitOK
		}
		return exitError
	}
	if err := start(ctx, args, opt); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		return exitError
	}
	return exitOK
}

func getDriverName(opt option) string {
	if opt.RawMode {
		return zetasqliteRawDriver
	}
	return zetasqliteDriver
}

func getDSN(args []string) string {
	if len(args) > 0 {
		return fmt.Sprintf("file:%s?cache=shared", args[0])
	}
	return "file::memory:?cache=shared"
}

func start(ctx context.Context, args []string, opt option) error {
	db, err := sql.Open(getDriverName(opt), getDSN(args))
	if err != nil {
		return fmt.Errorf("failed to open zetasqlite driver: %w", err)
	}
	defer db.Close()

	if !terminal.IsTerminal(int(os.Stdin.Fd())) {
		// use pipe
		query, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		if err := eval(ctx, args, opt, db, string(query)); err != nil {
			return err
		}
	}
	rl, err := readline.NewEx(&readline.Config{
		Prompt:      "zetasqlite> ",
		HistoryFile: opt.HistoryFile,
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
		if err := eval(ctx, args, opt, db, line); err != nil {
			if err == errQuit {
				break
			}
			return err
		}
	}
	return nil
}

func eval(ctx context.Context, args []string, opt option, db *sql.DB, query string) error {
	query = strings.TrimSpace(query)
	if cmd, exists := commandMap[query]; exists {
		return cmd(ctx, args, opt, db)
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return nil
	}
	columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return nil
	}
	columnNum := len(columns)
	queryArgs := make([]interface{}, columnNum)
	for i := 0; i < columnNum; i++ {
		var v interface{}
		queryArgs[i] = &v
	}
	header := strings.Join(columns, "|")
	fmt.Printf("%s\n", header)
	fmt.Printf("%s\n", strings.Repeat("-", len(header)))
	for rows.Next() {
		if err := rows.Scan(queryArgs...); err != nil {
			fmt.Printf("ERROR: %v", err)
			return nil
		}
		values := make([]string, 0, len(queryArgs))
		for _, arg := range queryArgs {
			v := reflect.ValueOf(arg).Elem().Interface()
			values = append(values, fmt.Sprint(v))
		}
		fmt.Printf("%s\n", strings.Join(values, "|"))
	}
	return nil
}

func stopCommand(_ context.Context, _ []string, _ option, _ *sql.DB) error { return errQuit }

func showTablesCommand(ctx context.Context, args []string, opt option, _ *sql.DB) error {
	db, err := sql.Open(zetasqliteRawDriver, getDSN(args))
	if err != nil {
		return fmt.Errorf("failed to open zetasqlite driver: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `SELECT name, spec FROM zetasqlite_catalog WHERE kind = "table"`)
	if err != nil {
		return nil
	}
	for rows.Next() {
		var (
			name string
			spec string
		)
		if err := rows.Scan(&name, &spec); err != nil {
			return err
		}
		var table internal.TableSpec
		if err := json.Unmarshal([]byte(spec), &table); err != nil {
			return err
		}
		fmt.Println(strings.Join(table.NamePath, "."))
	}
	return nil
}

func showFunctionsCommand(ctx context.Context, args []string, opt option, _ *sql.DB) error {
	db, err := sql.Open(zetasqliteRawDriver, getDSN(args))
	if err != nil {
		return fmt.Errorf("failed to open zetasqlite driver: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `SELECT name, spec FROM zetasqlite_catalog WHERE kind = "function"`)
	if err != nil {
		return nil
	}
	for rows.Next() {
		var (
			name string
			spec string
		)
		if err := rows.Scan(&name, &spec); err != nil {
			return err
		}
		var fn internal.FunctionSpec
		if err := json.Unmarshal([]byte(spec), &fn); err != nil {
			return err
		}
		fmt.Println(strings.Join(fn.NamePath, "."))
	}
	return nil
}
