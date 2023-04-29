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
	"github.com/goccy/go-zetasqlite"
	"github.com/goccy/go-zetasqlite/internal"
	"github.com/jessevdk/go-flags"
	"golang.org/x/crypto/ssh/terminal"
)

type option struct {
	RawMode       bool   `description:"specify the raw query mode. write sqlite3 query directly. this is a debug mode for developers" long:"raw"`
	HistoryFile   string `description:"specify the history file for used queries" long:"history" default:".zetasqlite_history"`
	AutoIndexMode bool   `description:"specify the auto index mode. automatically create an index when creating a table" long:"autoindex"`
	ExplainMode   bool   `description:"specify the explain mode. show results using sqlite3's explain query plan instead of executing the query" long:"explain"`
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
	errQuit = errors.New("exit normally")
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
	cli := &CLI{
		args:            args,
		historyFile:     opt.HistoryFile,
		isRawMode:       opt.RawMode,
		isAutoIndexMode: opt.AutoIndexMode,
		isExplainMode:   opt.ExplainMode,
	}
	if err := cli.run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err.Error())
		return exitError
	}
	return exitOK
}

type CLI struct {
	args            []string
	historyFile     string
	isRawMode       bool
	isAutoIndexMode bool
	isExplainMode   bool
}

func (cli *CLI) run(ctx context.Context) error {
	if !terminal.IsTerminal(int(os.Stdin.Fd())) {
		// use pipe
		query, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		if err := cli.runCommand(ctx, string(query)); err != nil {
			return err
		}
	}
	rl, err := readline.NewEx(&readline.Config{
		Prompt:      "zetasqlite> ",
		HistoryFile: cli.historyFile,
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
		if err := cli.runCommand(ctx, line); err != nil {
			if err == errQuit {
				break
			}
			return err
		}
	}
	return nil
}

func (cli *CLI) getDSN() string {
	if len(cli.args) > 0 {
		return fmt.Sprintf("file:%s?cache=shared", cli.args[0])
	}
	return "file::memory:?cache=shared"
}

func (cli *CLI) getDriverName() string {
	if cli.isRawMode {
		return zetasqliteRawDriver
	}
	return zetasqliteDriver
}

type CommandArgs struct {
	args        []string
	query       string
	subCommands []string
}

func (cli *CLI) runCommand(ctx context.Context, query string) error {
	query = strings.TrimSpace(query)
	commands := strings.Split(query, " ")
	if len(commands) == 0 {
		return nil
	}
	commandType := commands[0]
	var subCommands []string
	if len(commands) > 1 {
		subCommands = commands[1:]
	}
	switch commandType {
	case ".quit", ".exit":
		return errQuit
	case ".tables":
		return cli.showTablesCommand(ctx)
	case ".functions":
		return cli.showFunctionsCommand(ctx)
	case ".explain":
		return cli.explainModeCommand(ctx, subCommands)
	case ".autoindex":
		return cli.autoIndexModeCommand(ctx, subCommands)
	}
	return cli.defaultCommand(ctx, query)
}

func (cli *CLI) showTablesCommand(ctx context.Context) error {
	db, err := sql.Open(zetasqliteRawDriver, cli.getDSN())
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

func (cli *CLI) showFunctionsCommand(ctx context.Context) error {
	db, err := sql.Open(zetasqliteRawDriver, cli.getDSN())
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

func (cli *CLI) explainModeCommand(ctx context.Context, subCommands []string) error {
	if len(subCommands) == 0 {
		fmt.Println(".explain requires on/off argument")
		return nil
	}
	switch subCommands[0] {
	case "on":
		cli.isExplainMode = true
	case "off":
		cli.isExplainMode = false
	}
	return nil
}

func (cli *CLI) autoIndexModeCommand(ctx context.Context, subCommands []string) error {
	if len(subCommands) == 0 {
		fmt.Printf(".autoindex requires on/off argument")
		return nil
	}
	switch subCommands[1] {
	case "on":
		cli.isAutoIndexMode = true
	case "off":
		cli.isAutoIndexMode = false
	}
	return nil
}

func (cli *CLI) defaultCommand(ctx context.Context, query string) error {
	db, err := sql.Open(cli.getDriverName(), cli.getDSN())
	if err != nil {
		return fmt.Errorf("failed to open zetasqlite driver: %w", err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	if !cli.isRawMode {
		if err := conn.Raw(func(c interface{}) error {
			zetasqliteConn, ok := c.(*zetasqlite.ZetaSQLiteConn)
			if !ok {
				return fmt.Errorf("failed to get ZetaSQLiteConn from %T", c)
			}
			zetasqliteConn.SetExplainMode(cli.isExplainMode)
			zetasqliteConn.SetAutoIndexMode(cli.isAutoIndexMode)
			return nil
		}); err != nil {
			return fmt.Errorf("failed to setup connection: %w", err)
		}
	}
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return nil
	}
	defer rows.Close()
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
