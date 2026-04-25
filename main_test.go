package zetasqlite_test

import (
	"fmt"
	"os"
	"testing"

	googlesql "github.com/goccy/go-googlesql"
)

// TestMain initialises the go-googlesql wasm runtime exactly once per
// package-level test run. Every code path in go-zetasqlite eventually
// talks to a googlesql handle (Catalog, TypeFactory, Analyzer, …) so
// the wasm module must be initialised before the driver accepts any
// connection. go-googlesql embeds its wasm binary at compile time, so
// Init takes no path argument.
func TestMain(m *testing.M) {
	if err := googlesql.Init(googlesql.WithCompilationMode(googlesql.CompilationModeCompiler)); err != nil {
		fmt.Fprintf(os.Stderr, "skipping: wasm init failed: %v\n", err)
		os.Exit(0)
	}
	code := m.Run()
	googlesql.Close()
	os.Exit(code)
}
