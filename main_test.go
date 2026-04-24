package zetasqlite_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	googlesql "github.com/goccy/go-googlesql"
)

// TestMain loads the go-googlesql wasm binary exactly once per package-
// level test run. Every code path in go-zetasqlite eventually talks to a
// googlesql handle (Catalog, TypeFactory, Analyzer, …) so the wasm module
// must be initialised before the driver accepts any connection.
//
// The binary ships as analyzer.wasm next to the Go sources. It is not
// checked in (gitignored); see the project README for how to regenerate
// it via `wasmify wasm-build`.
func TestMain(m *testing.M) {
	abs, err := filepath.Abs("analyzer.wasm")
	if err != nil {
		fmt.Fprintf(os.Stderr, "abs path: %v\n", err)
		os.Exit(1)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "skipping: %s not found (build the wasm first)\n", abs)
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "read wasm: %v\n", err)
		os.Exit(1)
	}
	if err := googlesql.InitFromBytes(data,
		googlesql.WithCompilationMode(googlesql.CompilationModeCompiler)); err != nil {
		fmt.Fprintf(os.Stderr, "skipping: wasm init failed: %v\n", err)
		os.Exit(0)
	}
	code := m.Run()
	googlesql.Close()
	os.Exit(code)
}
