package internal

import (
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// CreateTableAsSelectStmtTransformer handles transformation of CreateTableAsSelectStmt nodes from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a CreateTableAsSelectStmt represents a CREATE TABLE AS SELECT statement,
// which creates a new table based on the result of a SELECT query. This transformer converts
// ZetaSQL CREATE TABLE AS SELECT statements to SQLite-compatible CREATE TABLE AS SELECT syntax.
//
// The transformer handles:
// - Extracting table name and creation options (IF NOT EXISTS)
// - Recursively transforming the SELECT query scan through the coordinator
// - Transforming each output column expression in the SELECT list
// - Creating the final CreateTableStatement structure for SQL generation
//
// This transformer bridges the gap between ZetaSQL's resolved AST structure and
// the SQLite CREATE TABLE AS SELECT statement representation.
type CreateTableAsSelectStmtTransformer struct {
	coordinator Coordinator // For recursive transformation of the inner SELECT query
}

// NewCreateTableAsSelectStmtTransformer creates a new CREATE TABLE AS SELECT statement transformer
func NewCreateTableAsSelectStmtTransformer(coordinator Coordinator) *CreateTableAsSelectStmtTransformer {
	return &CreateTableAsSelectStmtTransformer{
		coordinator: coordinator,
	}
}

// Transform transforms CREATE TABLE AS SELECT statement data into a SQL fragment
func (t *CreateTableAsSelectStmtTransformer) Transform(data StatementData, ctx TransformContext) (SQLFragment, error) {
	// Verify we got the expected statement type
	if data.Type != StatementTypeCreate || data.Create == nil || data.Create.Table == nil {
		return nil, fmt.Errorf("expected CREATE TABLE statement data for CREATE TABLE AS SELECT, got type %v", data.Type)
	}

	createData := data.Create.Table

	// Verify this is actually a CREATE TABLE AS SELECT (has AsSelect data)
	if createData.AsSelect == nil {
		return nil, fmt.Errorf("expected CREATE TABLE AS SELECT, but AsSelect data is nil")
	}

	// Transform the AS SELECT query using the coordinator
	// We need to create a SELECT statement data and transform it
	selectStmtData := StatementData{
		Type:   StatementTypeSelect,
		Select: createData.AsSelect,
	}

	selectFragment, err := t.coordinator.TransformStatement(selectStmtData, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform AS SELECT query: %w", err)
	}

	// Create the CREATE TABLE AS SELECT statement
	createTableStmt := &CreateTableStatement{
		IfNotExists: createData.IfNotExists,
		TableName:   createData.TableName,
		AsSelect:    selectFragment.(*SelectStatement),
	}

	return createTableStmt, nil
}

// CanTransform checks if this transformer can handle the given node type
func (t *CreateTableAsSelectStmtTransformer) CanTransform(node ast.Node) bool {
	_, ok := node.(*ast.CreateTableAsSelectStmtNode)
	return ok
}
