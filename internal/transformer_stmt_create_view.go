package internal

import (
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// CreateViewStmtTransformer handles transformation of CreateViewStmt nodes from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a CreateViewStmt represents a CREATE VIEW statement,
// which creates a new view based on the result of a SELECT query. This transformer converts
// ZetaSQL CREATE VIEW statements to SQLite-compatible CREATE VIEW syntax.
//
// The transformer handles:
// - Extracting view name and creation options (IF NOT EXISTS)
// - Recursively transforming the SELECT query scan through the coordinator
// - Transforming each output column expression in the SELECT list
// - Creating the final CreateViewStatement structure for SQL generation
//
// This transformer bridges the gap between ZetaSQL's resolved AST structure and
// the SQLite CREATE VIEW statement representation.
type CreateViewStmtTransformer struct {
	coordinator Coordinator // For recursive transformation of the inner SELECT query
}

// NewCreateViewStmtTransformer creates a new CREATE VIEW statement transformer
func NewCreateViewStmtTransformer(coordinator Coordinator) *CreateViewStmtTransformer {
	return &CreateViewStmtTransformer{
		coordinator: coordinator,
	}
}

// Transform transforms CREATE VIEW statement data into a SQL fragment
func (t *CreateViewStmtTransformer) Transform(data StatementData, ctx TransformContext) (SQLFragment, error) {
	// Verify we got the expected statement type
	if data.Type != StatementTypeCreate || data.Create == nil || data.Create.View == nil {
		return nil, fmt.Errorf("expected CREATE VIEW statement data for CREATE VIEW, got type %v", data.Type)
	}

	createData := data.Create.View

	// Transform the view's SELECT query using the coordinator
	// We need to create a SELECT statement data and transform it
	selectStmtData := StatementData{
		Type:   StatementTypeSelect,
		Select: &createData.Query,
	}

	selectFragment, err := t.coordinator.TransformStatement(selectStmtData, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform view SELECT query: %w", err)
	}

	// Create the CREATE VIEW statement
	createViewStmt := &CreateViewStatement{
		ViewName: createData.ViewName,
		Query:    selectFragment,
	}

	return createViewStmt, nil
}

// CanTransform checks if this transformer can handle the given node type
func (t *CreateViewStmtTransformer) CanTransform(node ast.Node) bool {
	_, ok := node.(*ast.CreateViewStmtNode)
	return ok
}
