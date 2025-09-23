package internal

import (
	"context"
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// Core transformation interfaces

// Transformer represents a pure transformation from input to output
type Transformer[Input, Output any] interface {
	Transform(input Input, ctx TransformContext) (Output, error)
}

// ExpressionTransformer specifically handles expression transformations
type ExpressionTransformer interface {
	Transformer[ExpressionData, *SQLExpression]
}

// StatementTransformer handles statement-level transformations
type StatementTransformer interface {
	Transformer[StatementData, SQLFragment]
}

// ScanTransformer handles scan node transformations
type ScanTransformer interface {
	Transformer[ScanData, *FromItem]
}

// Coordinator orchestrates the transformation process without doing the transformations itself
type Coordinator interface {
	// AST-based transformation methods (for initial entry points)
	TransformStatementNode(node ast.Node, ctx TransformContext) (SQLFragment, error)

	// Data-based transformation methods (for transformers working with pure data)
	TransformExpression(exprData ExpressionData, ctx TransformContext) (*SQLExpression, error)
	TransformStatement(stmtData StatementData, ctx TransformContext) (SQLFragment, error)
	TransformScan(scanData ScanData, ctx TransformContext) (*FromItem, error)
	TransformWithEntry(scanData ScanData, ctx TransformContext) (*WithClause, error)
}

// TransformContext provides contextual information for transformations
type TransformContext interface {
	// Context returns the underlying Go context
	Context() context.Context

	// FragmentContext provides column resolution and scoping
	FragmentContext() FragmentContextProvider

	// Config returns transformation configuration
	Config() *TransformConfig

	// WithFragmentContext returns a new context with updated fragment context
	WithFragmentContext(fc FragmentContextProvider) TransformContext

	// WITH clause support
	AddWithEntryColumnMapping(name string, columns []*ColumnData)
	GetWithEntryMapping(name string) []string
}

// FragmentContextProvider abstracts the fragment context functionality
type FragmentContextProvider interface {
	GetQualifiedColumnExpression(columnID int) *SQLExpression
	AddAvailableColumn(columnID int, info *ColumnInfo)
	GetID() string
	EnterScope() ScopeToken
	ExitScope(token ScopeToken)

	// Column ID to scope mapping for qualified references
	GetQualifiedColumnRef(columnID int) (columnName, tableAlias string)
	RegisterColumnScope(columnID int, scopeAlias string)
	RegisterColumnScopeMapping(scopeAlias string, columns []*ColumnData)
	AddAvailableColumnsForDML(data *ScanData)
}

// ScopeToken represents a scope boundary
type ScopeToken interface {
	ID() string
}
