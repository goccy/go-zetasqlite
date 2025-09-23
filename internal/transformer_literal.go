package internal

import (
	"fmt"
)

// LiteralTransformer handles transformation of literal values from ZetaSQL to SQLite.
//
// BigQuery/ZetaSQL supports rich literal types including complex values like STRUCT literals,
// ARRAY literals, and typed NULL values that don't have direct SQLite equivalents.
// Literals represent constant values in SQL expressions (numbers, strings, booleans, etc.).
//
// The transformer converts ZetaSQL literal values by:
// - Encoding complex ZetaSQL literals into SQLite-compatible string representations
// - Preserving type information through the encoding process
// - Handling special values like typed NULL, NaN, and infinity
// - Using the LiteralFromValue function for consistent encoding
//
// This ensures that complex BigQuery literal values can be properly represented and
// processed in the SQLite runtime environment while maintaining their semantic meaning.
type LiteralTransformer struct {
}

// NewLiteralTransformer creates a new literal transformer with the given configuration
func NewLiteralTransformer() *LiteralTransformer {
	return &LiteralTransformer{}
}

// Transform converts LiteralData to SQLExpression
func (t *LiteralTransformer) Transform(data ExpressionData, ctx TransformContext) (*SQLExpression, error) {
	encoded, err := LiteralFromValue(data.Literal.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to encode literal: %w", err)
	}
	return &SQLExpression{
		Type:  ExpressionTypeLiteral,
		Value: encoded,
	}, nil
}
