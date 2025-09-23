package internal

import (
	"fmt"
	"github.com/goccy/go-json"

	"github.com/goccy/go-zetasql/types"
)

// CastTransformer handles transformation of type casting operations from ZetaSQL to SQLite.
//
// BigQuery/ZetaSQL has a rich type system with complex types (STRUCT, ARRAY, etc.) and
// sophisticated casting rules that differ significantly from SQLite's simpler type system.
// ZetaSQL supports both explicit CAST() operations and implicit type coercion.
//
// The transformer converts ZetaSQL cast operations by:
// - Recursively transforming the expression being cast
// - Encoding source and target type information as JSON
// - Using the zetasqlite_cast runtime function for complex type conversions
// - Handling safe cast semantics (SAFE_CAST returns NULL on conversion failure)
//
// The zetasqlite_cast function bridges the type system gap by implementing ZetaSQL's
// casting semantics in the SQLite runtime, preserving behavior for complex types
// and edge cases that SQLite's native CAST cannot handle.
type CastTransformer struct {
	coordinator Coordinator // For recursive transformation of the cast expression
}

// NewCastTransformer creates a new cast transformer
func NewCastTransformer(coordinator Coordinator) *CastTransformer {
	return &CastTransformer{
		coordinator: coordinator,
	}
}

// Transform converts CastData to SQLExpression
func (t *CastTransformer) Transform(data ExpressionData, ctx TransformContext) (*SQLExpression, error) {
	if data.Type != ExpressionTypeCast || data.Cast == nil {
		return nil, fmt.Errorf("expected cast expression data, got type %v", data.Type)
	}

	cast := data.Cast

	// Transform the inner expression recursively
	innerExpr, err := t.coordinator.TransformExpression(data.Cast.Expression, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform cast expression: %w", err)
	}

	// Use zetasqlite_cast function for complex type conversions
	return t.createZetaSQLiteCast(innerExpr, cast)
}

// createZetaSQLiteCast creates a zetasqlite_cast function call for complex casts
func (t *CastTransformer) createZetaSQLiteCast(expr *SQLExpression, cast *CastData) (*SQLExpression, error) {
	jsonFromType, err := json.Marshal(newType(cast.FromType))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal source type: %w", err)
	}

	jsonToType, err := json.Marshal(newType(cast.ToType))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal source type: %w", err)
	}

	// Encode type information as JSON
	encodedFromType, err := NewLiteralExpressionFromGoValue(types.StringType(), string(jsonFromType))
	if err != nil {
		return nil, fmt.Errorf("failed to encode source type: %w", err)
	}

	encodedToType, err := NewLiteralExpressionFromGoValue(types.StringType(), string(jsonToType))
	if err != nil {
		return nil, fmt.Errorf("failed to encode target type: %w", err)
	}

	// Create the zetasqlite_cast function call
	return &SQLExpression{
		Type: ExpressionTypeFunction,
		FunctionCall: &FunctionCall{
			Name: "zetasqlite_cast",
			Arguments: []*SQLExpression{
				expr,            // Expression to cast
				encodedFromType, // Source type
				encodedToType,   // Target type
				NewLiteralExpression(fmt.Sprintf("%t", cast.ReturnNullOnErr)), // Safe cast flag
			},
		},
	}, nil
}
