package internal

import (
	"fmt"
	"github.com/goccy/go-zetasql/types"
)

// FunctionCallTransformer handles transformation of function calls from ZetaSQL to SQLite.
//
// BigQuery/ZetaSQL supports a rich set of built-in functions with different semantics than SQLite.
// This transformer bridges the gap by:
// - Converting ZetaSQL function calls to SQLite equivalents
// - Handling special ZetaSQL functions (IFNULL, IF, CASE) via custom zetasqlite_* functions
// - Managing window functions with proper OVER clause transformation
// - Processing function arguments recursively through the coordinator
// - Injecting current time for time-dependent functions when needed
//
// Key ZetaSQL -> SQLite transformations handled:
// - zetasqlite_ifnull -> CASE WHEN...IS NULL pattern
// - zetasqlite_if -> CASE WHEN...THEN...ELSE pattern
// - zetasqlite_case_* -> CASE expressions with proper value/condition handling
// - Window functions with PARTITION BY, ORDER BY, and frame specifications
// - Built-in function mapping through the function registry
//
// The transformer ensures function semantics are preserved across the SQL dialect boundary.
type FunctionCallTransformer struct {
	coordinator Coordinator // For recursive transformation of arguments
}

// NewFunctionCallTransformer creates a new function call transformer
func NewFunctionCallTransformer(coordinator Coordinator) *FunctionCallTransformer {
	return &FunctionCallTransformer{
		coordinator: coordinator,
	}
}

// Transform converts FunctionCallData to SQLExpression
func (t *FunctionCallTransformer) Transform(data ExpressionData, ctx TransformContext) (*SQLExpression, error) {
	if data.Type != ExpressionTypeFunction || data.Function == nil {
		return nil, fmt.Errorf("expected function call expression data, got type %v", data.Type)
	}

	function := data.Function

	// Transform arguments recursively
	args := make([]*SQLExpression, 0, len(function.Arguments))
	for i, argData := range function.Arguments {
		arg, err := t.coordinator.TransformExpression(argData, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform function argument %d: %w", i, err)
		}
		args = append(args, arg)
	}

	_, existsCurrentTime := currentTimeFuncMap[data.Function.Name]
	if existsCurrentTime {
		currentTime := CurrentTime(ctx.Context())
		if currentTime != nil {
			encodedCurrentTime, err := NewLiteralExpressionFromGoValue(types.Int64Type(), currentTime.UnixNano())
			if err != nil {
				return nil, fmt.Errorf("failed to encode current time: %w", err)
			}
			args = append(args, encodedCurrentTime)
		}
	}

	// Handle special ZetaSQL functions that need transformation
	switch function.Name {
	case "zetasqlite_ifnull":
		// Convert to CASE expression: IFNULL(a, b) => CASE WHEN a IS NULL THEN b ELSE a END
		if len(args) != 2 {
			return nil, fmt.Errorf("zetasqlite_ifnull requires exactly 2 arguments")
		}
		return NewCaseExpression(
			[]*WhenClause{
				{
					Condition: NewBinaryExpression(args[0], "IS", NewLiteralExpression("NULL")),
					Result:    args[1],
				},
			},
			args[0],
		), nil

	case "zetasqlite_if":
		// Convert to CASE expression: IF(condition, then_result, else_result) => CASE WHEN condition THEN then_result ELSE else_result END
		if len(args) != 3 {
			return nil, fmt.Errorf("zetasqlite_if requires exactly 3 arguments")
		}
		return NewCaseExpression([]*WhenClause{{Condition: args[0], Result: args[1]}}, args[2]), nil

	case "zetasqlite_case_no_value":
		// Convert to CASE expression: arguments are condition, result, condition, result, ..., [else]
		whenClauses := make([]*WhenClause, 0, len(args)/2)
		for i := 0; i < len(args)-1; i += 2 {
			whenClauses = append(whenClauses, &WhenClause{
				Condition: args[i],
				Result:    args[i+1],
			})
		}
		var elseExpr *SQLExpression
		// if args length is odd number, else statement exists
		if len(args) > (len(args)/2)*2 {
			elseExpr = args[len(args)-1]
		}
		return NewCaseExpression(whenClauses, elseExpr), nil

	case "zetasqlite_case_with_value":
		// Convert to CASE expression with value: first arg is value, then condition, result, condition, result, ..., [else]
		if len(args) < 3 {
			return nil, fmt.Errorf("zetasqlite_case_with_value requires at least 3 arguments")
		}

		valueExpr := args[0]
		remainingArgs := args[1:]

		whenClauses := make([]*WhenClause, 0, len(remainingArgs)/2)
		for i := 0; i < len(remainingArgs)-1; i += 2 {
			whenClauses = append(whenClauses, &WhenClause{
				Condition: remainingArgs[i],
				Result:    remainingArgs[i+1],
			})
		}
		var elseExpr *SQLExpression
		// if remaining args length is odd number, else statement exists
		if len(remainingArgs) > (len(remainingArgs)/2)*2 {
			elseExpr = remainingArgs[len(remainingArgs)-1]
		}
		return NewSimpleCaseExpression(valueExpr, whenClauses, elseExpr), nil

	default:
		var windowSpec *WindowSpecification
		if function.WindowSpec != nil {
			// Transform PARTITION BY expressions
			partitionBy := make([]*SQLExpression, 0, len(function.WindowSpec.PartitionBy))
			for _, partData := range function.WindowSpec.PartitionBy {
				expr, err := t.coordinator.TransformExpression(*partData, ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to transform partition by expression: %w", err)
				}

				// Apply collation so SQLite will partition the rows based on zetasqlite_collate return value
				expr.Collation = "zetasqlite_collate"

				partitionBy = append(partitionBy, expr)
			}

			// Transform ORDER BY expressions
			orderBy := make([]*OrderByItem, 0, len(function.WindowSpec.OrderBy))
			for _, orderData := range function.WindowSpec.OrderBy {
				expr, err := t.coordinator.TransformExpression(orderData.Expression, ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to transform order by expression: %w", err)
				}
				orderByItems, err := createOrderByItems(expr, orderData)
				if err != nil {
					return nil, fmt.Errorf("failed to create order by items: %w", err)
				}
				orderBy = append(orderBy, orderByItems...)
			}

			// Transform frame clause if present
			var frameClause *FrameClause
			if function.WindowSpec.FrameClause != nil {
				frameData := function.WindowSpec.FrameClause
				frameClause = &FrameClause{
					Unit: frameData.Unit,
				}

				// Transform start bound
				if frameData.Start != nil {
					var startOffset *SQLExpression
					if frameData.Start.Offset != (ExpressionData{}) {
						var err error
						startOffset, err = t.coordinator.TransformExpression(frameData.Start.Offset, ctx)
						if err != nil {
							return nil, fmt.Errorf("failed to transform frame start offset: %w", err)
						}
					}
					frameClause.Start = &FrameBound{
						Type:   frameData.Start.Type,
						Offset: startOffset,
					}
				}

				// Transform end bound
				if frameData.End != nil {
					var endOffset *SQLExpression
					if frameData.End.Offset != (ExpressionData{}) {
						var err error
						endOffset, err = t.coordinator.TransformExpression(frameData.End.Offset, ctx)
						if err != nil {
							return nil, fmt.Errorf("failed to transform frame end offset: %w", err)
						}
					}
					frameClause.End = &FrameBound{
						Type:   frameData.End.Type,
						Offset: endOffset,
					}
				}
			}

			windowSpec = &WindowSpecification{
				PartitionBy: partitionBy,
				OrderBy:     orderBy,
				FrameClause: frameClause,
			}
		}

		// Fast path optimization: bypass function calls for primitive type operations
		// Function calls incur huge overheads: as each call's args must be decoded/encoded, as well as
		// allocated within both the modernc.org/sqlite driver and the go-zetasqlite driver
		// This could happen potentially hundreds of thousands of times per query in the case of complex JOINs
		if canOptimizeFunction(function) {
			return optimizeFunctionToSQL(function.Name, args)
		}

		funcMap := funcMapFromContext(ctx.Context())
		if spec, exists := funcMap[function.Name]; exists {
			return spec.CallSQL(ctx.Context(), function, args)
		}
		// Default function call transformation
		return &SQLExpression{
			Type: ExpressionTypeFunction,
			FunctionCall: &FunctionCall{
				Name:       function.Name,
				Arguments:  args,
				WindowSpec: windowSpec,
			},
		}, nil
	}
}

// canOptimizeFunction checks if a function can be optimized to use direct SQL operators
func canOptimizeFunction(function *FunctionCallData) bool {
	_, found := functionToOperator[function.Name]
	if !found {
		return false
	}

	// Check argument count requirements
	switch function.Name {
	case "zetasqlite_not":
		if len(function.Arguments) != 1 {
			return false
		}
	case "zetasqlite_and", "zetasqlite_or":
		if len(function.Arguments) < 2 {
			return false
		}
	default: // comparison operators
		if len(function.Arguments) != 2 {
			return false
		}
	}

	// All arguments must be primitive SQLite-compatible types or optimizable expressions
	for _, arg := range function.Arguments {
		if !isPrimitiveSQLiteType(arg) {
			return false
		}
	}

	return true
}

var functionToOperator = map[string]string{
	// Comparison operators
	"zetasqlite_equal":            "=",
	"zetasqlite_not_equal":        "!=",
	"zetasqlite_less":             "<",
	"zetasqlite_greater":          ">",
	"zetasqlite_less_or_equal":    "<=",
	"zetasqlite_greater_or_equal": ">=",
	"zetasqlite_in":               "IN",
	// Logical operators
	"zetasqlite_and": "AND",
	"zetasqlite_or":  "OR",
	"zetasqlite_not": "NOT",
}

// optimizeFunctionToSQL converts functions to direct SQL operators
func optimizeFunctionToSQL(functionName string, args []*SQLExpression) (*SQLExpression, error) {
	operator, found := functionToOperator[functionName]
	if !found {
		return nil, fmt.Errorf("unknown optimizable function: %s", functionName)
	}

	switch functionName {
	case "zetasqlite_and", "zetasqlite_or":
		if len(args) < 2 {
			return nil, fmt.Errorf("%s expected at least 2 arguments, got %d", functionName, len(args))
		}
		// Chain multiple arguments with the operator
		result := args[0]
		for i := 1; i < len(args); i++ {
			result = NewBinaryExpression(result, operator, args[i])
		}
		return result, nil

	case "zetasqlite_not":
		if len(args) != 1 {
			return nil, fmt.Errorf("%s expected only 1 argument, got %d", functionName, len(args))
		}
		return NewNotExpression(args[0]), nil
	case "zetasqlite_in":
		return NewBinaryExpression(args[0], operator, NewListExpression(args[1:])), nil
	default: // comparison operators
		if len(args) != 2 {
			return nil, fmt.Errorf("%s expected 2 arguments, got %d", functionName, len(args))
		}
		return NewBinaryExpression(args[0], operator, args[1]), nil
	}
}

// isPrimitiveSQLiteType checks if an expression represents a primitive type that SQLite can handle natively
// or if it's an already-optimized expression that can be further optimized
func isPrimitiveSQLiteType(expr ExpressionData) bool {
	switch expr.Type {
	case ExpressionTypeLiteral:
		if expr.Literal == nil || expr.Literal.Value == nil {
			return false
		}
		// Check if the literal value is a primitive type
		switch expr.Literal.Value.(type) {
		case IntValue, FloatValue, BoolValue:
			return true
		case StringValue:
			// String literals can be compared directly in SQLite
			return true
		default:
			return false
		}
	case ExpressionTypeColumn:
		t := expr.Column.Type
		return t.IsInt32() ||
			t.IsInt64() ||
			t.IsUint32() ||
			t.IsUint64() ||
			t.IsBool() ||
			t.IsFloat() ||
			t.IsDouble() ||
			t.IsString()
	case ExpressionTypeFunction:
		// If this is an optimizable function, it can be treated as primitive for further optimization
		if expr.Function != nil {
			_, found := functionToOperator[expr.Function.Name]
			return found && canOptimizeFunction(expr.Function)
		}
		return false
	default:
		return false
	}
}
