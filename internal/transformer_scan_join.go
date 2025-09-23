package internal

import (
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// JoinScanTransformer handles JOIN scan transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a JoinScan represents SQL JOIN operations that combine rows from
// two input scans based on join conditions and join types. This includes INNER JOIN,
// LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN, and CROSS JOIN operations.
//
// The transformer converts ZetaSQL JoinScan nodes into SQLite JOIN clauses by:
// - Recursively transforming left and right input scans
// - Converting ZetaSQL join types to SQLite equivalents
// - Transforming join conditions through the coordinator
// - Wrapping the result in a SELECT * subquery for consistent output structure
//
// Join conditions are expressions that determine which rows from the left and right
// scans should be combined. The transformer ensures proper column qualification
// across the join boundary through the fragment context.
type JoinScanTransformer struct {
	coordinator Coordinator // For recursive transformation of the left and right scans
}

// NewJoinScanTransformer creates a new join scan transformer
func NewJoinScanTransformer(coordinator Coordinator) *JoinScanTransformer {
	return &JoinScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts JoinScanData to FromItem with JOIN clause
func (t *JoinScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeJoin || data.JoinScan == nil {
		return nil, fmt.Errorf("expected join scan data, got type %v", data.Type)
	}

	joinScanData := data.JoinScan

	leftFromItem, err := t.coordinator.TransformScan(joinScanData.LeftScan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform left scan in join: %w", err)
	}

	rightFromItem, err := t.coordinator.TransformScan(joinScanData.RightScan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform right scan in join: %w", err)
	}

	// Transform the join condition if present
	var joinCondition *SQLExpression
	if joinScanData.JoinCondition != nil {
		conditionExpr, err := t.coordinator.TransformExpression(*joinScanData.JoinCondition, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform join condition: %w", err)
		}
		joinCondition = conditionExpr
	}

	// Convert ZetaSQL join type to internal join type
	joinType := convertJoinType(joinScanData.JoinType)

	// Create the SELECT statement with JOIN clause
	selectStatement := &SelectStatement{
		SelectList: []*SelectListItem{{
			Expression: NewStarExpression(),
		}},
		FromClause: &FromItem{
			Type: FromItemTypeJoin,
			Join: &JoinClause{
				Type:      joinType,
				Left:      leftFromItem,
				Right:     rightFromItem,
				Condition: joinCondition,
			},
		},
	}

	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: selectStatement,
	}, nil
}

func convertJoinType(joinType ast.JoinType) JoinType {
	switch joinType {
	case ast.JoinTypeInner:
		return JoinTypeInner
	case ast.JoinTypeLeft:
		return JoinTypeLeft
	case ast.JoinTypeRight:
		return JoinTypeRight
	case ast.JoinTypeFull:
		return JoinTypeFull
	default:
		return JoinTypeInner
	}
}
