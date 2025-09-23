package internal

import (
	"fmt"
)

// ProjectScanTransformer handles projection scan transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a ProjectScan represents the SQL SELECT list operation that applies
// projections (computed expressions) to columns from an input scan. This corresponds to the
// "SELECT <expr_list>" part of a SQL query where expressions can be:
// - Simple column references (pass-through columns)
// - Computed expressions (functions, arithmetic, etc.)
// - Mix of both
//
// The transformer converts ZetaSQL ProjectScan nodes into SQLite SELECT statements with proper:
// - Column aliasing using ID-based naming for disambiguation
// - Expression transformation through the coordinator pattern
// - Fragment context management for column resolution
// - Recursive transformation of the input scan
//
// Key challenges addressed:
// - Ensuring SELECT list is never empty (which causes SQLite syntax errors)
// - Expression dependency resolution through fragment context
type ProjectScanTransformer struct {
	coordinator Coordinator // For recursive transformation of expressions and inner scan
}

// NewProjectScanTransformer creates a new project scan transformer
func NewProjectScanTransformer(coordinator Coordinator) *ProjectScanTransformer {
	return &ProjectScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts ProjectScanData to FromItem with SELECT statement
func (t *ProjectScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeProject || data.ProjectScan == nil {
		return nil, fmt.Errorf("expected project scan data, got type %v", data.Type)
	}

	projectData := data.ProjectScan

	innerFromItem, err := t.coordinator.TransformScan(data.ProjectScan.InputScan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform inner scan in project: %w", err)
	}

	// Build a map of computed expressions by column ID for efficient lookup
	computedExprMap := make(map[int]ExpressionData)
	for _, computedCol := range projectData.ExprList {
		computedExprMap[computedCol.Column.ColumnID()] = computedCol.Expression
	}

	// Now build the projection - expressions can now resolve column references
	selectList := make([]*SelectListItem, 0, len(data.ColumnList))

	for _, col := range data.ColumnList {
		var expr *SQLExpression

		// Create select list item with ID-based alias for disambiguation
		alias := generateIDBasedAlias(col.Name, col.ID)

		// Check if this column has a computed expression
		if computedExprData, hasComputed := computedExprMap[col.ID]; hasComputed {
			// Transform the computed expression
			transformedExpr, err := t.coordinator.TransformExpression(computedExprData, ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to transform projection expression for column %s: %w", col.Name, err)
			}
			expr = transformedExpr
		} else {
			// No computed expression - create a column reference that will be resolved by ColumnRefTransformer
			// This ensures proper qualification through the FragmentContext
			columnRefData := ExpressionData{
				Type: ExpressionTypeColumn,
				Column: &ColumnRefData{
					ColumnName: col.Name,
					ColumnID:   col.ID,
					TableName:  col.TableName,
				},
			}

			// Transform through coordinator to get qualified column reference
			transformedExpr, err := t.coordinator.TransformExpression(columnRefData, ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to transform column reference for %s: %w", col.Name, err)
			}
			expr = transformedExpr
		}

		item := &SelectListItem{
			Expression: expr,
			Alias:      alias,
		}
		selectList = append(selectList, item)
	}

	// Create the SELECT statement
	selectStmt := &SelectStatement{
		SelectList: selectList,
		FromClause: innerFromItem,
	}

	// Return as a subquery FROM item - alias will be set by coordinator
	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: selectStmt,
	}, nil
}
