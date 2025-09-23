package internal

import (
	"fmt"
	"strings"
)

// DMLStmtTransformer handles transformation of DML statement nodes (INSERT, UPDATE, DELETE) from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, DML statements include INSERT, UPDATE, and DELETE operations that modify table data.
// These operations have specific semantics and syntax that need to be converted to SQLite equivalents.
//
// The transformer converts ZetaSQL DML statements by:
// - Handling INSERT ... VALUES vs INSERT ... SELECT patterns
// - Converting UPDATE statements with SET clauses and optional WHERE conditions
// - Transforming DELETE statements with WHERE clauses
// - Properly formatting table names and column references for SQLite
// - Ensuring expression transformations work correctly within DML contexts
//
// This transformer bridges the gap between ZetaSQL's resolved DML AST structure and
// the SQLite DML statement representation, ensuring all components are properly
// transformed and SQL generation produces valid SQLite syntax.
type DMLStmtTransformer struct {
	coordinator Coordinator // For recursive transformation of expressions and scans
}

// NewDMLStmtTransformer creates a new DML statement transformer
func NewDMLStmtTransformer(coordinator Coordinator) *DMLStmtTransformer {
	return &DMLStmtTransformer{
		coordinator: coordinator,
	}
}

// Transform converts DML statement data to appropriate SQL statement fragments
func (t *DMLStmtTransformer) Transform(data StatementData, ctx TransformContext) (SQLFragment, error) {

	switch data.Type {
	case StatementTypeInsert:
		return t.transformInsert(data, ctx)
	case StatementTypeUpdate:
		return t.transformUpdate(data, ctx)
	case StatementTypeDelete:
		return t.transformDelete(data, ctx)
	default:
		return nil, fmt.Errorf("unsupported DML statement type: %v", data.Type)
	}
}

// transformInsert converts INSERT statement data to InsertStatement
func (t *DMLStmtTransformer) transformInsert(data StatementData, ctx TransformContext) (SQLFragment, error) {
	if data.Insert == nil {
		return nil, fmt.Errorf("expected insert statement data, got nil")
	}

	insertData := data.Insert
	
	// Format column names
	columns := make([]string, 0, len(insertData.Columns))
	for _, col := range insertData.Columns {
		columns = append(columns, fmt.Sprintf("`%s`", col))
	}

	insertStmt := &InsertStatement{
		TableName: insertData.TableName,
		Columns:   columns,
	}

	if insertData.Query != nil {
		// INSERT ... SELECT
		fromItem, err := t.coordinator.TransformScan(*insertData.Query.FromClause, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform insert query scan: %w", err)
		}

		// Create SELECT statement for the query
		selectStmt := NewSelectStatement()
		selectStmt.FromClause = fromItem

		// Transform select list items
		for _, item := range insertData.Query.SelectList {
			expr, err := t.coordinator.TransformExpression(item.Expression, ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to transform insert select expression: %w", err)
			}

			selectStmt.SelectList = append(selectStmt.SelectList, &SelectListItem{
				Expression: expr,
				Alias:      item.Alias,
			})
		}

		insertStmt.Query = selectStmt
	} else {
		// INSERT ... VALUES
		rows := make([]SQLFragment, 0, len(insertData.Values))
		for _, rowValues := range insertData.Values {
			valueStrings := make([]string, 0, len(rowValues))
			for _, value := range rowValues {
				expr, err := t.coordinator.TransformExpression(value, ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to transform insert value: %w", err)
				}
				valueStrings = append(valueStrings, expr.String())
			}

			rows = append(rows, &SQLExpression{
				Type:  ExpressionTypeLiteral,
				Value: strings.Join(valueStrings, ","),
			})
		}
		insertStmt.Rows = rows
	}

	return insertStmt, nil
}

// transformUpdate converts UPDATE statement data to UpdateStatement
func (t *DMLStmtTransformer) transformUpdate(data StatementData, ctx TransformContext) (SQLFragment, error) {
	if data.Update == nil {
		return nil, fmt.Errorf("expected update statement data, got nil")
	}

	updateData := data.Update

	// Add table scan columns to the fragment context before transforming WHERE clause
	ctx.FragmentContext().AddAvailableColumnsForDML(updateData.TableScan)

	// Create table scan for the target table
	table := &FromItem{
		Type:      FromItemTypeTable,
		TableName: updateData.TableName,
	}

	updateStmt := &UpdateStatement{
		Table: table,
	}

	// Transform FROM clause if present (for JOINs in UPDATE)
	// Makes columns from the FromClause available to FragmentContext for the SetItems / WhereClause
	if updateData.FromClause != nil {
		fromClause, err := t.coordinator.TransformScan(*updateData.FromClause, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform update from clause: %w", err)
		}
		updateStmt.FromClause = fromClause
	}

	// Transform SET items
	setItems := make([]*SetItem, 0, len(updateData.SetItems))
	for _, item := range updateData.SetItems {
		value, err := t.coordinator.TransformExpression(item.Value, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform update value: %w", err)
		}

		setItems = append(setItems, &SetItem{
			Column: NewLiteralExpression(item.Column),
			Value:  value,
		})
	}
	updateStmt.SetItems = setItems

	// Transform WHERE clause if present
	if updateData.WhereClause != nil {
		whereExpr, err := t.coordinator.TransformExpression(*updateData.WhereClause, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform update where clause: %w", err)
		}
		updateStmt.WhereClause = whereExpr
	}
	return updateStmt, nil
}

// transformDelete converts DELETE statement data to DeleteStatement
func (t *DMLStmtTransformer) transformDelete(data StatementData, ctx TransformContext) (SQLFragment, error) {
	if data.Delete == nil {
		return nil, fmt.Errorf("expected delete statement data, got nil")
	}

	deleteData := data.Delete

	// Create table reference
	table := &FromItem{
		Type:      FromItemTypeTable,
		TableName: deleteData.TableName,
	}

	deleteStmt := &DeleteStatement{
		Table: table,
	}

	// Add table scan columns to the fragment context before transforming WHERE clause
	ctx.FragmentContext().AddAvailableColumnsForDML(deleteData.TableScan)

	// Transform WHERE clause if present
	if deleteData.WhereClause != nil {
		whereExpr, err := t.coordinator.TransformExpression(*deleteData.WhereClause, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform delete where clause: %w", err)
		}
		deleteStmt.WhereExpr = whereExpr
	}

	return deleteStmt, nil
}
