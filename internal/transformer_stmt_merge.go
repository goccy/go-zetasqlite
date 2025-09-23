package internal

import (
	"fmt"
	"strings"

	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// MergeStmtTransformer handles transformation of MERGE statement nodes from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, MERGE statements provide a way to conditionally INSERT, UPDATE, or DELETE
// rows based on whether they match between a target table and a source table/query. Since SQLite
// doesn't have native MERGE support, this transformer converts MERGE statements into a series of
// SQLite statements that achieve equivalent behavior.
//
// The transformation strategy is:
// 1. Create a temporary table with a FULL OUTER JOIN of target and source tables
// 2. Generate conditional INSERT/UPDATE/DELETE statements based on WHEN clauses
// 3. Clean up the temporary table
//
// This maintains the same semantics as the original visitor pattern implementation while
// integrating with the new transformer architecture.
type MergeStmtTransformer struct {
	coordinator Coordinator // For recursive transformation of expressions and scans
}

// NewMergeStmtTransformer creates a new MERGE statement transformer
func NewMergeStmtTransformer(coordinator Coordinator) *MergeStmtTransformer {
	return &MergeStmtTransformer{
		coordinator: coordinator,
	}
}

// Transform converts MERGE statement data to a collection of SQL statements that simulate MERGE behavior
func (t *MergeStmtTransformer) Transform(data StatementData, ctx TransformContext) (SQLFragment, error) {
	if data.Type != StatementTypeMerge || data.Merge == nil {
		return nil, fmt.Errorf("expected MERGE statement data, got %v", data.Type)
	}

	mergeData := data.Merge

	// Transform target table scan
	targetTable, err := t.coordinator.TransformScan(*mergeData.TargetScan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform merge target table: %w", err)
	}

	// Transform source table/query scan
	sourceTable, err := t.coordinator.TransformScan(*mergeData.SourceScan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform merge source table: %w", err)
	}

	// Transform merge expression (join condition)
	mergeExpr, err := t.coordinator.TransformExpression(mergeData.MergeExpr, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform merge expression: %w", err)
	}

	// Validate merge expression is an equality condition (like the original implementation)
	if err := t.validateMergeExpression(mergeData.MergeExpr); err != nil {
		return nil, fmt.Errorf("unsupported merge expression: %w", err)
	}

	// Extract source and target column references from merge expression
	sourceColumn, targetColumn, err := t.extractMergeColumns(mergeData, mergeData.MergeExpr, mergeData.TargetTable)
	if err != nil {
		return nil, fmt.Errorf("failed to extract merge columns: %w", err)
	}

	// Create temporary merged table with FULL OUTER JOIN
	createTableStmt, columnMapping, err := t.createMergedTableStatement("zetasqlite_merged_table", sourceTable, targetTable, mergeData.SourceScan, mergeData.TargetScan, mergeExpr, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create merged table statement: %w", err)
	}

	mergedTableSourceColumnName, found := columnMapping.LookupName(sourceColumn)
	if !found {
		return nil, fmt.Errorf("failed to lookup merged source column name")
	}
	mergedTableTargetColumnName, found := columnMapping.LookupName(targetColumn)
	if !found {
		return nil, fmt.Errorf("failed to lookup merged target column name")
	}

	// Build the list of SQL statements
	var statements []string

	// 1. Create temporary merged table
	statements = append(statements, createTableStmt.String())

	// 2. Generate conditional statements based on WHEN clauses
	for _, whenClause := range mergeData.WhenClauses {
		stmt, err := t.transformWhenClause(
			whenClause, mergeData.TargetTable,
			targetColumn.Name,
			sourceTable,
			mergedTableSourceColumnName,
			mergedTableTargetColumnName,
			columnMapping,
			ctx,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to transform WHEN clause: %w", err)
		}
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	// 3. Drop temporary table
	statements = append(statements, "DROP TABLE zetasqlite_merged_table")

	// Create a compound statement fragment that represents all the statements
	return NewCompoundSQLFragment(statements), nil
}

// validateMergeExpression ensures the merge expression is a supported equality condition
func (t *MergeStmtTransformer) validateMergeExpression(mergeExpr ExpressionData) error {
	if mergeExpr.Type != ExpressionTypeFunction || mergeExpr.Function == nil {
		return fmt.Errorf("merge expression must be a function call")
	}

	// Check if it's an equality function
	if mergeExpr.Function.Name != "zetasqlite_equal" {
		return fmt.Errorf("currently MERGE expression is supported equal expression only")
	}

	if len(mergeExpr.Function.Arguments) != 2 {
		return fmt.Errorf("unexpected MERGE expression column num. expected 2 columns but specified %d", len(mergeExpr.Function.Arguments))
	}

	// Validate both arguments are column references
	for i, arg := range mergeExpr.Function.Arguments {
		if arg.Type != ExpressionTypeColumn {
			return fmt.Errorf("unexpected MERGE expression. expected column reference but got %v at position %d", arg.Type, i)
		}
	}

	return nil
}

// extractMergeColumns extracts source and target column references from the merge expression
func (t *MergeStmtTransformer) extractMergeColumns(mergeData *MergeData, mergeExpr ExpressionData, targetTableName string) (*ColumnData, *ColumnData, error) {
	if mergeExpr.Type != ExpressionTypeFunction || mergeExpr.Function == nil {
		return &ColumnData{}, &ColumnData{}, fmt.Errorf("invalid merge expression")
	}

	args := mergeExpr.Function.Arguments
	if len(args) != 2 {
		return &ColumnData{}, &ColumnData{}, fmt.Errorf("merge expression must have exactly 2 arguments")
	}

	colA := args[0]
	colB := args[1]

	// Determine which column belongs to target table and which to source table
	// (following the logic from the original implementation)
	if colA.Type == ExpressionTypeColumn && colA.Column != nil {
		if colA.Column.TableName == targetTableName {
			source := mergeData.SourceScan.FindColumnByID(colB.Column.ColumnID)
			target := mergeData.TargetScan.FindColumnByID(colA.Column.ColumnID)
			return source, target, nil // source, target
		}
	}

	if colB.Type == ExpressionTypeColumn && colB.Column != nil {
		source := mergeData.SourceScan.FindColumnByID(colA.Column.ColumnID)
		target := mergeData.TargetScan.FindColumnByID(colB.Column.ColumnID)
		if colB.Column.TableName == targetTableName {
			return source, target, nil // source, target
		} // source, target
	}

	return &ColumnData{}, &ColumnData{}, fmt.Errorf("could not determine source and target columns")
}

// transformWhenClause transforms a single WHEN clause into an appropriate SQL statement
func (t *MergeStmtTransformer) transformWhenClause(
	whenClause *MergeWhenClauseData,
	targetTableName string,
	targetColumnName string,
	sourceTable SQLFragment,
	mergedTableSourceColumnName,
	mergedTableTargetColumnName string,
	columnMapping *ColumnMapping,
	ctx TransformContext,
) (string, error) {

	// Generate the appropriate FROM clause for this match type
	var fromFilter *SQLExpression
	switch whenClause.MatchType {
	case ast.MatchTypeMatched:
		// Both target and source table have matching records
		// TODO: this used to use targetColumnName?
		fromFilter = NewBinaryExpression(
			NewBinaryExpression(NewColumnExpression(mergedTableSourceColumnName), "=", NewColumnExpression(targetColumnName)),
			"AND",
			NewBinaryExpression(NewColumnExpression(mergedTableTargetColumnName), "=", NewColumnExpression(targetColumnName)),
		)
	case ast.MatchTypeNotMatchedBySource:
		// Target table has record but source table doesn't
		fromFilter = NewBinaryExpression(
			NewBinaryExpression(NewColumnExpression(mergedTableTargetColumnName), "IS NOT", NewLiteralExpression("NULL")),
			"AND",
			NewBinaryExpression(NewColumnExpression(mergedTableSourceColumnName), "IS", NewLiteralExpression("NULL")),
		)
	case ast.MatchTypeNotMatchedByTarget:
		// Source table has record but target table doesn't
		fromFilter = NewBinaryExpression(
			NewBinaryExpression(NewColumnExpression(mergedTableTargetColumnName), "IS", NewLiteralExpression("NULL")),
			"AND",
			NewBinaryExpression(NewColumnExpression(mergedTableSourceColumnName), "IS NOT", NewLiteralExpression("NULL")),
		)
	default:
		return "", fmt.Errorf("unsupported match type: %v", whenClause.MatchType)
	}

	// Create WHERE clause with existence check
	subq := NewSelectStatement()
	subq.FromClause = &FromItem{Type: FromItemTypeTable, TableName: "zetasqlite_merged_table"}
	subq.SelectList = []*SelectListItem{{Expression: NewColumnExpression(mergedTableSourceColumnName)}, {Expression: NewColumnExpression(mergedTableTargetColumnName)}}
	subq.WhereClause = fromFilter
	existsStmt := NewExistsExpression(subq)

	// Generate the appropriate statement based on action type
	switch whenClause.ActionType {
	case ast.ActionTypeInsert:
		return t.transformInsertAction(whenClause, targetTableName, sourceTable, existsStmt.String(), columnMapping, ctx)
	case ast.ActionTypeUpdate:
		return t.transformUpdateAction(whenClause, targetTableName, fromFilter.String(), columnMapping, ctx)
	case ast.ActionTypeDelete:
		return (&DeleteStatement{
			Table:     &FromItem{TableName: targetTableName},
			WhereExpr: existsStmt,
		}).String(), nil
	default:
		return "", fmt.Errorf("unsupported action type: %v", whenClause.ActionType)
	}
}

// transformInsertAction transforms an INSERT action within a WHEN clause
func (t *MergeStmtTransformer) transformInsertAction(whenClause *MergeWhenClauseData, targetTableName string,
	sourceTable SQLFragment, whereStmt string, columnMapping *ColumnMapping, ctx TransformContext) (string, error) {

	values := make([]string, 0, len(whenClause.InsertValues))
	columns := make([]string, 0, len(whenClause.InsertColumns))
	for i, col := range whenClause.InsertColumns {
		// Format column names
		columns = append(columns, fmt.Sprintf("`%s`", col.Name))
		// Transform INSERT values
		value := whenClause.InsertValues[i]
		valueExpr, err := t.coordinator.TransformExpression(value, ctx)
		if err != nil {
			return "", fmt.Errorf("failed to transform insert value: %w", err)
		}

		valueExpr.Alias = col.Name
		values = append(values, valueExpr.String())
	}

	// Build INSERT statement
	return fmt.Sprintf(
		"INSERT INTO `%s` (%s) SELECT %s FROM %s",
		targetTableName,
		strings.Join(columns, ","),
		strings.Join(values, ","),
		sourceTable.String(),
	), nil
}

// transformUpdateAction transforms an UPDATE action within a WHEN clause
func (t *MergeStmtTransformer) transformUpdateAction(whenClause *MergeWhenClauseData, targetTableName, whereStmt string,
	columnMapping *ColumnMapping, ctx TransformContext) (string, error) {

	// Transform SET items
	setItems := make([]string, 0, len(whenClause.SetItems))
	for _, item := range whenClause.SetItems {
		valueExpr, err := t.coordinator.TransformExpression(item.Value, ctx)
		if err != nil {
			return "", fmt.Errorf("failed to transform update value: %w", err)
		}

		// Replace column references with mapped names from the temporary table
		valueString := valueExpr.String()
		for column, mapping := range columnMapping.AllColumnMap {
			expr, err := t.coordinator.TransformExpression(ExpressionData{
				Type:   ExpressionTypeColumn,
				Column: &ColumnRefData{ColumnID: column.ID},
			}, ctx)
			if err != nil {
				return "", fmt.Errorf("failed to transform update value: %w", err)
			}

			valueString = strings.ReplaceAll(valueString, expr.String(), fmt.Sprintf("`%s`", mapping))
		}

		setItems = append(setItems, fmt.Sprintf("`%s`= %s", item.Column, valueString))
	}

	// Build UPDATE statement
	return fmt.Sprintf(
		"UPDATE `%s` SET %s FROM zetasqlite_merged_table WHERE %s",
		targetTableName,
		strings.Join(setItems, ","),
		whereStmt,
	), nil
}

// CreateMergedTableStatement creates a CREATE TABLE AS SELECT statement using the merged table pattern
// for MERGE operations with distinct column naming. This generates the SQL pattern:
// CREATE TABLE tableName AS SELECT DISTINCT sourceCol1 AS merged_sourceCol1, targetCol1 AS merged_targetCol1, ... FROM (
//
//	SELECT * FROM sourceTable LEFT JOIN targetTable ON joinCondition
//	UNION ALL
//	SELECT * FROM targetTable LEFT JOIN sourceTable ON joinCondition
//
// )
// Returns the CreateTableStatement and a mapping of original -> new column names
func (t *MergeStmtTransformer) createMergedTableStatement(tableName string, sourceTable, targetTable *FromItem, sourceTableData, targetTableData *ScanData, joinCondition *SQLExpression, ctx TransformContext) (*CreateTableStatement, *ColumnMapping, error) {
	// Create distinct column mappings
	columnMapping := t.createColumnMapping(sourceTableData.ColumnList, targetTableData.ColumnList)

	// Create the inner subquery with LEFT JOIN and explicit column selection
	leftJoin, err := t.createJoinWithColumnMapping(&FromItem{
		Type: FromItemTypeJoin,
		Join: &JoinClause{
			Type:      JoinTypeLeft,
			Left:      sourceTable,
			Right:     targetTable,
			Condition: joinCondition,
		},
	}, columnMapping, ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create left join: %w", err)
	}

	rightJoin, err := t.createJoinWithColumnMapping(&FromItem{
		Type: FromItemTypeJoin,
		Join: &JoinClause{
			Type:      JoinTypeLeft,
			Left:      targetTable,
			Right:     sourceTable,
			Condition: joinCondition,
		},
	}, columnMapping, ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create right join: %w", err)
	}

	// Create the UNION ALL operation
	unionOperation := &SetOperation{
		Type:     "UNION",
		Modifier: "ALL",
		Items:    []*SelectStatement{leftJoin, rightJoin},
	}

	// Create the outer subquery with UNION ALL
	unionStatement := NewSelectStatement()
	unionStatement.SetOperation = unionOperation

	// Create the final SELECT DISTINCT * from the UNION subquery
	distinctQuery := &SelectStatement{
		SelectType: SelectTypeDistinct,
		SelectList: []*SelectListItem{
			{Expression: NewStarExpression()},
		},
		FromClause: NewSubqueryFromItem(unionStatement, "merged_union"),
	}

	// Create the CREATE TABLE AS SELECT statement
	return &CreateTableStatement{
		TableName: tableName,
		AsSelect:  distinctQuery,
	}, columnMapping, nil
}

// createColumnMapping creates distinct column names and mappings
func (t *MergeStmtTransformer) createColumnMapping(sourceColumns, targetColumns []*ColumnData) *ColumnMapping {
	mapping := &ColumnMapping{
		SourceColumnMap: make(map[*ColumnData]string),
		TargetColumnMap: make(map[*ColumnData]string),
		AllColumnMap:    make(map[*ColumnData]string),
	}

	usedNames := make(map[string]bool)

	// Process source columns
	for _, col := range sourceColumns {
		newName := fmt.Sprintf("merged_source_%s", col.Name)
		counter := 1
		originalNewName := newName

		// Ensure uniqueness
		for usedNames[newName] {
			newName = fmt.Sprintf("%s_%d", originalNewName, counter)
			counter++
		}

		usedNames[newName] = true
		mapping.SourceColumnMap[col] = newName
		mapping.AllColumnMap[col] = newName
	}

	// Process target columns
	for _, col := range targetColumns {
		newName := fmt.Sprintf("merged_target_%s", col.Name)
		counter := 1
		originalNewName := newName

		// Ensure uniqueness
		for usedNames[newName] {
			newName = fmt.Sprintf("%s_%d", originalNewName, counter)
			counter++
		}

		usedNames[newName] = true
		mapping.TargetColumnMap[col] = newName
		mapping.AllColumnMap[col] = newName
	}

	return mapping
}

// createJoinWithColumnMapping creates a SELECT statement with explicit column mapping for joins
func (t *MergeStmtTransformer) createJoinWithColumnMapping(joinFromItem *FromItem, mapping *ColumnMapping, ctx TransformContext) (*SelectStatement, error) {
	stmt := NewSelectStatement()
	stmt.FromClause = joinFromItem

	// Build explicit SELECT list with column mappings
	stmt.SelectList = []*SelectListItem{}

	// Add source columns
	for col, newName := range mapping.SourceColumnMap {
		exprData := ExpressionData{
			Type:   ExpressionTypeColumn,
			Column: &ColumnRefData{ColumnID: col.ID},
		}

		expr, err := t.coordinator.TransformExpression(exprData, ctx)
		if err != nil {
			return nil, err
		}

		stmt.SelectList = append(stmt.SelectList, &SelectListItem{
			Expression: expr,
			Alias:      newName,
		})
	}

	// Add target columns
	for col, newName := range mapping.TargetColumnMap {
		exprData := ExpressionData{
			Type:   ExpressionTypeColumn,
			Column: &ColumnRefData{ColumnID: col.ID},
		}

		expr, err := t.coordinator.TransformExpression(exprData, ctx)
		if err != nil {
			return nil, err
		}

		stmt.SelectList = append(stmt.SelectList, &SelectListItem{
			Expression: expr,
			Alias:      newName,
		})
	}

	return stmt, nil
}

// ColumnMapping represents the mapping between original and new column names
type ColumnMapping struct {
	SourceColumnMap map[*ColumnData]string // original column -> new column name for source table
	TargetColumnMap map[*ColumnData]string // original column -> new column name for target table
	AllColumnMap    map[*ColumnData]string // all original column  -> new column names
}

func (m ColumnMapping) LookupName(column *ColumnData) (string, bool) {
	name, found := m.AllColumnMap[column]
	return name, found
}
