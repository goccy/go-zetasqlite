package internal

// flattenSelectForRecursiveCTE flattens a SELECT statement to expose the recursive table reference
// at the top level, as required by SQLite's recursive CTE implementation.
//
// It collapses chains of subqueries that only perform column aliasing without other operations
// (like GROUP BY, HAVING, LIMIT, ORDER BY, or SetOperations).
//
// Example transformation:
//
//	SELECT expr1 AS col1 FROM (
//	  SELECT expr2 AS expr1 FROM (
//	    SELECT col FROM table WHERE condition
//	  )
//	)
//
// Becomes:
//
//	SELECT expr1(expr2(col)) AS col1 FROM table WHERE condition
//
// This is essential for recursive CTEs because SQLite requires the recursive reference
// to be directly in the FROM clause, not buried in nested subqueries.
func flattenSelectForRecursiveCTE(stmt *SelectStatement) *SelectStatement {
	if stmt == nil {
		return stmt
	}

	// First, recursively flatten any JOINs in the FROM clause and collect column mappings
	if stmt.FromClause != nil && stmt.FromClause.Type == FromItemTypeJoin {
		joinAliasMap := make(map[string]*SQLExpression)
		stmt.FromClause = flattenJoinFromItem(stmt.FromClause, joinAliasMap)

		// Apply the JOIN column mappings to SELECT list, WHERE clause, and JOIN conditions
		if len(joinAliasMap) > 0 {
			for i, item := range stmt.SelectList {
				stmt.SelectList[i].Expression = substituteColumnRefs(item.Expression, joinAliasMap, "")
			}
			stmt.WhereClause = substituteColumnRefs(stmt.WhereClause, joinAliasMap, "")

			// Also substitute in JOIN conditions
			substituteInJoinConditions(stmt.FromClause, joinAliasMap)
		}
	}

	// Base case: if this statement doesn't have a FROM clause or it's not a subquery, nothing to flatten
	if stmt.FromClause == nil || stmt.FromClause.Type != FromItemTypeSubquery {
		return stmt
	}

	innerStmt := stmt.FromClause.Subquery
	if innerStmt == nil {
		return stmt
	}

	// Check if the inner statement can be flattened
	// We can only flatten if it has no aggregations, grouping, ordering, limits, or set operations
	if !canFlattenSelect(innerStmt) {
		return stmt
	}

	// Build a map of aliases to expressions from the inner SELECT list
	aliasMap := buildColumnAliasMap(innerStmt.SelectList)

	// Create the flattened statement
	flattened := &SelectStatement{
		// Combine WITH clauses from both levels, resolving conflicts by preferring inner CTEs
		WithClauses: mergeWithClauses(stmt.WithClauses, innerStmt.WithClauses),
		SelectType:  stmt.SelectType,
		// Use the inner FROM clause (this exposes the table reference)
		FromClause: innerStmt.FromClause,
		// Preserve GROUP BY, HAVING, ORDER BY, LIMIT from outer statement
		GroupByList:  stmt.GroupByList,
		HavingClause: stmt.HavingClause,
		OrderByList:  stmt.OrderByList,
		LimitClause:  stmt.LimitClause,
	}

	// Rewrite the SELECT list: substitute column references with their definitions
	flattened.SelectList = make([]*SelectListItem, len(stmt.SelectList))
	for i, item := range stmt.SelectList {
		flattened.SelectList[i] = &SelectListItem{
			Expression:      substituteColumnRefs(item.Expression, aliasMap, stmt.FromClause.Alias),
			Alias:           item.Alias,
			IsStarExpansion: item.IsStarExpansion,
			ExceptColumns:   item.ExceptColumns,
			ReplaceColumns:  item.ReplaceColumns,
		}
	}

	// Merge WHERE clauses from both levels
	flattened.WhereClause = mergeWhereConditions(stmt.WhereClause, innerStmt.WhereClause, aliasMap, stmt.FromClause.Alias)

	// Recursively flatten in case there are multiple levels of nesting
	return flattenSelectForRecursiveCTE(flattened)
}

// flattenJoinFromItem flattens subqueries within JOIN clauses to expose recursive references
// and populates aliasMap with column mappings from unwrapped subqueries
func flattenJoinFromItem(fromItem *FromItem, aliasMap map[string]*SQLExpression) *FromItem {
	if fromItem == nil || fromItem.Type != FromItemTypeJoin || fromItem.Join == nil {
		return fromItem
	}

	join := fromItem.Join

	// Recursively flatten left and right sides of the join
	join.Left = flattenFromItemInJoin(join.Left, aliasMap)
	join.Right = flattenFromItemInJoin(join.Right, aliasMap)

	return fromItem
}

// flattenFromItemInJoin flattens a FromItem that appears in a JOIN context
// and populates aliasMap with column mappings from unwrapped subqueries
func flattenFromItemInJoin(fromItem *FromItem, aliasMap map[string]*SQLExpression) *FromItem {
	if fromItem == nil {
		return nil
	}

	// If this is a subquery, try to unwrap it if it's just a pass-through
	if fromItem.Type == FromItemTypeSubquery && fromItem.Subquery != nil {
		subquery := fromItem.Subquery

		// Check if this subquery is just selecting * from another source
		// This is common for RecursiveRefScan which wraps the table reference in a subquery
		if canUnwrapSubquery(subquery) {
			// Replace with the inner FROM item, preserving the alias
			innerFrom := subquery.FromClause
			if innerFrom != nil {
				// Determine the final alias that will be used
				finalAlias := fromItem.Alias
				if finalAlias == "" {
					finalAlias = innerFrom.Alias
				} else {
					innerFrom.Alias = finalAlias
				}

				// Build column mappings from the subquery's SELECT list
				// Update table aliases to point to the final alias
				for _, item := range subquery.SelectList {
					if item.Alias != "" && item.Expression != nil {
						mappedExpr := copyExpression(item.Expression)
						// Update any table aliases in the expression to use the final alias
						updateTableAlias(mappedExpr, finalAlias)
						aliasMap[item.Alias] = mappedExpr
					}
				}

				// Recursively flatten in case there are more layers
				return flattenFromItemInJoin(innerFrom, aliasMap)
			}
		}
	}

	// If this is a JOIN, recursively flatten it
	if fromItem.Type == FromItemTypeJoin {
		return flattenJoinFromItem(fromItem, aliasMap)
	}

	return fromItem
}

// updateTableAlias updates all column references in an expression to use the specified table alias
func updateTableAlias(expr *SQLExpression, newAlias string) {
	if expr == nil {
		return
	}

	switch expr.Type {
	case ExpressionTypeColumn:
		// Update the table alias for this column reference
		expr.TableAlias = newAlias

	case ExpressionTypeStar:
		// Update the table alias for star expressions (e.g., table.*)
		expr.TableAlias = newAlias

	case ExpressionTypeFunction:
		if expr.FunctionCall != nil {
			for _, arg := range expr.FunctionCall.Arguments {
				updateTableAlias(arg, newAlias)
			}
		}

	case ExpressionTypeBinary:
		if expr.BinaryExpression != nil {
			updateTableAlias(expr.BinaryExpression.Left, newAlias)
			updateTableAlias(expr.BinaryExpression.Right, newAlias)
		}

	case ExpressionTypeUnary:
		// Handle unary expressions like NOT, -, +
		if expr.UnaryExpression != nil {
			updateTableAlias(expr.UnaryExpression.Expression, newAlias)
		}

	case ExpressionTypeList:
		// Handle list expressions like IN (...) or tuple expressions
		if expr.ListExpression != nil {
			for _, listExpr := range expr.ListExpression.Expressions {
				updateTableAlias(listExpr, newAlias)
			}
		}

	case ExpressionTypeCase:
		if expr.CaseExpression != nil {
			updateTableAlias(expr.CaseExpression.CaseExpr, newAlias)
			for _, when := range expr.CaseExpression.WhenClauses {
				updateTableAlias(when.Condition, newAlias)
				updateTableAlias(when.Result, newAlias)
			}
			updateTableAlias(expr.CaseExpression.ElseExpr, newAlias)
		}

	// Types with their own scope - don't traverse into them
	case ExpressionTypeSubquery, ExpressionTypeExists:
		return

	// Simple types with no nested expressions - no action needed
	case ExpressionTypeLiteral, ExpressionTypeParameter:
		return
	}
}

// canUnwrapSubquery checks if a subquery can be safely unwrapped in a JOIN context
func canUnwrapSubquery(stmt *SelectStatement) bool {
	if stmt == nil {
		return false
	}

	// Can only unwrap if there are no complex operations
	if len(stmt.GroupByList) > 0 || stmt.HavingClause != nil ||
		len(stmt.OrderByList) > 0 || stmt.LimitClause != nil ||
		stmt.SetOperation != nil || stmt.WhereClause != nil {
		return false
	}

	// Check if SELECT list is just pass-through columns (SELECT *)
	// or simple column references without transformation
	if len(stmt.SelectList) == 0 {
		return false
	}

	for _, item := range stmt.SelectList {
		if item.IsStarExpansion {
			continue
		}
		// Only allow simple column references
		if item.Expression == nil || item.Expression.Type != ExpressionTypeColumn {
			return false
		}
	}

	return true
}

// canFlattenSelect determines if a SELECT statement can be safely flattened.
// A statement can be flattened if it only does column projection without:
// - GROUP BY (aggregation changes semantics)
// - HAVING (requires grouping)
// - ORDER BY (affects result order)
// - LIMIT/OFFSET (affects result set size)
// - SetOperation (UNION/INTERSECT/EXCEPT have their own semantics)
func canFlattenSelect(stmt *SelectStatement) bool {
	if stmt == nil {
		return false
	}

	return len(stmt.GroupByList) == 0 &&
		stmt.HavingClause == nil &&
		len(stmt.OrderByList) == 0 &&
		stmt.LimitClause == nil &&
		stmt.SetOperation == nil
}

// mergeWithClauses combines WITH clauses from outer and inner SELECT statements,
// resolving name conflicts by preferring inner CTEs.
//
// When flattening nested queries, if both levels define CTEs with the same name,
// we keep the inner CTE because:
// 1. In SQL scoping, inner CTEs shadow outer ones within the inner scope
// 2. After flattening, the query references the inner CTE's definition
// 3. We're exposing the inner query's table references
//
// Example:
//
//	Outer: WITH x AS (SELECT 1) ...
//	Inner: WITH x AS (SELECT 2) ...
//	Merged: WITH x AS (SELECT 2) ... (inner wins)
func mergeWithClauses(outerClauses, innerClauses []*WithClause) []*WithClause {
	if len(outerClauses) == 0 {
		return innerClauses
	}
	if len(innerClauses) == 0 {
		return outerClauses
	}

	// Build a map of inner CTE names for O(1) duplicate detection
	innerNames := make(map[string]bool)
	for _, clause := range innerClauses {
		innerNames[clause.Name] = true
	}

	// Start with all inner clauses (they take precedence)
	result := make([]*WithClause, len(innerClauses))
	copy(result, innerClauses)

	// Add outer clauses that don't conflict with inner ones
	for _, outerClause := range outerClauses {
		if !innerNames[outerClause.Name] {
			result = append(result, outerClause)
		}
		// If conflict exists, prefer inner (skip outer clause)
	}

	return result
}

// buildColumnAliasMap creates a mapping from column aliases to their expressions.
// This is used during flattening to substitute column references.
func buildColumnAliasMap(selectList []*SelectListItem) map[string]*SQLExpression {
	result := make(map[string]*SQLExpression)
	for _, item := range selectList {
		if item.Alias != "" && item.Expression != nil {
			result[item.Alias] = item.Expression
		}
	}
	return result
}

// substituteColumnRefs rewrites an expression by replacing column references
// with their definitions from the alias map.
//
// For example, if we have:
//
//	Outer: SELECT f(col1) FROM (...)
//	Inner: SELECT x AS col1 FROM table
//	Map: {col1 -> x}
//
// Then f(col1) becomes f(x)
//
// This function includes protection against circular references in the alias map
// by limiting recursion depth.
func substituteColumnRefs(expr *SQLExpression, aliasMap map[string]*SQLExpression, tableAlias string) *SQLExpression {
	return substituteColumnRefsWithDepth(expr, aliasMap, tableAlias, 0)
}

// substituteColumnRefsWithDepth is the internal implementation of substituteColumnRefs
// with depth tracking to prevent infinite recursion from circular references.
func substituteColumnRefsWithDepth(expr *SQLExpression, aliasMap map[string]*SQLExpression, tableAlias string, depth int) *SQLExpression {
	// Protect against circular references or excessively deep expression trees
	const maxDepth = 100
	if depth > maxDepth {
		// Return expression as-is to prevent stack overflow
		return expr
	}

	if expr == nil {
		return nil
	}

	switch expr.Type {
	case ExpressionTypeColumn:
		// If this column reference matches our table alias OR has no table alias,
		// and is in the alias map, substitute it
		if (expr.TableAlias == tableAlias || expr.TableAlias == "") {
			if substitution, found := aliasMap[expr.Value]; found {
				// Return a copy of the substitution to avoid mutation
				return copyExpression(substitution)
			}
		}
		// If no substitution found, return the expression as-is
		return expr

	case ExpressionTypeFunction:
		if expr.FunctionCall == nil {
			return expr
		}
		// Recursively substitute in function arguments
		newArgs := make([]*SQLExpression, len(expr.FunctionCall.Arguments))
		for i, arg := range expr.FunctionCall.Arguments {
			newArgs[i] = substituteColumnRefsWithDepth(arg, aliasMap, tableAlias, depth+1)
		}
		return &SQLExpression{
			Type: ExpressionTypeFunction,
			FunctionCall: &FunctionCall{
				Name:       expr.FunctionCall.Name,
				Arguments:  newArgs,
				IsDistinct: expr.FunctionCall.IsDistinct,
				WindowSpec: expr.FunctionCall.WindowSpec,
			},
			Collation: expr.Collation,
		}

	case ExpressionTypeBinary:
		if expr.BinaryExpression == nil {
			return expr
		}
		// Recursively substitute in both operands
		return &SQLExpression{
			Type: ExpressionTypeBinary,
			BinaryExpression: &BinaryExpression{
				Left:     substituteColumnRefsWithDepth(expr.BinaryExpression.Left, aliasMap, tableAlias, depth+1),
				Operator: expr.BinaryExpression.Operator,
				Right:    substituteColumnRefsWithDepth(expr.BinaryExpression.Right, aliasMap, tableAlias, depth+1),
			},
			Collation: expr.Collation,
		}

	case ExpressionTypeUnary:
		if expr.UnaryExpression == nil {
			return expr
		}
		// Recursively substitute in unary operand
		return &SQLExpression{
			Type: ExpressionTypeUnary,
			UnaryExpression: &UnaryExpression{
				Operator:   expr.UnaryExpression.Operator,
				Expression: substituteColumnRefsWithDepth(expr.UnaryExpression.Expression, aliasMap, tableAlias, depth+1),
			},
			Collation: expr.Collation,
		}

	case ExpressionTypeList:
		if expr.ListExpression == nil {
			return expr
		}
		// Recursively substitute in list elements
		newExpressions := make([]*SQLExpression, len(expr.ListExpression.Expressions))
		for i, listExpr := range expr.ListExpression.Expressions {
			newExpressions[i] = substituteColumnRefsWithDepth(listExpr, aliasMap, tableAlias, depth+1)
		}
		return &SQLExpression{
			Type: ExpressionTypeList,
			ListExpression: &ListExpression{
				Expressions: newExpressions,
			},
			Collation: expr.Collation,
		}

	case ExpressionTypeCase:
		if expr.CaseExpression == nil {
			return expr
		}
		// Recursively substitute in CASE expression components
		newWhenClauses := make([]*WhenClause, len(expr.CaseExpression.WhenClauses))
		for i, when := range expr.CaseExpression.WhenClauses {
			newWhenClauses[i] = &WhenClause{
				Condition: substituteColumnRefsWithDepth(when.Condition, aliasMap, tableAlias, depth+1),
				Result:    substituteColumnRefsWithDepth(when.Result, aliasMap, tableAlias, depth+1),
			}
		}
		return &SQLExpression{
			Type: ExpressionTypeCase,
			CaseExpression: &CaseExpression{
				CaseExpr:    substituteColumnRefsWithDepth(expr.CaseExpression.CaseExpr, aliasMap, tableAlias, depth+1),
				WhenClauses: newWhenClauses,
				ElseExpr:    substituteColumnRefsWithDepth(expr.CaseExpression.ElseExpr, aliasMap, tableAlias, depth+1),
			},
			Collation: expr.Collation,
		}

	case ExpressionTypeExists:
		// EXISTS subqueries are not substituted
		return expr

	case ExpressionTypeSubquery:
		// Subquery expressions are not substituted
		return expr

	case ExpressionTypeLiteral, ExpressionTypeParameter, ExpressionTypeStar:
		// Literals, parameters, and star expressions don't need substitution
		return expr

	default:
		// Unknown expression type - return as-is
		return expr
	}
}

// copyExpression creates a deep copy of an SQL expression to avoid mutation
func copyExpression(expr *SQLExpression) *SQLExpression {
	if expr == nil {
		return nil
	}

	copied := &SQLExpression{
		Type:       expr.Type,
		Value:      expr.Value,
		Alias:      expr.Alias,
		TableAlias: expr.TableAlias,
		Collation:  expr.Collation,
	}

	if expr.BinaryExpression != nil {
		copied.BinaryExpression = &BinaryExpression{
			Left:     copyExpression(expr.BinaryExpression.Left),
			Operator: expr.BinaryExpression.Operator,
			Right:    copyExpression(expr.BinaryExpression.Right),
		}
	}

	if expr.FunctionCall != nil {
		copiedArgs := make([]*SQLExpression, len(expr.FunctionCall.Arguments))
		for i, arg := range expr.FunctionCall.Arguments {
			copiedArgs[i] = copyExpression(arg)
		}
		copied.FunctionCall = &FunctionCall{
			Name:       expr.FunctionCall.Name,
			Arguments:  copiedArgs,
			IsDistinct: expr.FunctionCall.IsDistinct,
			WindowSpec: expr.FunctionCall.WindowSpec,
		}
	}

	if expr.UnaryExpression != nil {
		copied.UnaryExpression = &UnaryExpression{
			Operator:   expr.UnaryExpression.Operator,
			Expression: copyExpression(expr.UnaryExpression.Expression),
		}
	}

	if expr.ListExpression != nil {
		copiedExpressions := make([]*SQLExpression, len(expr.ListExpression.Expressions))
		for i, listExpr := range expr.ListExpression.Expressions {
			copiedExpressions[i] = copyExpression(listExpr)
		}
		copied.ListExpression = &ListExpression{
			Expressions: copiedExpressions,
		}
	}

	if expr.CaseExpression != nil {
		copiedWhenClauses := make([]*WhenClause, len(expr.CaseExpression.WhenClauses))
		for i, when := range expr.CaseExpression.WhenClauses {
			copiedWhenClauses[i] = &WhenClause{
				Condition: copyExpression(when.Condition),
				Result:    copyExpression(when.Result),
			}
		}
		copied.CaseExpression = &CaseExpression{
			CaseExpr:    copyExpression(expr.CaseExpression.CaseExpr),
			WhenClauses: copiedWhenClauses,
			ElseExpr:    copyExpression(expr.CaseExpression.ElseExpr),
		}
	}

	if expr.ExistsExpr != nil {
		copied.ExistsExpr = &ExistsExpression{
			Subquery: expr.ExistsExpr.Subquery, // Subqueries are not deep-copied
		}
	}

	if expr.Subquery != nil {
		copied.Subquery = expr.Subquery // Subqueries are not deep-copied
	}

	return copied
}

// substituteInJoinConditions recursively substitutes column references in JOIN conditions
func substituteInJoinConditions(fromItem *FromItem, aliasMap map[string]*SQLExpression) {
	if fromItem == nil || fromItem.Type != FromItemTypeJoin || fromItem.Join == nil {
		return
	}

	join := fromItem.Join

	// Substitute in the JOIN condition
	if join.Condition != nil {
		join.Condition = substituteColumnRefs(join.Condition, aliasMap, "")
	}

	// Recursively process nested JOINs
	if join.Left != nil && join.Left.Type == FromItemTypeJoin {
		substituteInJoinConditions(join.Left, aliasMap)
	}
	if join.Right != nil && join.Right.Type == FromItemTypeJoin {
		substituteInJoinConditions(join.Right, aliasMap)
	}
}

// mergeWhereConditions combines WHERE clauses from outer and inner SELECT statements.
// It substitutes column references in the outer WHERE clause using the alias map,
// then combines both conditions with AND.
func mergeWhereConditions(outerWhere, innerWhere *SQLExpression, aliasMap map[string]*SQLExpression, tableAlias string) *SQLExpression {
	// Substitute aliases in outer WHERE clause
	if outerWhere != nil {
		outerWhere = substituteColumnRefs(outerWhere, aliasMap, tableAlias)
	}

	// Combine with AND if both exist
	if outerWhere != nil && innerWhere != nil {
		return &SQLExpression{
			Type: ExpressionTypeBinary,
			BinaryExpression: &BinaryExpression{
				Left:     innerWhere,
				Operator: "AND",
				Right:    outerWhere,
			},
		}
	}

	// Return whichever one exists (or nil if both are nil)
	if outerWhere != nil {
		return outerWhere
	}
	return innerWhere
}
