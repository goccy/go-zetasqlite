package zetasqlite

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
)

type Node interface {
	FormatSQL(context.Context) (string, error)
}

func newNode(node ast.Node) Node {
	if node == nil {
		return nil
	}
	switch node.Kind() {
	case ast.Literal:
		return newLiteralNode(node.(*ast.LiteralNode))
	case ast.Parameter:
		return newParameterNode(node.(*ast.ParameterNode))
	case ast.ExpressionColumn:
		return newExpressionColumnNode(node.(*ast.ExpressionColumnNode))
	case ast.ColumnRef:
		return newColumnRefNode(node.(*ast.ColumnRefNode))
	case ast.Constant:
		return newConstantNode(node.(*ast.ConstantNode))
	case ast.SystemVariable:
		return newSystemVariableNode(node.(*ast.SystemVariableNode))
	case ast.InlineLambda:
		return newInlineLambdaNode(node.(*ast.InlineLambdaNode))
	case ast.FilterFieldArg:
		return newFilterFieldArgNode(node.(*ast.FilterFieldArgNode))
	case ast.FilterField:
		return newFilterFieldNode(node.(*ast.FilterFieldNode))
	case ast.FunctionCall:
		return newFunctionCallNode(node.(*ast.FunctionCallNode))
	case ast.AggregateFunctionCall:
		return newAggregateFunctionCallNode(node.(*ast.AggregateFunctionCallNode))
	case ast.AnalyticFunctionCall:
		return newAnalyticFunctionCallNode(node.(*ast.AnalyticFunctionCallNode))
	case ast.ExtendedCastElement:
		return newExtendedCastElementNode(node.(*ast.ExtendedCastElementNode))
	case ast.ExtendedCast:
		return newExtendedCastNode(node.(*ast.ExtendedCastNode))
	case ast.Cast:
		return newCastNode(node.(*ast.CastNode))
	case ast.MakeStruct:
		return newMakeStructNode(node.(*ast.MakeStructNode))
	case ast.MakeProto:
		return newMakeProtoNode(node.(*ast.MakeProtoNode))
	case ast.MakeProtoField:
		return newMakeProtoFieldNode(node.(*ast.MakeProtoFieldNode))
	case ast.GetStructField:
		return newGetStructFieldNode(node.(*ast.GetStructFieldNode))
	case ast.GetProtoField:
		return newGetProtoFieldNode(node.(*ast.GetProtoFieldNode))
	case ast.GetJsonField:
		return newGetJsonFieldNode(node.(*ast.GetJsonFieldNode))
	case ast.Flatten:
		return newFlattenNode(node.(*ast.FlattenNode))
	case ast.FlattenedArg:
		return newFlattenedArgNode(node.(*ast.FlattenedArgNode))
	case ast.ReplaceFieldItem:
		return newReplaceFieldItemNode(node.(*ast.ReplaceFieldItemNode))
	case ast.ReplaceField:
		return newReplaceFieldNode(node.(*ast.ReplaceFieldNode))
	case ast.SubqueryExpr:
		return newSubqueryExprNode(node.(*ast.SubqueryExprNode))
	case ast.LetExpr:
		return newLetExprNode(node.(*ast.LetExprNode))
	case ast.Model:
		return newModelNode(node.(*ast.ModelNode))
	case ast.Connection:
		return newConnectionNode(node.(*ast.ConnectionNode))
	case ast.Descriptor:
		return newDescriptorNode(node.(*ast.DescriptorNode))
	case ast.SingleRowScan:
		return newSingleRowScanNode(node.(*ast.SingleRowScanNode))
	case ast.TableScan:
		return newTableScanNode(node.(*ast.TableScanNode))
	case ast.JoinScan:
		return newJoinScanNode(node.(*ast.JoinScanNode))
	case ast.ArrayScan:
		return newArrayScanNode(node.(*ast.ArrayScanNode))
	case ast.ColumnHolder:
		return newColumnHolderNode(node.(*ast.ColumnHolderNode))
	case ast.FilterScan:
		return newFilterScanNode(node.(*ast.FilterScanNode))
	case ast.GroupingSet:
		return newGroupingSetNode(node.(*ast.GroupingSetNode))
	case ast.AggregateScan:
		return newAggregateScanNode(node.(*ast.AggregateScanNode))
	case ast.AnonymizedAggregateScan:
		return newAnonymizedAggregateScanNode(node.(*ast.AnonymizedAggregateScanNode))
	case ast.SetOperationItem:
		return newSetOperationItemNode(node.(*ast.SetOperationItemNode))
	case ast.SetOperationScan:
		return newSetOperationScanNode(node.(*ast.SetOperationScanNode))
	case ast.OrderByScan:
		return newOrderByScanNode(node.(*ast.OrderByScanNode))
	case ast.LimitOffsetScan:
		return newLimitOffsetScanNode(node.(*ast.LimitOffsetScanNode))
	case ast.WithRefScan:
		return newWithRefScanNode(node.(*ast.WithRefScanNode))
	case ast.AnalyticScan:
		return newAnalyticScanNode(node.(*ast.AnalyticScanNode))
	case ast.SampleScan:
		return newSampleScanNode(node.(*ast.SampleScanNode))
	case ast.ComputedColumn:
		return newComputedColumnNode(node.(*ast.ComputedColumnNode))
	case ast.OrderByItem:
		return newOrderByItemNode(node.(*ast.OrderByItemNode))
	case ast.ColumnAnnotations:
		return newColumnAnnotationsNode(node.(*ast.ColumnAnnotationsNode))
	case ast.GeneratedColumnInfo:
		return newGeneratedColumnInfoNode(node.(*ast.GeneratedColumnInfoNode))
	case ast.ColumnDefaultValue:
		return newColumnDefaultValueNode(node.(*ast.ColumnDefaultValueNode))
	case ast.ColumnDefinition:
		return newColumnDefinitionNode(node.(*ast.ColumnDefinitionNode))
	case ast.PrimaryKey:
		return newPrimaryKeyNode(node.(*ast.PrimaryKeyNode))
	case ast.ForeignKey:
		return newForeignKeyNode(node.(*ast.ForeignKeyNode))
	case ast.CheckConstraint:
		return newCheckConstraintNode(node.(*ast.CheckConstraintNode))
	case ast.OutputColumn:
		return newOutputColumnNode(node.(*ast.OutputColumnNode))
	case ast.ProjectScan:
		return newProjectScanNode(node.(*ast.ProjectScanNode))
	case ast.TVFScan:
		return newTVFScanNode(node.(*ast.TVFScanNode))
	case ast.GroupRowsScan:
		return newGroupRowsScanNode(node.(*ast.GroupRowsScanNode))
	case ast.FunctionArgument:
		return newFunctionArgumentNode(node.(*ast.FunctionArgumentNode))
	case ast.ExplainStmt:
		return newExplainStmtNode(node.(*ast.ExplainStmtNode))
	case ast.QueryStmt:
		return newQueryStmtNode(node.(*ast.QueryStmtNode))
	case ast.CreateDatabaseStmt:
		return newCreateDatabaseStmtNode(node.(*ast.CreateDatabaseStmtNode))
	case ast.IndexItem:
		return newIndexItemNode(node.(*ast.IndexItemNode))
	case ast.UnnestItem:
		return newUnnestItemNode(node.(*ast.UnnestItemNode))
	case ast.CreateIndexStmt:
		return newCreateIndexStmtNode(node.(*ast.CreateIndexStmtNode))
	case ast.CreateSchemaStmt:
		return newCreateSchemaStmtNode(node.(*ast.CreateSchemaStmtNode))
	case ast.CreateTableStmt:
		return newCreateTableStmtNode(node.(*ast.CreateTableStmtNode))
	case ast.CreateTableAsSelectStmt:
		return newCreateTableAsSelectStmtNode(node.(*ast.CreateTableAsSelectStmtNode))
	case ast.CreateModelStmt:
		return newCreateModelStmtNode(node.(*ast.CreateModelStmtNode))
	case ast.CreateViewStmt:
		return newCreateViewStmtNode(node.(*ast.CreateViewStmtNode))
	case ast.WithPartitionColumns:
		return newWithPartitionColumnsNode(node.(*ast.WithPartitionColumnsNode))
	case ast.CreateSnapshotTableStmt:
		return newCreateSnapshotTableStmtNode(node.(*ast.CreateSnapshotTableStmtNode))
	case ast.CreateExternalTableStmt:
		return newCreateExternalTableStmtNode(node.(*ast.CreateExternalTableStmtNode))
	case ast.ExportModelStmt:
		return newExportModelStmtNode(node.(*ast.ExportModelStmtNode))
	case ast.ExportDataStmt:
		return newExportDataStmtNode(node.(*ast.ExportDataStmtNode))
	case ast.DefineTableStmt:
		return newDefineTableStmtNode(node.(*ast.DefineTableStmtNode))
	case ast.DescribeStmt:
		return newDescribeStmtNode(node.(*ast.DescribeStmtNode))
	case ast.ShowStmt:
		return newShowStmtNode(node.(*ast.ShowStmtNode))
	case ast.BeginStmt:
		return newBeginStmtNode(node.(*ast.BeginStmtNode))
	case ast.SetTransactionStmt:
		return newSetTransactionStmtNode(node.(*ast.SetTransactionStmtNode))
	case ast.CommitStmt:
		return newCommitStmtNode(node.(*ast.CommitStmtNode))
	case ast.RollbackStmt:
		return newRollbackStmtNode(node.(*ast.RollbackStmtNode))
	case ast.StartBatchStmt:
		return newStartBatchStmtNode(node.(*ast.StartBatchStmtNode))
	case ast.RunBatchStmt:
		return newRunBatchStmtNode(node.(*ast.RunBatchStmtNode))
	case ast.AbortBatchStmt:
		return newAbortBatchStmtNode(node.(*ast.AbortBatchStmtNode))
	case ast.DropStmt:
		return newDropStmtNode(node.(*ast.DropStmtNode))
	case ast.DropMaterializedViewStmt:
		return newDropMaterializedViewStmtNode(node.(*ast.DropMaterializedViewStmtNode))
	case ast.DropSnapshotTableStmt:
		return newDropSnapshotTableStmtNode(node.(*ast.DropSnapshotTableStmtNode))
	case ast.RecursiveRefScan:
		return newRecursiveRefScanNode(node.(*ast.RecursiveRefScanNode))
	case ast.RecursiveScan:
		return newRecursiveScanNode(node.(*ast.RecursiveScanNode))
	case ast.WithScan:
		return newWithScanNode(node.(*ast.WithScanNode))
	case ast.WithEntry:
		return newWithEntryNode(node.(*ast.WithEntryNode))
	case ast.Option:
		return newOptionNode(node.(*ast.OptionNode))
	case ast.WindowPartitioning:
		return newWindowPartitioningNode(node.(*ast.WindowPartitioningNode))
	case ast.WindowOrdering:
		return newWindowOrderingNode(node.(*ast.WindowOrderingNode))
	case ast.WindowFrame:
		return newWindowFrameNode(node.(*ast.WindowFrameNode))
	case ast.AnalyticFunctionGroup:
		return newAnalyticFunctionGroupNode(node.(*ast.AnalyticFunctionGroupNode))
	case ast.WindowFrameExpr:
		return newWindowFrameExprNode(node.(*ast.WindowFrameExprNode))
	case ast.DMLValue:
		return newDMLValueNode(node.(*ast.DMLValueNode))
	case ast.DMLDefault:
		return newDMLDefaultNode(node.(*ast.DMLDefaultNode))
	case ast.AssertStmt:
		return newAssertStmtNode(node.(*ast.AssertStmtNode))
	case ast.AssertRowsModified:
		return newAssertRowsModifiedNode(node.(*ast.AssertRowsModifiedNode))
	case ast.InsertRow:
		return newInsertRowNode(node.(*ast.InsertRowNode))
	case ast.InsertStmt:
		return newInsertStmtNode(node.(*ast.InsertStmtNode))
	case ast.DeleteStmt:
		return newDeleteStmtNode(node.(*ast.DeleteStmtNode))
	case ast.UpdateItem:
		return newUpdateItemNode(node.(*ast.UpdateItemNode))
	case ast.UpdateArrayItem:
		return newUpdateArrayItemNode(node.(*ast.UpdateArrayItemNode))
	case ast.UpdateStmt:
		return newUpdateStmtNode(node.(*ast.UpdateStmtNode))
	case ast.MergeWhen:
		return newMergeWhenNode(node.(*ast.MergeWhenNode))
	case ast.MergeStmt:
		return newMergeStmtNode(node.(*ast.MergeStmtNode))
	case ast.TruncateStmt:
		return newTruncateStmtNode(node.(*ast.TruncateStmtNode))
	case ast.ObjectUnit:
		return newObjectUnitNode(node.(*ast.ObjectUnitNode))
	case ast.Privilege:
		return newPrivilegeNode(node.(*ast.PrivilegeNode))
	case ast.GrantStmt:
		return newGrantStmtNode(node.(*ast.GrantStmtNode))
	case ast.RevokeStmt:
		return newRevokeStmtNode(node.(*ast.RevokeStmtNode))
	case ast.AlterDatabaseStmt:
		return newAlterDatabaseStmtNode(node.(*ast.AlterDatabaseStmtNode))
	case ast.AlterMaterializedViewStmt:
		return newAlterMaterializedViewStmtNode(node.(*ast.AlterMaterializedViewStmtNode))
	case ast.AlterSchemaStmt:
		return newAlterSchemaStmtNode(node.(*ast.AlterSchemaStmtNode))
	case ast.AlterTableStmt:
		return newAlterTableStmtNode(node.(*ast.AlterTableStmtNode))
	case ast.AlterViewStmt:
		return newAlterViewStmtNode(node.(*ast.AlterViewStmtNode))
	case ast.SetOptionsAction:
		return newSetOptionsActionNode(node.(*ast.SetOptionsActionNode))
	case ast.AddColumnAction:
		return newAddColumnActionNode(node.(*ast.AddColumnActionNode))
	case ast.AddConstraintAction:
		return newAddConstraintActionNode(node.(*ast.AddConstraintActionNode))
	case ast.DropConstraintAction:
		return newDropConstraintActionNode(node.(*ast.DropConstraintActionNode))
	case ast.DropPrimaryKeyAction:
		return newDropPrimaryKeyActionNode(node.(*ast.DropPrimaryKeyActionNode))
	case ast.AlterColumnOptionsAction:
		return newAlterColumnOptionsActionNode(node.(*ast.AlterColumnOptionsActionNode))
	case ast.AlterColumnDropNotNullAction:
		return newAlterColumnDropNotNullActionNode(node.(*ast.AlterColumnDropNotNullActionNode))
	case ast.AlterColumnSetDataTypeAction:
		return newAlterColumnSetDataTypeActionNode(node.(*ast.AlterColumnSetDataTypeActionNode))
	case ast.AlterColumnSetDefaultAction:
		return newAlterColumnSetDefaultActionNode(node.(*ast.AlterColumnSetDefaultActionNode))
	case ast.AlterColumnDropDefaultAction:
		return newAlterColumnDropDefaultActionNode(node.(*ast.AlterColumnDropDefaultActionNode))
	case ast.DropColumnAction:
		return newDropColumnActionNode(node.(*ast.DropColumnActionNode))
	case ast.RenameColumnAction:
		return newRenameColumnActionNode(node.(*ast.RenameColumnActionNode))
	case ast.SetAsAction:
		return newSetAsActionNode(node.(*ast.SetAsActionNode))
	case ast.SetCollateClause:
		return newSetCollateClauseNode(node.(*ast.SetCollateClauseNode))
	case ast.AlterTableSetOptionsStmt:
		return newAlterTableSetOptionsStmtNode(node.(*ast.AlterTableSetOptionsStmtNode))
	case ast.RenameStmt:
		return newRenameStmtNode(node.(*ast.RenameStmtNode))
	case ast.CreatePrivilegeRestrictionStmt:
		return newCreatePrivilegeRestrictionStmtNode(node.(*ast.CreatePrivilegeRestrictionStmtNode))
	case ast.CreateRowAccessPolicyStmt:
		return newCreateRowAccessPolicyStmtNode(node.(*ast.CreateRowAccessPolicyStmtNode))
	case ast.DropPrivilegeRestrictionStmt:
		return newDropPrivilegeRestrictionStmtNode(node.(*ast.DropPrivilegeRestrictionStmtNode))
	case ast.DropRowAccessPolicyStmt:
		return newDropRowAccessPolicyStmtNode(node.(*ast.DropRowAccessPolicyStmtNode))
	case ast.DropSearchIndexStmt:
		return newDropSearchIndexStmtNode(node.(*ast.DropSearchIndexStmtNode))
	case ast.GrantToAction:
		return newGrantToActionNode(node.(*ast.GrantToActionNode))
	case ast.RestrictToAction:
		return newRestrictToActionNode(node.(*ast.RestrictToActionNode))
	case ast.AddToRestricteeListAction:
		return newAddToRestricteeListActionNode(node.(*ast.AddToRestricteeListActionNode))
	case ast.RemoveFromRestricteeListAction:
		return newRemoveFromRestricteeListActionNode(node.(*ast.RemoveFromRestricteeListActionNode))
	case ast.FilterUsingAction:
		return newFilterUsingActionNode(node.(*ast.FilterUsingActionNode))
	case ast.RevokeFromAction:
		return newRevokeFromActionNode(node.(*ast.RevokeFromActionNode))
	case ast.RenameToAction:
		return newRenameToActionNode(node.(*ast.RenameToActionNode))
	case ast.AlterPrivilegeRestrictionStmt:
		return newAlterPrivilegeRestrictionStmtNode(node.(*ast.AlterPrivilegeRestrictionStmtNode))
	case ast.AlterRowAccessPolicyStmt:
		return newAlterRowAccessPolicyStmtNode(node.(*ast.AlterRowAccessPolicyStmtNode))
	case ast.AlterAllRowAccessPoliciesStmt:
		return newAlterAllRowAccessPoliciesStmtNode(node.(*ast.AlterAllRowAccessPoliciesStmtNode))
	case ast.CreateConstantStmt:
		return newCreateConstantStmtNode(node.(*ast.CreateConstantStmtNode))
	case ast.CreateFunctionStmt:
		return newCreateFunctionStmtNode(node.(*ast.CreateFunctionStmtNode))
	case ast.ArgumentDef:
		return newArgumentDefNode(node.(*ast.ArgumentDefNode))
	case ast.ArgumentRef:
		return newArgumentRefNode(node.(*ast.ArgumentRefNode))
	case ast.CreateTableFunctionStmt:
		return newCreateTableFunctionStmtNode(node.(*ast.CreateTableFunctionStmtNode))
	case ast.RelationArgumentScan:
		return newRelationArgumentScanNode(node.(*ast.RelationArgumentScanNode))
	case ast.ArgumentList:
		return newArgumentListNode(node.(*ast.ArgumentListNode))
	case ast.FunctionSignatureHolder:
		return newFunctionSignatureHolderNode(node.(*ast.FunctionSignatureHolderNode))
	case ast.DropFunctionStmt:
		return newDropFunctionStmtNode(node.(*ast.DropFunctionStmtNode))
	case ast.DropTableFunctionStmt:
		return newDropTableFunctionStmtNode(node.(*ast.DropTableFunctionStmtNode))
	case ast.CallStmt:
		return newCallStmtNode(node.(*ast.CallStmtNode))
	case ast.ImportStmt:
		return newImportStmtNode(node.(*ast.ImportStmtNode))
	case ast.ModuleStmt:
		return newModuleStmtNode(node.(*ast.ModuleStmtNode))
	case ast.AggregateHavingModifier:
		return newAggregateHavingModifierNode(node.(*ast.AggregateHavingModifierNode))
	case ast.CreateMaterializedViewStmt:
		return newCreateMaterializedViewStmtNode(node.(*ast.CreateMaterializedViewStmtNode))
	case ast.CreateProcedureStmt:
		return newCreateProcedureStmtNode(node.(*ast.CreateProcedureStmtNode))
	case ast.ExecuteImmediateArgument:
		return newExecuteImmediateArgumentNode(node.(*ast.ExecuteImmediateArgumentNode))
	case ast.ExecuteImmediateStmt:
		return newExecuteImmediateStmtNode(node.(*ast.ExecuteImmediateStmtNode))
	case ast.AssignmentStmt:
		return newAssignmentStmtNode(node.(*ast.AssignmentStmtNode))
	case ast.CreateEntityStmt:
		return newCreateEntityStmtNode(node.(*ast.CreateEntityStmtNode))
	case ast.AlterEntityStmt:
		return newAlterEntityStmtNode(node.(*ast.AlterEntityStmtNode))
	case ast.PivotColumn:
		return newPivotColumnNode(node.(*ast.PivotColumnNode))
	case ast.PivotScan:
		return newPivotScanNode(node.(*ast.PivotScanNode))
	case ast.ReturningClause:
		return newReturningClauseNode(node.(*ast.ReturningClauseNode))
	case ast.UnpivotArg:
		return newUnpivotArgNode(node.(*ast.UnpivotArgNode))
	case ast.UnpivotScan:
		return newUnpivotScanNode(node.(*ast.UnpivotScanNode))
	case ast.CloneDataStmt:
		return newCloneDataStmtNode(node.(*ast.CloneDataStmtNode))
	case ast.TableAndColumnInfo:
		return newTableAndColumnInfoNode(node.(*ast.TableAndColumnInfoNode))
	case ast.AnalyzeStmt:
		return newAnalyzeStmtNode(node.(*ast.AnalyzeStmtNode))
	case ast.AuxLoadDataStmt:
		return newAuxLoadDataStmtNode(node.(*ast.AuxLoadDataStmtNode))
	}
	return nil
}

type LiteralNode struct {
	node *ast.LiteralNode
}

func (n *LiteralNode) toJSONValue(value types.Value) string {
	jsonValue := n.toJSONValueRecursive(value)
	switch value.Type().Kind() {
	case types.ARRAY:
		return toArrayValueFromJSONString(jsonValue)
	case types.STRUCT:
		return toStructValueFromJSONString(jsonValue)
	}
	return jsonValue
}

func (n *LiteralNode) toJSONValueRecursive(value types.Value) string {
	switch value.Type().Kind() {
	case types.ARRAY:
		elems := []string{}
		for i := 0; i < value.NumElements(); i++ {
			elem := value.Element(i)
			elems = append(elems, n.toJSONValue(elem))
		}
		return fmt.Sprintf("[%s]", strings.Join(elems, ","))
	case types.STRUCT:
		fields := []string{}
		structType := value.Type().AsStruct()
		for i := 0; i < value.NumFields(); i++ {
			field := value.Field(i)
			name := structType.Field(i).Name()
			val := n.toJSONValue(field)
			fields = append(
				fields,
				fmt.Sprintf("%s:%s", strconv.Quote(name), string(val)),
			)
		}
		return fmt.Sprintf("{%s}", strings.Join(fields, ","))
	default:
		return value.SQLLiteral(0)
	}
}

func (n *LiteralNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return n.toJSONValue(n.node.Value()), nil
}

type ParameterNode struct {
	node *ast.ParameterNode
}

func (n *ParameterNode) FormatSQL(ctx context.Context) (string, error) {
	return fmt.Sprintf("@%s", n.node.Name()), nil
	//return "?", nil
}

type ExpressionColumnNode struct {
	node *ast.ExpressionColumnNode
}

func (n *ExpressionColumnNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ColumnRefNode struct {
	node *ast.ColumnRefNode
}

func (n *ColumnRefNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return fmt.Sprintf("`%s`", n.node.Column().Name()), nil
}

type ConstantNode struct {
	node *ast.ConstantNode
}

func (n *ConstantNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type SystemVariableNode struct {
	node *ast.SystemVariableNode
}

func (n *SystemVariableNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type InlineLambdaNode struct {
	node *ast.InlineLambdaNode
}

func (n *InlineLambdaNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type FilterFieldArgNode struct {
	node *ast.FilterFieldArgNode
}

func (n *FilterFieldArgNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type FilterFieldNode struct {
	node *ast.FilterFieldNode
}

func (n *FilterFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type FunctionCallNode struct {
	node *ast.FunctionCallNode
}

func (n *FunctionCallNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	args := []string{}
	for _, a := range n.node.ArgumentList() {
		arg, err := newNode(a).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		args = append(args, arg)
	}
	resultType := strings.ToLower(n.node.Signature().ResultType().Type().TypeName(0))
	funcName := n.node.Function().FullName(false)
	if strings.HasPrefix(funcName, "$") {
		funcName = fmt.Sprintf("zetasqlite_%s_%s", funcName[1:], resultType)
	} else if _, exists := builtinFuncMap[funcName]; exists {
		funcName = fmt.Sprintf("zetasqlite_%s_%s", funcName, resultType)
	} else {
		fullpath := fullNamePathFromContext(ctx)
		path := fullpath.paths[fullpath.idx]
		funcName = formatName(
			mergeNamePath(
				namePathFromContext(ctx),
				path,
			),
		)
		fullpath.idx++
	}
	return fmt.Sprintf(
		"%s(%s)",
		funcName,
		strings.Join(args, ","),
	), nil
}

type AggregateFunctionCallNode struct {
	node *ast.AggregateFunctionCallNode
}

func (n *AggregateFunctionCallNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	args := []string{}
	for _, a := range n.node.ArgumentList() {
		arg, err := newNode(a).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		args = append(args, arg)
	}
	resultType := strings.ToLower(n.node.Signature().ResultType().Type().TypeName(0))
	funcName := n.node.Function().FullName(false)
	if strings.HasPrefix(funcName, "$") {
		funcName = fmt.Sprintf("zetasqlite_%s_%s", funcName[1:], resultType)
	} else if _, exists := builtinAggregateFuncMap[funcName]; exists {
		funcName = fmt.Sprintf("zetasqlite_%s_%s", funcName, resultType)
	} else {
		fullpath := fullNamePathFromContext(ctx)
		path := fullpath.paths[fullpath.idx]
		funcName = formatName(
			mergeNamePath(
				namePathFromContext(ctx),
				path,
			),
		)
		fullpath.idx++
	}
	return fmt.Sprintf(
		"%s(%s)",
		funcName,
		strings.Join(args, ","),
	), nil
}

type AnalyticFunctionCallNode struct {
	node *ast.AnalyticFunctionCallNode
}

func (n *AnalyticFunctionCallNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ExtendedCastElementNode struct {
	node *ast.ExtendedCastElementNode
}

func (n *ExtendedCastElementNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ExtendedCastNode struct {
	node *ast.ExtendedCastNode
}

func (n *ExtendedCastNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CastNode struct {
	node *ast.CastNode
}

func (n *CastNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type MakeStructNode struct {
	node *ast.MakeStructNode
}

func (n *MakeStructNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type MakeProtoNode struct {
	node *ast.MakeProtoNode
}

func (n *MakeProtoNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type MakeProtoFieldNode struct {
	node *ast.MakeProtoFieldNode
}

func (n *MakeProtoFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type GetStructFieldNode struct {
	node *ast.GetStructFieldNode
}

func (n *GetStructFieldNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	expr, err := newNode(n.node.Expr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	typeSuffix := strings.ToLower(n.node.Type().TypeName(0))
	idx := n.node.FieldIdx()
	return fmt.Sprintf("zetasqlite_get_struct_field_%s(%s, %d)", typeSuffix, expr, idx), nil
}

type GetProtoFieldNode struct {
	node *ast.GetProtoFieldNode
}

func (n *GetProtoFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type GetJsonFieldNode struct {
	node *ast.GetJsonFieldNode
}

func (n *GetJsonFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type FlattenNode struct {
	node *ast.FlattenNode
}

func (n *FlattenNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type FlattenedArgNode struct {
	node *ast.FlattenedArgNode
}

func (n *FlattenedArgNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ReplaceFieldItemNode struct {
	node *ast.ReplaceFieldItemNode
}

func (n *ReplaceFieldItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ReplaceFieldNode struct {
	node *ast.ReplaceFieldNode
}

func (n *ReplaceFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type SubqueryExprNode struct {
	node *ast.SubqueryExprNode
}

func (n *SubqueryExprNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	sql, err := newNode(n.node.Subquery()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	switch n.node.SubqueryType() {
	case ast.SubqueryTypeExists:
		return fmt.Sprintf("EXISTS (SELECT * %s)", sql), nil
	}
	return sql, nil
}

type LetExprNode struct {
	node *ast.LetExprNode
}

func (n *LetExprNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ModelNode struct {
	node *ast.ModelNode
}

func (n *ModelNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ConnectionNode struct {
	node *ast.ConnectionNode
}

func (n *ConnectionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DescriptorNode struct {
	node *ast.DescriptorNode
}

func (n *DescriptorNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type SingleRowScanNode struct {
	node *ast.SingleRowScanNode
}

func (n *SingleRowScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type TableScanNode struct {
	node *ast.TableScanNode
}

func (n *TableScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	fullpath := fullNamePathFromContext(ctx)
	path := fullpath.paths[fullpath.idx]
	tableName := formatName(
		mergeNamePath(
			namePathFromContext(ctx),
			path,
		),
	)
	fullpath.idx++
	return fmt.Sprintf("FROM `%s`", tableName), nil
}

type JoinScanNode struct {
	node *ast.JoinScanNode
}

func (n *JoinScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ArrayScanNode struct {
	node *ast.ArrayScanNode
}

func (n *ArrayScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	arrayExpr, err := newNode(n.node.ArrayExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	colName := n.node.ElementColumn().Name()
	return fmt.Sprintf(
		"FROM ( SELECT json_each.value AS %s FROM json_each(zetasqlite_decode_array(%s)) )",
		colName,
		arrayExpr,
	), nil
}

type ColumnHolderNode struct {
	node *ast.ColumnHolderNode
}

func (n *ColumnHolderNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type FilterScanNode struct {
	node *ast.FilterScanNode
}

func (n *FilterScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	filter, err := newNode(n.node.FilterExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s WHERE %s", input, filter), nil
}

type GroupingSetNode struct {
	node *ast.GroupingSetNode
}

func (n *GroupingSetNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AggregateScanNode struct {
	node *ast.AggregateScanNode
}

func (n *AggregateScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	for _, agg := range n.node.AggregateList() {
		// assign sql to column ref map
		if _, err := newNode(agg).FormatSQL(ctx); err != nil {
			return "", err
		}
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	columns := []string{}
	for _, col := range n.node.ColumnList() {
		columnMap := columnRefMap(ctx)
		if ref, exists := columnMap[col.Name()]; exists {
			columns = append(columns, ref)
		} else {
			columns = append(columns, col.Name())
		}
	}
	return fmt.Sprintf("%s %s", strings.Join(columns, ","), input), nil
}

type AnonymizedAggregateScanNode struct {
	node *ast.AnonymizedAggregateScanNode
}

func (n *AnonymizedAggregateScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type SetOperationItemNode struct {
	node *ast.SetOperationItemNode
}

func (n *SetOperationItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type SetOperationScanNode struct {
	node *ast.SetOperationScanNode
}

func (n *SetOperationScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type OrderByScanNode struct {
	node *ast.OrderByScanNode
}

func (n *OrderByScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type LimitOffsetScanNode struct {
	node *ast.LimitOffsetScanNode
}

func (n *LimitOffsetScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type WithRefScanNode struct {
	node *ast.WithRefScanNode
}

func (n *WithRefScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AnalyticScanNode struct {
	node *ast.AnalyticScanNode
}

func (n *AnalyticScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type SampleScanNode struct {
	node *ast.SampleScanNode
}

func (n *SampleScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ComputedColumnNode struct {
	node *ast.ComputedColumnNode
}

func (n *ComputedColumnNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	sql, err := newNode(n.node.Expr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	columnMap := columnRefMap(ctx)
	columnMap[n.node.Column().Name()] = sql
	return "", nil
}

type OrderByItemNode struct {
	node *ast.OrderByItemNode
}

func (n *OrderByItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ColumnAnnotationsNode struct {
	node *ast.ColumnAnnotationsNode
}

func (n *ColumnAnnotationsNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type GeneratedColumnInfoNode struct {
	node *ast.GeneratedColumnInfoNode
}

func (n *GeneratedColumnInfoNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ColumnDefaultValueNode struct {
	node *ast.ColumnDefaultValueNode
}

func (n *ColumnDefaultValueNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ColumnDefinitionNode struct {
	node *ast.ColumnDefinitionNode
}

func (n *ColumnDefinitionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type PrimaryKeyNode struct {
	node *ast.PrimaryKeyNode
}

func (n *PrimaryKeyNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ForeignKeyNode struct {
	node *ast.ForeignKeyNode
}

func (n *ForeignKeyNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CheckConstraintNode struct {
	node *ast.CheckConstraintNode
}

func (n *CheckConstraintNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type OutputColumnNode struct {
	node *ast.OutputColumnNode
}

func (n *OutputColumnNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	columnMap := columnRefMap(ctx)
	if ref, exists := columnMap[n.node.Name()]; exists {
		return ref, nil
	}
	return fmt.Sprintf("`%s`", n.node.Name()), nil
}

type ProjectScanNode struct {
	node *ast.ProjectScanNode
}

func (n *ProjectScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	for _, col := range n.node.ExprList() {
		// assign expr to columnRefMap
		if _, err := newNode(col).FormatSQL(ctx); err != nil {
			return "", err
		}
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return input, nil
}

type TVFScanNode struct {
	node *ast.TVFScanNode
}

func (n *TVFScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type GroupRowsScanNode struct {
	node *ast.GroupRowsScanNode
}

func (n *GroupRowsScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type FunctionArgumentNode struct {
	node *ast.FunctionArgumentNode
}

func (n *FunctionArgumentNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ExplainStmtNode struct {
	node *ast.ExplainStmtNode
}

func (n *ExplainStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type QueryStmtNode struct {
	node *ast.QueryStmtNode
}

func (n *QueryStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	query, err := newNode(n.node.Query()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	cols := []string{}
	for _, c := range n.node.OutputColumnList() {
		col, err := newNode(c).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		cols = append(cols, col)
	}
	return fmt.Sprintf("SELECT %s %s", strings.Join(cols, ","), query), nil
}

type CreateDatabaseStmtNode struct {
	node *ast.CreateDatabaseStmtNode
}

func (n *CreateDatabaseStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type IndexItemNode struct {
	node *ast.IndexItemNode
}

func (n *IndexItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type UnnestItemNode struct {
	node *ast.UnnestItemNode
}

func (n *UnnestItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateIndexStmtNode struct {
	node *ast.CreateIndexStmtNode
}

func (n *CreateIndexStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateSchemaStmtNode struct {
	node *ast.CreateSchemaStmtNode
}

func (n *CreateSchemaStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateTableStmtNode struct {
	node *ast.CreateTableStmtNode
}

func (n *CreateTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateTableAsSelectStmtNode struct {
	node *ast.CreateTableAsSelectStmtNode
}

func (n *CreateTableAsSelectStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateModelStmtNode struct {
	node *ast.CreateModelStmtNode
}

func (n *CreateModelStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateViewStmtNode struct {
	node *ast.CreateViewStmtNode
}

func (n *CreateViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type WithPartitionColumnsNode struct {
	node *ast.WithPartitionColumnsNode
}

func (n *WithPartitionColumnsNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateSnapshotTableStmtNode struct {
	node *ast.CreateSnapshotTableStmtNode
}

func (n *CreateSnapshotTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateExternalTableStmtNode struct {
	node *ast.CreateExternalTableStmtNode
}

func (n *CreateExternalTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ExportModelStmtNode struct {
	node *ast.ExportModelStmtNode
}

func (n *ExportModelStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ExportDataStmtNode struct {
	node *ast.ExportDataStmtNode
}

func (n *ExportDataStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DefineTableStmtNode struct {
	node *ast.DefineTableStmtNode
}

func (n *DefineTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DescribeStmtNode struct {
	node *ast.DescribeStmtNode
}

func (n *DescribeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ShowStmtNode struct {
	node *ast.ShowStmtNode
}

func (n *ShowStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type BeginStmtNode struct {
	node *ast.BeginStmtNode
}

func (n *BeginStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type SetTransactionStmtNode struct {
	node *ast.SetTransactionStmtNode
}

func (n *SetTransactionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CommitStmtNode struct {
	node *ast.CommitStmtNode
}

func (n *CommitStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RollbackStmtNode struct {
	node *ast.RollbackStmtNode
}

func (n *RollbackStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type StartBatchStmtNode struct {
	node *ast.StartBatchStmtNode
}

func (n *StartBatchStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RunBatchStmtNode struct {
	node *ast.RunBatchStmtNode
}

func (n *RunBatchStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AbortBatchStmtNode struct {
	node *ast.AbortBatchStmtNode
}

func (n *AbortBatchStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DropStmtNode struct {
	node *ast.DropStmtNode
}

func (n *DropStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DropMaterializedViewStmtNode struct {
	node *ast.DropMaterializedViewStmtNode
}

func (n *DropMaterializedViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DropSnapshotTableStmtNode struct {
	node *ast.DropSnapshotTableStmtNode
}

func (n *DropSnapshotTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RecursiveRefScanNode struct {
	node *ast.RecursiveRefScanNode
}

func (n *RecursiveRefScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RecursiveScanNode struct {
	node *ast.RecursiveScanNode
}

func (n *RecursiveScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type WithScanNode struct {
	node *ast.WithScanNode
}

func (n *WithScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type WithEntryNode struct {
	node *ast.WithEntryNode
}

func (n *WithEntryNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type OptionNode struct {
	node *ast.OptionNode
}

func (n *OptionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type WindowPartitioningNode struct {
	node *ast.WindowPartitioningNode
}

func (n *WindowPartitioningNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type WindowOrderingNode struct {
	node *ast.WindowOrderingNode
}

func (n *WindowOrderingNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type WindowFrameNode struct {
	node *ast.WindowFrameNode
}

func (n *WindowFrameNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AnalyticFunctionGroupNode struct {
	node *ast.AnalyticFunctionGroupNode
}

func (n *AnalyticFunctionGroupNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type WindowFrameExprNode struct {
	node *ast.WindowFrameExprNode
}

func (n *WindowFrameExprNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DMLValueNode struct {
	node *ast.DMLValueNode
}

func (n *DMLValueNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	return newNode(n.node.Value()).FormatSQL(ctx)
}

type DMLDefaultNode struct {
	node *ast.DMLDefaultNode
}

func (n *DMLDefaultNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AssertStmtNode struct {
	node *ast.AssertStmtNode
}

func (n *AssertStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AssertRowsModifiedNode struct {
	node *ast.AssertRowsModifiedNode
}

func (n *AssertRowsModifiedNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type InsertRowNode struct {
	node *ast.InsertRowNode
}

func (n *InsertRowNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	values := []string{}
	for _, value := range n.node.ValueList() {
		sql, err := newNode(value).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		values = append(values, sql)
	}
	return fmt.Sprintf("(%s)", strings.Join(values, ",")), nil
}

type InsertStmtNode struct {
	node *ast.InsertStmtNode
}

func (n *InsertStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	fullpath := fullNamePathFromContext(ctx)
	path := fullpath.paths[fullpath.idx]
	formattedTableName := formatName(
		mergeNamePath(
			namePathFromContext(ctx),
			path,
		),
	)
	fullpath.idx++
	columns := []string{}
	for _, col := range n.node.InsertColumnList() {
		columns = append(columns, fmt.Sprintf("`%s`", col.Name()))
	}
	rows := []string{}
	for _, row := range n.node.RowList() {
		sql, err := newNode(row).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		rows = append(rows, sql)
	}
	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
		formattedTableName,
		strings.Join(columns, ","),
		strings.Join(rows, ","),
	), nil
}

type DeleteStmtNode struct {
	node *ast.DeleteStmtNode
}

func (n *DeleteStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	fullpath := fullNamePathFromContext(ctx)
	path := fullpath.paths[fullpath.idx]
	formattedTableName := formatName(
		mergeNamePath(
			namePathFromContext(ctx),
			path,
		),
	)
	fullpath.idx++
	where, err := newNode(n.node.WhereExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"DELETE FROM `%s` WHERE %s",
		formattedTableName,
		where,
	), nil
}

type UpdateItemNode struct {
	node *ast.UpdateItemNode
}

func (n *UpdateItemNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	target, err := newNode(n.node.Target()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	setValue, err := newNode(n.node.SetValue()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s=%s", target, setValue), nil
}

type UpdateArrayItemNode struct {
	node *ast.UpdateArrayItemNode
}

func (n *UpdateArrayItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type UpdateStmtNode struct {
	node *ast.UpdateStmtNode
}

func (n *UpdateStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	fullpath := fullNamePathFromContext(ctx)
	path := fullpath.paths[fullpath.idx]
	formattedTableName := formatName(
		mergeNamePath(
			namePathFromContext(ctx),
			path,
		),
	)
	fullpath.idx++
	updateItems := []string{}
	for _, item := range n.node.UpdateItemList() {
		sql, err := newNode(item).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		updateItems = append(updateItems, sql)
	}
	where, err := newNode(n.node.WhereExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"UPDATE `%s` SET %s WHERE %s",
		formattedTableName,
		strings.Join(updateItems, ","),
		where,
	), nil
}

type MergeWhenNode struct {
	node *ast.MergeWhenNode
}

func (n *MergeWhenNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type MergeStmtNode struct {
	node *ast.MergeStmtNode
}

func (n *MergeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type TruncateStmtNode struct {
	node *ast.TruncateStmtNode
}

func (n *TruncateStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ObjectUnitNode struct {
	node *ast.ObjectUnitNode
}

func (n *ObjectUnitNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type PrivilegeNode struct {
	node *ast.PrivilegeNode
}

func (n *PrivilegeNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type GrantStmtNode struct {
	node *ast.GrantStmtNode
}

func (n *GrantStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RevokeStmtNode struct {
	node *ast.RevokeStmtNode
}

func (n *RevokeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterDatabaseStmtNode struct {
	node *ast.AlterDatabaseStmtNode
}

func (n *AlterDatabaseStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterMaterializedViewStmtNode struct {
	node *ast.AlterMaterializedViewStmtNode
}

func (n *AlterMaterializedViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterSchemaStmtNode struct {
	node *ast.AlterSchemaStmtNode
}

func (n *AlterSchemaStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterTableStmtNode struct {
	node *ast.AlterTableStmtNode
}

func (n *AlterTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterViewStmtNode struct {
	node *ast.AlterViewStmtNode
}

func (n *AlterViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type SetOptionsActionNode struct {
	node *ast.SetOptionsActionNode
}

func (n *SetOptionsActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AddColumnActionNode struct {
	node *ast.AddColumnActionNode
}

func (n *AddColumnActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AddConstraintActionNode struct {
	node *ast.AddConstraintActionNode
}

func (n *AddConstraintActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DropConstraintActionNode struct {
	node *ast.DropConstraintActionNode
}

func (n *DropConstraintActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DropPrimaryKeyActionNode struct {
	node *ast.DropPrimaryKeyActionNode
}

func (n *DropPrimaryKeyActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterColumnOptionsActionNode struct {
	node *ast.AlterColumnOptionsActionNode
}

func (n *AlterColumnOptionsActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterColumnDropNotNullActionNode struct {
	node *ast.AlterColumnDropNotNullActionNode
}

func (n *AlterColumnDropNotNullActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterColumnSetDataTypeActionNode struct {
	node *ast.AlterColumnSetDataTypeActionNode
}

func (n *AlterColumnSetDataTypeActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterColumnSetDefaultActionNode struct {
	node *ast.AlterColumnSetDefaultActionNode
}

func (n *AlterColumnSetDefaultActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterColumnDropDefaultActionNode struct {
	node *ast.AlterColumnDropDefaultActionNode
}

func (n *AlterColumnDropDefaultActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DropColumnActionNode struct {
	node *ast.DropColumnActionNode
}

func (n *DropColumnActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RenameColumnActionNode struct {
	node *ast.RenameColumnActionNode
}

func (n *RenameColumnActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type SetAsActionNode struct {
	node *ast.SetAsActionNode
}

func (n *SetAsActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type SetCollateClauseNode struct {
	node *ast.SetCollateClauseNode
}

func (n *SetCollateClauseNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterTableSetOptionsStmtNode struct {
	node *ast.AlterTableSetOptionsStmtNode
}

func (n *AlterTableSetOptionsStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RenameStmtNode struct {
	node *ast.RenameStmtNode
}

func (n *RenameStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreatePrivilegeRestrictionStmtNode struct {
	node *ast.CreatePrivilegeRestrictionStmtNode
}

func (n *CreatePrivilegeRestrictionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateRowAccessPolicyStmtNode struct {
	node *ast.CreateRowAccessPolicyStmtNode
}

func (n *CreateRowAccessPolicyStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DropPrivilegeRestrictionStmtNode struct {
	node *ast.DropPrivilegeRestrictionStmtNode
}

func (n *DropPrivilegeRestrictionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DropRowAccessPolicyStmtNode struct {
	node *ast.DropRowAccessPolicyStmtNode
}

func (n *DropRowAccessPolicyStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DropSearchIndexStmtNode struct {
	node *ast.DropSearchIndexStmtNode
}

func (n *DropSearchIndexStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type GrantToActionNode struct {
	node *ast.GrantToActionNode
}

func (n *GrantToActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RestrictToActionNode struct {
	node *ast.RestrictToActionNode
}

func (n *RestrictToActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AddToRestricteeListActionNode struct {
	node *ast.AddToRestricteeListActionNode
}

func (n *AddToRestricteeListActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RemoveFromRestricteeListActionNode struct {
	node *ast.RemoveFromRestricteeListActionNode
}

func (n *RemoveFromRestricteeListActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type FilterUsingActionNode struct {
	node *ast.FilterUsingActionNode
}

func (n *FilterUsingActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RevokeFromActionNode struct {
	node *ast.RevokeFromActionNode
}

func (n *RevokeFromActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RenameToActionNode struct {
	node *ast.RenameToActionNode
}

func (n *RenameToActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterPrivilegeRestrictionStmtNode struct {
	node *ast.AlterPrivilegeRestrictionStmtNode
}

func (n *AlterPrivilegeRestrictionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterRowAccessPolicyStmtNode struct {
	node *ast.AlterRowAccessPolicyStmtNode
}

func (n *AlterRowAccessPolicyStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterAllRowAccessPoliciesStmtNode struct {
	node *ast.AlterAllRowAccessPoliciesStmtNode
}

func (n *AlterAllRowAccessPoliciesStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateConstantStmtNode struct {
	node *ast.CreateConstantStmtNode
}

func (n *CreateConstantStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateFunctionStmtNode struct {
	node *ast.CreateFunctionStmtNode
}

func (n *CreateFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ArgumentDefNode struct {
	node *ast.ArgumentDefNode
}

func (n *ArgumentDefNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ArgumentRefNode struct {
	node *ast.ArgumentRefNode
}

func (n *ArgumentRefNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return "?", nil
}

type CreateTableFunctionStmtNode struct {
	node *ast.CreateTableFunctionStmtNode
}

func (n *CreateTableFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type RelationArgumentScanNode struct {
	node *ast.RelationArgumentScanNode
}

func (n *RelationArgumentScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ArgumentListNode struct {
	node *ast.ArgumentListNode
}

func (n *ArgumentListNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type FunctionSignatureHolderNode struct {
	node *ast.FunctionSignatureHolderNode
}

func (n *FunctionSignatureHolderNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DropFunctionStmtNode struct {
	node *ast.DropFunctionStmtNode
}

func (n *DropFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type DropTableFunctionStmtNode struct {
	node *ast.DropTableFunctionStmtNode
}

func (n *DropTableFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CallStmtNode struct {
	node *ast.CallStmtNode
}

func (n *CallStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ImportStmtNode struct {
	node *ast.ImportStmtNode
}

func (n *ImportStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ModuleStmtNode struct {
	node *ast.ModuleStmtNode
}

func (n *ModuleStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AggregateHavingModifierNode struct {
	node *ast.AggregateHavingModifierNode
}

func (n *AggregateHavingModifierNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateMaterializedViewStmtNode struct {
	node *ast.CreateMaterializedViewStmtNode
}

func (n *CreateMaterializedViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateProcedureStmtNode struct {
	node *ast.CreateProcedureStmtNode
}

func (n *CreateProcedureStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ExecuteImmediateArgumentNode struct {
	node *ast.ExecuteImmediateArgumentNode
}

func (n *ExecuteImmediateArgumentNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ExecuteImmediateStmtNode struct {
	node *ast.ExecuteImmediateStmtNode
}

func (n *ExecuteImmediateStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AssignmentStmtNode struct {
	node *ast.AssignmentStmtNode
}

func (n *AssignmentStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CreateEntityStmtNode struct {
	node *ast.CreateEntityStmtNode
}

func (n *CreateEntityStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AlterEntityStmtNode struct {
	node *ast.AlterEntityStmtNode
}

func (n *AlterEntityStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type PivotColumnNode struct {
	node *ast.PivotColumnNode
}

func (n *PivotColumnNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type PivotScanNode struct {
	node *ast.PivotScanNode
}

func (n *PivotScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type ReturningClauseNode struct {
	node *ast.ReturningClauseNode
}

func (n *ReturningClauseNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type UnpivotArgNode struct {
	node *ast.UnpivotArgNode
}

func (n *UnpivotArgNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type UnpivotScanNode struct {
	node *ast.UnpivotScanNode
}

func (n *UnpivotScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type CloneDataStmtNode struct {
	node *ast.CloneDataStmtNode
}

func (n *CloneDataStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type TableAndColumnInfoNode struct {
	node *ast.TableAndColumnInfoNode
}

func (n *TableAndColumnInfoNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AnalyzeStmtNode struct {
	node *ast.AnalyzeStmtNode
}

func (n *AnalyzeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

type AuxLoadDataStmtNode struct {
	node *ast.AuxLoadDataStmtNode
}

func (n *AuxLoadDataStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func newLiteralNode(n *ast.LiteralNode) *LiteralNode {
	return &LiteralNode{node: n}
}

func newParameterNode(n *ast.ParameterNode) *ParameterNode {
	return &ParameterNode{node: n}
}

func newExpressionColumnNode(n *ast.ExpressionColumnNode) *ExpressionColumnNode {
	return &ExpressionColumnNode{node: n}
}

func newColumnRefNode(n *ast.ColumnRefNode) *ColumnRefNode {
	return &ColumnRefNode{node: n}
}

func newConstantNode(n *ast.ConstantNode) *ConstantNode {
	return &ConstantNode{node: n}
}

func newSystemVariableNode(n *ast.SystemVariableNode) *SystemVariableNode {
	return &SystemVariableNode{node: n}
}

func newInlineLambdaNode(n *ast.InlineLambdaNode) *InlineLambdaNode {
	return &InlineLambdaNode{node: n}
}

func newFilterFieldArgNode(n *ast.FilterFieldArgNode) *FilterFieldArgNode {
	return &FilterFieldArgNode{node: n}
}

func newFilterFieldNode(n *ast.FilterFieldNode) *FilterFieldNode {
	return &FilterFieldNode{node: n}
}

func newFunctionCallNode(n *ast.FunctionCallNode) *FunctionCallNode {
	return &FunctionCallNode{node: n}
}

func newAggregateFunctionCallNode(n *ast.AggregateFunctionCallNode) *AggregateFunctionCallNode {
	return &AggregateFunctionCallNode{node: n}
}

func newAnalyticFunctionCallNode(n *ast.AnalyticFunctionCallNode) *AnalyticFunctionCallNode {
	return &AnalyticFunctionCallNode{node: n}
}

func newExtendedCastElementNode(n *ast.ExtendedCastElementNode) *ExtendedCastElementNode {
	return &ExtendedCastElementNode{node: n}
}

func newExtendedCastNode(n *ast.ExtendedCastNode) *ExtendedCastNode {
	return &ExtendedCastNode{node: n}
}

func newCastNode(n *ast.CastNode) *CastNode {
	return &CastNode{node: n}
}

func newMakeStructNode(n *ast.MakeStructNode) *MakeStructNode {
	return &MakeStructNode{node: n}
}

func newMakeProtoNode(n *ast.MakeProtoNode) *MakeProtoNode {
	return &MakeProtoNode{node: n}
}

func newMakeProtoFieldNode(n *ast.MakeProtoFieldNode) *MakeProtoFieldNode {
	return &MakeProtoFieldNode{node: n}
}

func newGetStructFieldNode(n *ast.GetStructFieldNode) *GetStructFieldNode {
	return &GetStructFieldNode{node: n}
}

func newGetProtoFieldNode(n *ast.GetProtoFieldNode) *GetProtoFieldNode {
	return &GetProtoFieldNode{node: n}
}

func newGetJsonFieldNode(n *ast.GetJsonFieldNode) *GetJsonFieldNode {
	return &GetJsonFieldNode{node: n}
}

func newFlattenNode(n *ast.FlattenNode) *FlattenNode {
	return &FlattenNode{node: n}
}

func newFlattenedArgNode(n *ast.FlattenedArgNode) *FlattenedArgNode {
	return &FlattenedArgNode{node: n}
}

func newReplaceFieldItemNode(n *ast.ReplaceFieldItemNode) *ReplaceFieldItemNode {
	return &ReplaceFieldItemNode{node: n}
}

func newReplaceFieldNode(n *ast.ReplaceFieldNode) *ReplaceFieldNode {
	return &ReplaceFieldNode{node: n}
}

func newSubqueryExprNode(n *ast.SubqueryExprNode) *SubqueryExprNode {
	return &SubqueryExprNode{node: n}
}

func newLetExprNode(n *ast.LetExprNode) *LetExprNode {
	return &LetExprNode{node: n}
}

func newModelNode(n *ast.ModelNode) *ModelNode {
	return &ModelNode{node: n}
}

func newConnectionNode(n *ast.ConnectionNode) *ConnectionNode {
	return &ConnectionNode{node: n}
}

func newDescriptorNode(n *ast.DescriptorNode) *DescriptorNode {
	return &DescriptorNode{node: n}
}

func newSingleRowScanNode(n *ast.SingleRowScanNode) *SingleRowScanNode {
	return &SingleRowScanNode{node: n}
}

func newTableScanNode(n *ast.TableScanNode) *TableScanNode {
	return &TableScanNode{node: n}
}

func newJoinScanNode(n *ast.JoinScanNode) *JoinScanNode {
	return &JoinScanNode{node: n}
}

func newArrayScanNode(n *ast.ArrayScanNode) *ArrayScanNode {
	return &ArrayScanNode{node: n}
}

func newColumnHolderNode(n *ast.ColumnHolderNode) *ColumnHolderNode {
	return &ColumnHolderNode{node: n}
}

func newFilterScanNode(n *ast.FilterScanNode) *FilterScanNode {
	return &FilterScanNode{node: n}
}

func newGroupingSetNode(n *ast.GroupingSetNode) *GroupingSetNode {
	return &GroupingSetNode{node: n}
}

func newAggregateScanNode(n *ast.AggregateScanNode) *AggregateScanNode {
	return &AggregateScanNode{node: n}
}

func newAnonymizedAggregateScanNode(n *ast.AnonymizedAggregateScanNode) *AnonymizedAggregateScanNode {
	return &AnonymizedAggregateScanNode{node: n}
}

func newSetOperationItemNode(n *ast.SetOperationItemNode) *SetOperationItemNode {
	return &SetOperationItemNode{node: n}
}

func newSetOperationScanNode(n *ast.SetOperationScanNode) *SetOperationScanNode {
	return &SetOperationScanNode{node: n}
}

func newOrderByScanNode(n *ast.OrderByScanNode) *OrderByScanNode {
	return &OrderByScanNode{node: n}
}

func newLimitOffsetScanNode(n *ast.LimitOffsetScanNode) *LimitOffsetScanNode {
	return &LimitOffsetScanNode{node: n}
}

func newWithRefScanNode(n *ast.WithRefScanNode) *WithRefScanNode {
	return &WithRefScanNode{node: n}
}

func newAnalyticScanNode(n *ast.AnalyticScanNode) *AnalyticScanNode {
	return &AnalyticScanNode{node: n}
}

func newSampleScanNode(n *ast.SampleScanNode) *SampleScanNode {
	return &SampleScanNode{node: n}
}

func newComputedColumnNode(n *ast.ComputedColumnNode) *ComputedColumnNode {
	return &ComputedColumnNode{node: n}
}

func newOrderByItemNode(n *ast.OrderByItemNode) *OrderByItemNode {
	return &OrderByItemNode{node: n}
}

func newColumnAnnotationsNode(n *ast.ColumnAnnotationsNode) *ColumnAnnotationsNode {
	return &ColumnAnnotationsNode{node: n}
}

func newGeneratedColumnInfoNode(n *ast.GeneratedColumnInfoNode) *GeneratedColumnInfoNode {
	return &GeneratedColumnInfoNode{node: n}
}

func newColumnDefaultValueNode(n *ast.ColumnDefaultValueNode) *ColumnDefaultValueNode {
	return &ColumnDefaultValueNode{node: n}
}

func newColumnDefinitionNode(n *ast.ColumnDefinitionNode) *ColumnDefinitionNode {
	return &ColumnDefinitionNode{node: n}
}

func newPrimaryKeyNode(n *ast.PrimaryKeyNode) *PrimaryKeyNode {
	return &PrimaryKeyNode{node: n}
}

func newForeignKeyNode(n *ast.ForeignKeyNode) *ForeignKeyNode {
	return &ForeignKeyNode{node: n}
}

func newCheckConstraintNode(n *ast.CheckConstraintNode) *CheckConstraintNode {
	return &CheckConstraintNode{node: n}
}

func newOutputColumnNode(n *ast.OutputColumnNode) *OutputColumnNode {
	return &OutputColumnNode{node: n}
}

func newProjectScanNode(n *ast.ProjectScanNode) *ProjectScanNode {
	return &ProjectScanNode{node: n}
}

func newTVFScanNode(n *ast.TVFScanNode) *TVFScanNode {
	return &TVFScanNode{node: n}
}

func newGroupRowsScanNode(n *ast.GroupRowsScanNode) *GroupRowsScanNode {
	return &GroupRowsScanNode{node: n}
}

func newFunctionArgumentNode(n *ast.FunctionArgumentNode) *FunctionArgumentNode {
	return &FunctionArgumentNode{node: n}
}

func newExplainStmtNode(n *ast.ExplainStmtNode) *ExplainStmtNode {
	return &ExplainStmtNode{node: n}
}

func newQueryStmtNode(n *ast.QueryStmtNode) *QueryStmtNode {
	return &QueryStmtNode{node: n}
}

func newCreateDatabaseStmtNode(n *ast.CreateDatabaseStmtNode) *CreateDatabaseStmtNode {
	return &CreateDatabaseStmtNode{node: n}
}

func newIndexItemNode(n *ast.IndexItemNode) *IndexItemNode {
	return &IndexItemNode{node: n}
}

func newUnnestItemNode(n *ast.UnnestItemNode) *UnnestItemNode {
	return &UnnestItemNode{node: n}
}

func newCreateIndexStmtNode(n *ast.CreateIndexStmtNode) *CreateIndexStmtNode {
	return &CreateIndexStmtNode{node: n}
}

func newCreateSchemaStmtNode(n *ast.CreateSchemaStmtNode) *CreateSchemaStmtNode {
	return &CreateSchemaStmtNode{node: n}
}

func newCreateTableStmtNode(n *ast.CreateTableStmtNode) *CreateTableStmtNode {
	return &CreateTableStmtNode{node: n}
}

func newCreateTableAsSelectStmtNode(n *ast.CreateTableAsSelectStmtNode) *CreateTableAsSelectStmtNode {
	return &CreateTableAsSelectStmtNode{node: n}
}

func newCreateModelStmtNode(n *ast.CreateModelStmtNode) *CreateModelStmtNode {
	return &CreateModelStmtNode{node: n}
}

func newCreateViewStmtNode(n *ast.CreateViewStmtNode) *CreateViewStmtNode {
	return &CreateViewStmtNode{node: n}
}

func newWithPartitionColumnsNode(n *ast.WithPartitionColumnsNode) *WithPartitionColumnsNode {
	return &WithPartitionColumnsNode{node: n}
}

func newCreateSnapshotTableStmtNode(n *ast.CreateSnapshotTableStmtNode) *CreateSnapshotTableStmtNode {
	return &CreateSnapshotTableStmtNode{node: n}
}

func newCreateExternalTableStmtNode(n *ast.CreateExternalTableStmtNode) *CreateExternalTableStmtNode {
	return &CreateExternalTableStmtNode{node: n}
}

func newExportModelStmtNode(n *ast.ExportModelStmtNode) *ExportModelStmtNode {
	return &ExportModelStmtNode{node: n}
}

func newExportDataStmtNode(n *ast.ExportDataStmtNode) *ExportDataStmtNode {
	return &ExportDataStmtNode{node: n}
}

func newDefineTableStmtNode(n *ast.DefineTableStmtNode) *DefineTableStmtNode {
	return &DefineTableStmtNode{node: n}
}

func newDescribeStmtNode(n *ast.DescribeStmtNode) *DescribeStmtNode {
	return &DescribeStmtNode{node: n}
}

func newShowStmtNode(n *ast.ShowStmtNode) *ShowStmtNode {
	return &ShowStmtNode{node: n}
}

func newBeginStmtNode(n *ast.BeginStmtNode) *BeginStmtNode {
	return &BeginStmtNode{node: n}
}

func newSetTransactionStmtNode(n *ast.SetTransactionStmtNode) *SetTransactionStmtNode {
	return &SetTransactionStmtNode{node: n}
}

func newCommitStmtNode(n *ast.CommitStmtNode) *CommitStmtNode {
	return &CommitStmtNode{node: n}
}

func newRollbackStmtNode(n *ast.RollbackStmtNode) *RollbackStmtNode {
	return &RollbackStmtNode{node: n}
}

func newStartBatchStmtNode(n *ast.StartBatchStmtNode) *StartBatchStmtNode {
	return &StartBatchStmtNode{node: n}
}

func newRunBatchStmtNode(n *ast.RunBatchStmtNode) *RunBatchStmtNode {
	return &RunBatchStmtNode{node: n}
}

func newAbortBatchStmtNode(n *ast.AbortBatchStmtNode) *AbortBatchStmtNode {
	return &AbortBatchStmtNode{node: n}
}

func newDropStmtNode(n *ast.DropStmtNode) *DropStmtNode {
	return &DropStmtNode{node: n}
}

func newDropMaterializedViewStmtNode(n *ast.DropMaterializedViewStmtNode) *DropMaterializedViewStmtNode {
	return &DropMaterializedViewStmtNode{node: n}
}

func newDropSnapshotTableStmtNode(n *ast.DropSnapshotTableStmtNode) *DropSnapshotTableStmtNode {
	return &DropSnapshotTableStmtNode{node: n}
}

func newRecursiveRefScanNode(n *ast.RecursiveRefScanNode) *RecursiveRefScanNode {
	return &RecursiveRefScanNode{node: n}
}

func newRecursiveScanNode(n *ast.RecursiveScanNode) *RecursiveScanNode {
	return &RecursiveScanNode{node: n}
}

func newWithScanNode(n *ast.WithScanNode) *WithScanNode {
	return &WithScanNode{node: n}
}

func newWithEntryNode(n *ast.WithEntryNode) *WithEntryNode {
	return &WithEntryNode{node: n}
}

func newOptionNode(n *ast.OptionNode) *OptionNode {
	return &OptionNode{node: n}
}

func newWindowPartitioningNode(n *ast.WindowPartitioningNode) *WindowPartitioningNode {
	return &WindowPartitioningNode{node: n}
}

func newWindowOrderingNode(n *ast.WindowOrderingNode) *WindowOrderingNode {
	return &WindowOrderingNode{node: n}
}

func newWindowFrameNode(n *ast.WindowFrameNode) *WindowFrameNode {
	return &WindowFrameNode{node: n}
}

func newAnalyticFunctionGroupNode(n *ast.AnalyticFunctionGroupNode) *AnalyticFunctionGroupNode {
	return &AnalyticFunctionGroupNode{node: n}
}

func newWindowFrameExprNode(n *ast.WindowFrameExprNode) *WindowFrameExprNode {
	return &WindowFrameExprNode{node: n}
}

func newDMLValueNode(n *ast.DMLValueNode) *DMLValueNode {
	return &DMLValueNode{node: n}
}

func newDMLDefaultNode(n *ast.DMLDefaultNode) *DMLDefaultNode {
	return &DMLDefaultNode{node: n}
}

func newAssertStmtNode(n *ast.AssertStmtNode) *AssertStmtNode {
	return &AssertStmtNode{node: n}
}

func newAssertRowsModifiedNode(n *ast.AssertRowsModifiedNode) *AssertRowsModifiedNode {
	return &AssertRowsModifiedNode{node: n}
}

func newInsertRowNode(n *ast.InsertRowNode) *InsertRowNode {
	return &InsertRowNode{node: n}
}

func newInsertStmtNode(n *ast.InsertStmtNode) *InsertStmtNode {
	return &InsertStmtNode{node: n}
}

func newDeleteStmtNode(n *ast.DeleteStmtNode) *DeleteStmtNode {
	return &DeleteStmtNode{node: n}
}

func newUpdateItemNode(n *ast.UpdateItemNode) *UpdateItemNode {
	return &UpdateItemNode{node: n}
}

func newUpdateArrayItemNode(n *ast.UpdateArrayItemNode) *UpdateArrayItemNode {
	return &UpdateArrayItemNode{node: n}
}

func newUpdateStmtNode(n *ast.UpdateStmtNode) *UpdateStmtNode {
	return &UpdateStmtNode{node: n}
}

func newMergeWhenNode(n *ast.MergeWhenNode) *MergeWhenNode {
	return &MergeWhenNode{node: n}
}

func newMergeStmtNode(n *ast.MergeStmtNode) *MergeStmtNode {
	return &MergeStmtNode{node: n}
}

func newTruncateStmtNode(n *ast.TruncateStmtNode) *TruncateStmtNode {
	return &TruncateStmtNode{node: n}
}

func newObjectUnitNode(n *ast.ObjectUnitNode) *ObjectUnitNode {
	return &ObjectUnitNode{node: n}
}

func newPrivilegeNode(n *ast.PrivilegeNode) *PrivilegeNode {
	return &PrivilegeNode{node: n}
}

func newGrantStmtNode(n *ast.GrantStmtNode) *GrantStmtNode {
	return &GrantStmtNode{node: n}
}

func newRevokeStmtNode(n *ast.RevokeStmtNode) *RevokeStmtNode {
	return &RevokeStmtNode{node: n}
}

func newAlterDatabaseStmtNode(n *ast.AlterDatabaseStmtNode) *AlterDatabaseStmtNode {
	return &AlterDatabaseStmtNode{node: n}
}

func newAlterMaterializedViewStmtNode(n *ast.AlterMaterializedViewStmtNode) *AlterMaterializedViewStmtNode {
	return &AlterMaterializedViewStmtNode{node: n}
}

func newAlterSchemaStmtNode(n *ast.AlterSchemaStmtNode) *AlterSchemaStmtNode {
	return &AlterSchemaStmtNode{node: n}
}

func newAlterTableStmtNode(n *ast.AlterTableStmtNode) *AlterTableStmtNode {
	return &AlterTableStmtNode{node: n}
}

func newAlterViewStmtNode(n *ast.AlterViewStmtNode) *AlterViewStmtNode {
	return &AlterViewStmtNode{node: n}
}

func newSetOptionsActionNode(n *ast.SetOptionsActionNode) *SetOptionsActionNode {
	return &SetOptionsActionNode{node: n}
}

func newAddColumnActionNode(n *ast.AddColumnActionNode) *AddColumnActionNode {
	return &AddColumnActionNode{node: n}
}

func newAddConstraintActionNode(n *ast.AddConstraintActionNode) *AddConstraintActionNode {
	return &AddConstraintActionNode{node: n}
}

func newDropConstraintActionNode(n *ast.DropConstraintActionNode) *DropConstraintActionNode {
	return &DropConstraintActionNode{node: n}
}

func newDropPrimaryKeyActionNode(n *ast.DropPrimaryKeyActionNode) *DropPrimaryKeyActionNode {
	return &DropPrimaryKeyActionNode{node: n}
}

func newAlterColumnOptionsActionNode(n *ast.AlterColumnOptionsActionNode) *AlterColumnOptionsActionNode {
	return &AlterColumnOptionsActionNode{node: n}
}

func newAlterColumnDropNotNullActionNode(n *ast.AlterColumnDropNotNullActionNode) *AlterColumnDropNotNullActionNode {
	return &AlterColumnDropNotNullActionNode{node: n}
}

func newAlterColumnSetDataTypeActionNode(n *ast.AlterColumnSetDataTypeActionNode) *AlterColumnSetDataTypeActionNode {
	return &AlterColumnSetDataTypeActionNode{node: n}
}

func newAlterColumnSetDefaultActionNode(n *ast.AlterColumnSetDefaultActionNode) *AlterColumnSetDefaultActionNode {
	return &AlterColumnSetDefaultActionNode{node: n}
}

func newAlterColumnDropDefaultActionNode(n *ast.AlterColumnDropDefaultActionNode) *AlterColumnDropDefaultActionNode {
	return &AlterColumnDropDefaultActionNode{node: n}
}

func newDropColumnActionNode(n *ast.DropColumnActionNode) *DropColumnActionNode {
	return &DropColumnActionNode{node: n}
}

func newRenameColumnActionNode(n *ast.RenameColumnActionNode) *RenameColumnActionNode {
	return &RenameColumnActionNode{node: n}
}

func newSetAsActionNode(n *ast.SetAsActionNode) *SetAsActionNode {
	return &SetAsActionNode{node: n}
}

func newSetCollateClauseNode(n *ast.SetCollateClauseNode) *SetCollateClauseNode {
	return &SetCollateClauseNode{node: n}
}

func newAlterTableSetOptionsStmtNode(n *ast.AlterTableSetOptionsStmtNode) *AlterTableSetOptionsStmtNode {
	return &AlterTableSetOptionsStmtNode{node: n}
}

func newRenameStmtNode(n *ast.RenameStmtNode) *RenameStmtNode {
	return &RenameStmtNode{node: n}
}

func newCreatePrivilegeRestrictionStmtNode(n *ast.CreatePrivilegeRestrictionStmtNode) *CreatePrivilegeRestrictionStmtNode {
	return &CreatePrivilegeRestrictionStmtNode{node: n}
}

func newCreateRowAccessPolicyStmtNode(n *ast.CreateRowAccessPolicyStmtNode) *CreateRowAccessPolicyStmtNode {
	return &CreateRowAccessPolicyStmtNode{node: n}
}

func newDropPrivilegeRestrictionStmtNode(n *ast.DropPrivilegeRestrictionStmtNode) *DropPrivilegeRestrictionStmtNode {
	return &DropPrivilegeRestrictionStmtNode{node: n}
}

func newDropRowAccessPolicyStmtNode(n *ast.DropRowAccessPolicyStmtNode) *DropRowAccessPolicyStmtNode {
	return &DropRowAccessPolicyStmtNode{node: n}
}

func newDropSearchIndexStmtNode(n *ast.DropSearchIndexStmtNode) *DropSearchIndexStmtNode {
	return &DropSearchIndexStmtNode{node: n}
}

func newGrantToActionNode(n *ast.GrantToActionNode) *GrantToActionNode {
	return &GrantToActionNode{node: n}
}

func newRestrictToActionNode(n *ast.RestrictToActionNode) *RestrictToActionNode {
	return &RestrictToActionNode{node: n}
}

func newAddToRestricteeListActionNode(n *ast.AddToRestricteeListActionNode) *AddToRestricteeListActionNode {
	return &AddToRestricteeListActionNode{node: n}
}

func newRemoveFromRestricteeListActionNode(n *ast.RemoveFromRestricteeListActionNode) *RemoveFromRestricteeListActionNode {
	return &RemoveFromRestricteeListActionNode{node: n}
}

func newFilterUsingActionNode(n *ast.FilterUsingActionNode) *FilterUsingActionNode {
	return &FilterUsingActionNode{node: n}
}

func newRevokeFromActionNode(n *ast.RevokeFromActionNode) *RevokeFromActionNode {
	return &RevokeFromActionNode{node: n}
}

func newRenameToActionNode(n *ast.RenameToActionNode) *RenameToActionNode {
	return &RenameToActionNode{node: n}
}

func newAlterPrivilegeRestrictionStmtNode(n *ast.AlterPrivilegeRestrictionStmtNode) *AlterPrivilegeRestrictionStmtNode {
	return &AlterPrivilegeRestrictionStmtNode{node: n}
}

func newAlterRowAccessPolicyStmtNode(n *ast.AlterRowAccessPolicyStmtNode) *AlterRowAccessPolicyStmtNode {
	return &AlterRowAccessPolicyStmtNode{node: n}
}

func newAlterAllRowAccessPoliciesStmtNode(n *ast.AlterAllRowAccessPoliciesStmtNode) *AlterAllRowAccessPoliciesStmtNode {
	return &AlterAllRowAccessPoliciesStmtNode{node: n}
}

func newCreateConstantStmtNode(n *ast.CreateConstantStmtNode) *CreateConstantStmtNode {
	return &CreateConstantStmtNode{node: n}
}

func newCreateFunctionStmtNode(n *ast.CreateFunctionStmtNode) *CreateFunctionStmtNode {
	return &CreateFunctionStmtNode{node: n}
}

func newArgumentDefNode(n *ast.ArgumentDefNode) *ArgumentDefNode {
	return &ArgumentDefNode{node: n}
}

func newArgumentRefNode(n *ast.ArgumentRefNode) *ArgumentRefNode {
	return &ArgumentRefNode{node: n}
}

func newCreateTableFunctionStmtNode(n *ast.CreateTableFunctionStmtNode) *CreateTableFunctionStmtNode {
	return &CreateTableFunctionStmtNode{node: n}
}

func newRelationArgumentScanNode(n *ast.RelationArgumentScanNode) *RelationArgumentScanNode {
	return &RelationArgumentScanNode{node: n}
}

func newArgumentListNode(n *ast.ArgumentListNode) *ArgumentListNode {
	return &ArgumentListNode{node: n}
}

func newFunctionSignatureHolderNode(n *ast.FunctionSignatureHolderNode) *FunctionSignatureHolderNode {
	return &FunctionSignatureHolderNode{node: n}
}

func newDropFunctionStmtNode(n *ast.DropFunctionStmtNode) *DropFunctionStmtNode {
	return &DropFunctionStmtNode{node: n}
}

func newDropTableFunctionStmtNode(n *ast.DropTableFunctionStmtNode) *DropTableFunctionStmtNode {
	return &DropTableFunctionStmtNode{node: n}
}

func newCallStmtNode(n *ast.CallStmtNode) *CallStmtNode {
	return &CallStmtNode{node: n}
}

func newImportStmtNode(n *ast.ImportStmtNode) *ImportStmtNode {
	return &ImportStmtNode{node: n}
}

func newModuleStmtNode(n *ast.ModuleStmtNode) *ModuleStmtNode {
	return &ModuleStmtNode{node: n}
}

func newAggregateHavingModifierNode(n *ast.AggregateHavingModifierNode) *AggregateHavingModifierNode {
	return &AggregateHavingModifierNode{node: n}
}

func newCreateMaterializedViewStmtNode(n *ast.CreateMaterializedViewStmtNode) *CreateMaterializedViewStmtNode {
	return &CreateMaterializedViewStmtNode{node: n}
}

func newCreateProcedureStmtNode(n *ast.CreateProcedureStmtNode) *CreateProcedureStmtNode {
	return &CreateProcedureStmtNode{node: n}
}

func newExecuteImmediateArgumentNode(n *ast.ExecuteImmediateArgumentNode) *ExecuteImmediateArgumentNode {
	return &ExecuteImmediateArgumentNode{node: n}
}

func newExecuteImmediateStmtNode(n *ast.ExecuteImmediateStmtNode) *ExecuteImmediateStmtNode {
	return &ExecuteImmediateStmtNode{node: n}
}

func newAssignmentStmtNode(n *ast.AssignmentStmtNode) *AssignmentStmtNode {
	return &AssignmentStmtNode{node: n}
}

func newCreateEntityStmtNode(n *ast.CreateEntityStmtNode) *CreateEntityStmtNode {
	return &CreateEntityStmtNode{node: n}
}

func newAlterEntityStmtNode(n *ast.AlterEntityStmtNode) *AlterEntityStmtNode {
	return &AlterEntityStmtNode{node: n}
}

func newPivotColumnNode(n *ast.PivotColumnNode) *PivotColumnNode {
	return &PivotColumnNode{node: n}
}

func newPivotScanNode(n *ast.PivotScanNode) *PivotScanNode {
	return &PivotScanNode{node: n}
}

func newReturningClauseNode(n *ast.ReturningClauseNode) *ReturningClauseNode {
	return &ReturningClauseNode{node: n}
}

func newUnpivotArgNode(n *ast.UnpivotArgNode) *UnpivotArgNode {
	return &UnpivotArgNode{node: n}
}

func newUnpivotScanNode(n *ast.UnpivotScanNode) *UnpivotScanNode {
	return &UnpivotScanNode{node: n}
}

func newCloneDataStmtNode(n *ast.CloneDataStmtNode) *CloneDataStmtNode {
	return &CloneDataStmtNode{node: n}
}

func newTableAndColumnInfoNode(n *ast.TableAndColumnInfoNode) *TableAndColumnInfoNode {
	return &TableAndColumnInfoNode{node: n}
}

func newAnalyzeStmtNode(n *ast.AnalyzeStmtNode) *AnalyzeStmtNode {
	return &AnalyzeStmtNode{node: n}
}

func newAuxLoadDataStmtNode(n *ast.AuxLoadDataStmtNode) *AuxLoadDataStmtNode {
	return &AuxLoadDataStmtNode{node: n}
}
