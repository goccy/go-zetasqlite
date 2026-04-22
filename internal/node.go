package internal

import (
	googlesql "github.com/goccy/go-googlesql"
)

func newNode(node googlesql.ResolvedNodeNode) Formatter {
	if node == nil {
		return nil
	}
	switch m1(AsResolvedNode(node).NodeKind()) {
	case googlesql.ResolvedNodeKindResolvedLiteral:
		return newLiteralNode(node.(googlesql.ResolvedLiteralNode))
	case googlesql.ResolvedNodeKindResolvedParameter:
		return newParameterNode(node.(googlesql.ResolvedParameterNode))
	case googlesql.ResolvedNodeKindResolvedExpressionColumn:
		return newExpressionColumnNode(node.(googlesql.ResolvedExpressionColumnNode))
	case googlesql.ResolvedNodeKindResolvedColumnRef:
		return newColumnRefNode(node.(googlesql.ResolvedColumnRefNode))
	case googlesql.ResolvedNodeKindResolvedConstant:
		return newConstantNode(node.(googlesql.ResolvedConstantNode))
	case googlesql.ResolvedNodeKindResolvedSystemVariable:
		return newSystemVariableNode(node.(googlesql.ResolvedSystemVariableNode))
	case googlesql.ResolvedNodeKindResolvedInlineLambda:
		return newInlineLambdaNode(node.(googlesql.ResolvedInlineLambdaNode))
	case googlesql.ResolvedNodeKindResolvedFilterFieldArg:
		return newFilterFieldArgNode(node.(googlesql.ResolvedFilterFieldArgNode))
	case googlesql.ResolvedNodeKindResolvedFilterField:
		return newFilterFieldNode(node.(googlesql.ResolvedFilterFieldNode))
	case googlesql.ResolvedNodeKindResolvedFunctionCall:
		return newFunctionCallNode(node.(googlesql.ResolvedFunctionCallNode))
	case googlesql.ResolvedNodeKindResolvedAggregateFunctionCall:
		return newAggregateFunctionCallNode(node.(googlesql.ResolvedAggregateFunctionCallNode))
	case googlesql.ResolvedNodeKindResolvedAnalyticFunctionCall:
		return newAnalyticFunctionCallNode(node.(googlesql.ResolvedAnalyticFunctionCallNode))
	case googlesql.ResolvedNodeKindResolvedExtendedCastElement:
		return newExtendedCastElementNode(node.(googlesql.ResolvedExtendedCastElementNode))
	case googlesql.ResolvedNodeKindResolvedExtendedCast:
		return newExtendedCastNode(node.(googlesql.ResolvedExtendedCastNode))
	case googlesql.ResolvedNodeKindResolvedCast:
		return newCastNode(node.(googlesql.ResolvedCastNode))
	case googlesql.ResolvedNodeKindResolvedMakeStruct:
		return newMakeStructNode(node.(googlesql.ResolvedMakeStructNode))
	case googlesql.ResolvedNodeKindResolvedMakeProto:
		return newMakeProtoNode(node.(googlesql.ResolvedMakeProtoNode))
	case googlesql.ResolvedNodeKindResolvedMakeProtoField:
		return newMakeProtoFieldNode(node.(googlesql.ResolvedMakeProtoFieldNode))
	case googlesql.ResolvedNodeKindResolvedGetStructField:
		return newGetStructFieldNode(node.(googlesql.ResolvedGetStructFieldNode))
	case googlesql.ResolvedNodeKindResolvedGetProtoField:
		return newGetProtoFieldNode(node.(googlesql.ResolvedGetProtoFieldNode))
	case googlesql.ResolvedNodeKindResolvedGetJsonField:
		return newGetJsonFieldNode(node.(googlesql.ResolvedGetJsonFieldNode))
	case googlesql.ResolvedNodeKindResolvedFlatten:
		return newFlattenNode(node.(googlesql.ResolvedFlattenNode))
	case googlesql.ResolvedNodeKindResolvedFlattenedArg:
		return newFlattenedArgNode(node.(googlesql.ResolvedFlattenedArgNode))
	case googlesql.ResolvedNodeKindResolvedReplaceFieldItem:
		return newReplaceFieldItemNode(node.(googlesql.ResolvedReplaceFieldItemNode))
	case googlesql.ResolvedNodeKindResolvedReplaceField:
		return newReplaceFieldNode(node.(googlesql.ResolvedReplaceFieldNode))
	case googlesql.ResolvedNodeKindResolvedSubqueryExpr:
		return newSubqueryExprNode(node.(googlesql.ResolvedSubqueryExprNode))
	// ResolvedLetExpr isn't in the current googlesql export set; the
	// case used to dispatch to newLetExprNode. Reinstate when the
	// corresponding node kind re-enters the bridge.
	case googlesql.ResolvedNodeKindResolvedModel:
		return newModelNode(node.(googlesql.ResolvedModelNode))
	case googlesql.ResolvedNodeKindResolvedConnection:
		return newConnectionNode(node.(googlesql.ResolvedConnectionNode))
	case googlesql.ResolvedNodeKindResolvedDescriptor:
		return newDescriptorNode(node.(googlesql.ResolvedDescriptorNode))
	case googlesql.ResolvedNodeKindResolvedSingleRowScan:
		return newSingleRowScanNode(node.(googlesql.ResolvedSingleRowScanNode))
	case googlesql.ResolvedNodeKindResolvedTableScan:
		return newTableScanNode(node.(googlesql.ResolvedTableScanNode))
	case googlesql.ResolvedNodeKindResolvedJoinScan:
		return newJoinScanNode(node.(googlesql.ResolvedJoinScanNode))
	case googlesql.ResolvedNodeKindResolvedArrayScan:
		return newArrayScanNode(node.(googlesql.ResolvedArrayScanNode))
	case googlesql.ResolvedNodeKindResolvedColumnHolder:
		return newColumnHolderNode(node.(googlesql.ResolvedColumnHolderNode))
	case googlesql.ResolvedNodeKindResolvedFilterScan:
		return newFilterScanNode(node.(googlesql.ResolvedFilterScanNode))
	case googlesql.ResolvedNodeKindResolvedGroupingSet:
		return newGroupingSetNode(node.(googlesql.ResolvedGroupingSetNode))
	case googlesql.ResolvedNodeKindResolvedAggregateScan:
		return newAggregateScanNode(node.(googlesql.ResolvedAggregateScanNode))
	case googlesql.ResolvedNodeKindResolvedAnonymizedAggregateScan:
		return newAnonymizedAggregateScanNode(node.(googlesql.ResolvedAnonymizedAggregateScanNode))
	case googlesql.ResolvedNodeKindResolvedSetOperationItem:
		return newSetOperationItemNode(node.(googlesql.ResolvedSetOperationItemNode))
	case googlesql.ResolvedNodeKindResolvedSetOperationScan:
		return newSetOperationScanNode(node.(googlesql.ResolvedSetOperationScanNode))
	case googlesql.ResolvedNodeKindResolvedOrderByScan:
		return newOrderByScanNode(node.(googlesql.ResolvedOrderByScanNode))
	case googlesql.ResolvedNodeKindResolvedLimitOffsetScan:
		return newLimitOffsetScanNode(node.(googlesql.ResolvedLimitOffsetScanNode))
	case googlesql.ResolvedNodeKindResolvedWithRefScan:
		return newWithRefScanNode(node.(googlesql.ResolvedWithRefScanNode))
	case googlesql.ResolvedNodeKindResolvedAnalyticScan:
		return newAnalyticScanNode(node.(googlesql.ResolvedAnalyticScanNode))
	case googlesql.ResolvedNodeKindResolvedSampleScan:
		return newSampleScanNode(node.(googlesql.ResolvedSampleScanNode))
	case googlesql.ResolvedNodeKindResolvedComputedColumn:
		return newComputedColumnNode(node.(googlesql.ResolvedComputedColumnNode))
	case googlesql.ResolvedNodeKindResolvedOrderByItem:
		return newOrderByItemNode(node.(googlesql.ResolvedOrderByItemNode))
	case googlesql.ResolvedNodeKindResolvedColumnAnnotations:
		return newColumnAnnotationsNode(node.(googlesql.ResolvedColumnAnnotationsNode))
	case googlesql.ResolvedNodeKindResolvedGeneratedColumnInfo:
		return newGeneratedColumnInfoNode(node.(googlesql.ResolvedGeneratedColumnInfoNode))
	case googlesql.ResolvedNodeKindResolvedColumnDefaultValue:
		return newColumnDefaultValueNode(node.(googlesql.ResolvedColumnDefaultValueNode))
	case googlesql.ResolvedNodeKindResolvedColumnDefinition:
		return newColumnDefinitionNode(node.(googlesql.ResolvedColumnDefinitionNode))
	case googlesql.ResolvedNodeKindResolvedPrimaryKey:
		return newPrimaryKeyNode(node.(googlesql.ResolvedPrimaryKeyNode))
	case googlesql.ResolvedNodeKindResolvedForeignKey:
		return newForeignKeyNode(node.(googlesql.ResolvedForeignKeyNode))
	case googlesql.ResolvedNodeKindResolvedCheckConstraint:
		return newCheckConstraintNode(node.(googlesql.ResolvedCheckConstraintNode))
	case googlesql.ResolvedNodeKindResolvedOutputColumn:
		return newOutputColumnNode(node.(googlesql.ResolvedOutputColumnNode))
	case googlesql.ResolvedNodeKindResolvedProjectScan:
		return newProjectScanNode(node.(googlesql.ResolvedProjectScanNode))
	case ResolvedNodeKindResolvedTVFScan:
		return newTVFScanNode(node.(googlesql.ResolvedTVFScanNode))
	case googlesql.ResolvedNodeKindResolvedGroupRowsScan:
		return newGroupRowsScanNode(node.(googlesql.ResolvedGroupRowsScanNode))
	case googlesql.ResolvedNodeKindResolvedFunctionArgument:
		return newFunctionArgumentNode(node.(googlesql.ResolvedFunctionArgumentNode))
	case googlesql.ResolvedNodeKindResolvedExplainStmt:
		return newExplainStmtNode(node.(googlesql.ResolvedExplainStmtNode))
	case googlesql.ResolvedNodeKindResolvedQueryStmt:
		return newQueryStmtNode(node.(googlesql.ResolvedQueryStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateDatabaseStmt:
		return newCreateDatabaseStmtNode(node.(googlesql.ResolvedCreateDatabaseStmtNode))
	case googlesql.ResolvedNodeKindResolvedIndexItem:
		return newIndexItemNode(node.(googlesql.ResolvedIndexItemNode))
	case googlesql.ResolvedNodeKindResolvedUnnestItem:
		return newUnnestItemNode(node.(googlesql.ResolvedUnnestItemNode))
	case googlesql.ResolvedNodeKindResolvedCreateIndexStmt:
		return newCreateIndexStmtNode(node.(googlesql.ResolvedCreateIndexStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateSchemaStmt:
		return newCreateSchemaStmtNode(node.(googlesql.ResolvedCreateSchemaStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateTableStmt:
		return newCreateTableStmtNode(node.(googlesql.ResolvedCreateTableStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateTableAsSelectStmt:
		return newCreateTableAsSelectStmtNode(node.(googlesql.ResolvedCreateTableAsSelectStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateModelStmt:
		return newCreateModelStmtNode(node.(googlesql.ResolvedCreateModelStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateViewStmt:
		return newCreateViewStmtNode(node.(googlesql.ResolvedCreateViewStmtNode))
	case googlesql.ResolvedNodeKindResolvedWithPartitionColumns:
		return newWithPartitionColumnsNode(node.(googlesql.ResolvedWithPartitionColumnsNode))
	case googlesql.ResolvedNodeKindResolvedCreateSnapshotTableStmt:
		return newCreateSnapshotTableStmtNode(node.(googlesql.ResolvedCreateSnapshotTableStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateExternalTableStmt:
		return newCreateExternalTableStmtNode(node.(googlesql.ResolvedCreateExternalTableStmtNode))
	case googlesql.ResolvedNodeKindResolvedExportModelStmt:
		return newExportModelStmtNode(node.(googlesql.ResolvedExportModelStmtNode))
	case googlesql.ResolvedNodeKindResolvedExportDataStmt:
		return newExportDataStmtNode(node.(googlesql.ResolvedExportDataStmtNode))
	case googlesql.ResolvedNodeKindResolvedDefineTableStmt:
		return newDefineTableStmtNode(node.(googlesql.ResolvedDefineTableStmtNode))
	case googlesql.ResolvedNodeKindResolvedDescribeStmt:
		return newDescribeStmtNode(node.(googlesql.ResolvedDescribeStmtNode))
	case googlesql.ResolvedNodeKindResolvedShowStmt:
		return newShowStmtNode(node.(googlesql.ResolvedShowStmtNode))
	case googlesql.ResolvedNodeKindResolvedBeginStmt:
		return newBeginStmtNode(node.(googlesql.ResolvedBeginStmtNode))
	case googlesql.ResolvedNodeKindResolvedSetTransactionStmt:
		return newSetTransactionStmtNode(node.(googlesql.ResolvedSetTransactionStmtNode))
	case googlesql.ResolvedNodeKindResolvedCommitStmt:
		return newCommitStmtNode(node.(googlesql.ResolvedCommitStmtNode))
	case googlesql.ResolvedNodeKindResolvedRollbackStmt:
		return newRollbackStmtNode(node.(googlesql.ResolvedRollbackStmtNode))
	case googlesql.ResolvedNodeKindResolvedStartBatchStmt:
		return newStartBatchStmtNode(node.(googlesql.ResolvedStartBatchStmtNode))
	case googlesql.ResolvedNodeKindResolvedRunBatchStmt:
		return newRunBatchStmtNode(node.(googlesql.ResolvedRunBatchStmtNode))
	case googlesql.ResolvedNodeKindResolvedAbortBatchStmt:
		return newAbortBatchStmtNode(node.(googlesql.ResolvedAbortBatchStmtNode))
	case googlesql.ResolvedNodeKindResolvedDropStmt:
		return newDropStmtNode(node.(googlesql.ResolvedDropStmtNode))
	case googlesql.ResolvedNodeKindResolvedDropMaterializedViewStmt:
		return newDropMaterializedViewStmtNode(node.(googlesql.ResolvedDropMaterializedViewStmtNode))
	case googlesql.ResolvedNodeKindResolvedDropSnapshotTableStmt:
		return newDropSnapshotTableStmtNode(node.(googlesql.ResolvedDropSnapshotTableStmtNode))
	case googlesql.ResolvedNodeKindResolvedRecursiveRefScan:
		return newRecursiveRefScanNode(node.(googlesql.ResolvedRecursiveRefScanNode))
	case googlesql.ResolvedNodeKindResolvedRecursiveScan:
		return newRecursiveScanNode(node.(googlesql.ResolvedRecursiveScanNode))
	case googlesql.ResolvedNodeKindResolvedWithScan:
		return newWithScanNode(node.(googlesql.ResolvedWithScanNode))
	case googlesql.ResolvedNodeKindResolvedWithEntry:
		return newWithEntryNode(node.(googlesql.ResolvedWithEntryNode))
	case googlesql.ResolvedNodeKindResolvedOption:
		return newOptionNode(node.(googlesql.ResolvedOptionNode))
	case googlesql.ResolvedNodeKindResolvedWindowPartitioning:
		return newWindowPartitioningNode(node.(googlesql.ResolvedWindowPartitioningNode))
	case googlesql.ResolvedNodeKindResolvedWindowOrdering:
		return newWindowOrderingNode(node.(googlesql.ResolvedWindowOrderingNode))
	case googlesql.ResolvedNodeKindResolvedWindowFrame:
		return newWindowFrameNode(node.(googlesql.ResolvedWindowFrameNode))
	case googlesql.ResolvedNodeKindResolvedAnalyticFunctionGroup:
		return newAnalyticFunctionGroupNode(node.(googlesql.ResolvedAnalyticFunctionGroupNode))
	case googlesql.ResolvedNodeKindResolvedWindowFrameExpr:
		return newWindowFrameExprNode(node.(googlesql.ResolvedWindowFrameExprNode))
	case ResolvedNodeKindResolvedDMLValue:
		return newDMLValueNode(node.(googlesql.ResolvedDMLValueNode))
	case ResolvedNodeKindResolvedDMLDefault:
		return newDMLDefaultNode(node.(googlesql.ResolvedDMLDefaultNode))
	case googlesql.ResolvedNodeKindResolvedAssertStmt:
		return newAssertStmtNode(node.(googlesql.ResolvedAssertStmtNode))
	case googlesql.ResolvedNodeKindResolvedAssertRowsModified:
		return newAssertRowsModifiedNode(node.(googlesql.ResolvedAssertRowsModifiedNode))
	case googlesql.ResolvedNodeKindResolvedInsertRow:
		return newInsertRowNode(node.(googlesql.ResolvedInsertRowNode))
	case googlesql.ResolvedNodeKindResolvedInsertStmt:
		return newInsertStmtNode(node.(googlesql.ResolvedInsertStmtNode))
	case googlesql.ResolvedNodeKindResolvedDeleteStmt:
		return newDeleteStmtNode(node.(googlesql.ResolvedDeleteStmtNode))
	case googlesql.ResolvedNodeKindResolvedUpdateItem:
		return newUpdateItemNode(node.(googlesql.ResolvedUpdateItemNode))
	// ResolvedUpdateArrayItem isn't in the current googlesql export set.
	case googlesql.ResolvedNodeKindResolvedUpdateStmt:
		return newUpdateStmtNode(node.(googlesql.ResolvedUpdateStmtNode))
	case googlesql.ResolvedNodeKindResolvedMergeWhen:
		return newMergeWhenNode(node.(googlesql.ResolvedMergeWhenNode))
	case googlesql.ResolvedNodeKindResolvedMergeStmt:
		return newMergeStmtNode(node.(googlesql.ResolvedMergeStmtNode))
	case googlesql.ResolvedNodeKindResolvedTruncateStmt:
		return newTruncateStmtNode(node.(googlesql.ResolvedTruncateStmtNode))
	case googlesql.ResolvedNodeKindResolvedObjectUnit:
		return newObjectUnitNode(node.(googlesql.ResolvedObjectUnitNode))
	case googlesql.ResolvedNodeKindResolvedPrivilege:
		return newPrivilegeNode(node.(googlesql.ResolvedPrivilegeNode))
	case googlesql.ResolvedNodeKindResolvedGrantStmt:
		return newGrantStmtNode(node.(googlesql.ResolvedGrantStmtNode))
	case googlesql.ResolvedNodeKindResolvedRevokeStmt:
		return newRevokeStmtNode(node.(googlesql.ResolvedRevokeStmtNode))
	case googlesql.ResolvedNodeKindResolvedAlterDatabaseStmt:
		return newAlterDatabaseStmtNode(node.(googlesql.ResolvedAlterDatabaseStmtNode))
	case googlesql.ResolvedNodeKindResolvedAlterMaterializedViewStmt:
		return newAlterMaterializedViewStmtNode(node.(googlesql.ResolvedAlterMaterializedViewStmtNode))
	case googlesql.ResolvedNodeKindResolvedAlterSchemaStmt:
		return newAlterSchemaStmtNode(node.(googlesql.ResolvedAlterSchemaStmtNode))
	case googlesql.ResolvedNodeKindResolvedAlterTableStmt:
		return newAlterTableStmtNode(node.(googlesql.ResolvedAlterTableStmtNode))
	case googlesql.ResolvedNodeKindResolvedAlterViewStmt:
		return newAlterViewStmtNode(node.(googlesql.ResolvedAlterViewStmtNode))
	case googlesql.ResolvedNodeKindResolvedSetOptionsAction:
		return newSetOptionsActionNode(node.(googlesql.ResolvedSetOptionsActionNode))
	case googlesql.ResolvedNodeKindResolvedAddColumnAction:
		return newAddColumnActionNode(node.(googlesql.ResolvedAddColumnActionNode))
	case googlesql.ResolvedNodeKindResolvedAddConstraintAction:
		return newAddConstraintActionNode(node.(googlesql.ResolvedAddConstraintActionNode))
	case googlesql.ResolvedNodeKindResolvedDropConstraintAction:
		return newDropConstraintActionNode(node.(googlesql.ResolvedDropConstraintActionNode))
	case googlesql.ResolvedNodeKindResolvedDropPrimaryKeyAction:
		return newDropPrimaryKeyActionNode(node.(googlesql.ResolvedDropPrimaryKeyActionNode))
	case googlesql.ResolvedNodeKindResolvedAlterColumnOptionsAction:
		return newAlterColumnOptionsActionNode(node.(googlesql.ResolvedAlterColumnOptionsActionNode))
	case googlesql.ResolvedNodeKindResolvedAlterColumnDropNotNullAction:
		return newAlterColumnDropNotNullActionNode(node.(googlesql.ResolvedAlterColumnDropNotNullActionNode))
	case googlesql.ResolvedNodeKindResolvedAlterColumnSetDataTypeAction:
		return newAlterColumnSetDataTypeActionNode(node.(googlesql.ResolvedAlterColumnSetDataTypeActionNode))
	case googlesql.ResolvedNodeKindResolvedAlterColumnSetDefaultAction:
		return newAlterColumnSetDefaultActionNode(node.(googlesql.ResolvedAlterColumnSetDefaultActionNode))
	case googlesql.ResolvedNodeKindResolvedAlterColumnDropDefaultAction:
		return newAlterColumnDropDefaultActionNode(node.(googlesql.ResolvedAlterColumnDropDefaultActionNode))
	case googlesql.ResolvedNodeKindResolvedDropColumnAction:
		return newDropColumnActionNode(node.(googlesql.ResolvedDropColumnActionNode))
	case googlesql.ResolvedNodeKindResolvedRenameColumnAction:
		return newRenameColumnActionNode(node.(googlesql.ResolvedRenameColumnActionNode))
	case googlesql.ResolvedNodeKindResolvedSetAsAction:
		return newSetAsActionNode(node.(googlesql.ResolvedSetAsActionNode))
	case googlesql.ResolvedNodeKindResolvedSetCollateClause:
		return newSetCollateClauseNode(node.(googlesql.ResolvedSetCollateClauseNode))
	case googlesql.ResolvedNodeKindResolvedAlterTableSetOptionsStmt:
		return newAlterTableSetOptionsStmtNode(node.(googlesql.ResolvedAlterTableSetOptionsStmtNode))
	case googlesql.ResolvedNodeKindResolvedRenameStmt:
		return newRenameStmtNode(node.(googlesql.ResolvedRenameStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreatePrivilegeRestrictionStmt:
		return newCreatePrivilegeRestrictionStmtNode(node.(googlesql.ResolvedCreatePrivilegeRestrictionStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateRowAccessPolicyStmt:
		return newCreateRowAccessPolicyStmtNode(node.(googlesql.ResolvedCreateRowAccessPolicyStmtNode))
	case googlesql.ResolvedNodeKindResolvedDropPrivilegeRestrictionStmt:
		return newDropPrivilegeRestrictionStmtNode(node.(googlesql.ResolvedDropPrivilegeRestrictionStmtNode))
	case googlesql.ResolvedNodeKindResolvedDropRowAccessPolicyStmt:
		return newDropRowAccessPolicyStmtNode(node.(googlesql.ResolvedDropRowAccessPolicyStmtNode))
	// ResolvedDropSearchIndexStmt isn't in the current googlesql export set.
	case googlesql.ResolvedNodeKindResolvedGrantToAction:
		return newGrantToActionNode(node.(googlesql.ResolvedGrantToActionNode))
	case googlesql.ResolvedNodeKindResolvedRestrictToAction:
		return newRestrictToActionNode(node.(googlesql.ResolvedRestrictToActionNode))
	case googlesql.ResolvedNodeKindResolvedAddToRestricteeListAction:
		return newAddToRestricteeListActionNode(node.(googlesql.ResolvedAddToRestricteeListActionNode))
	case googlesql.ResolvedNodeKindResolvedRemoveFromRestricteeListAction:
		return newRemoveFromRestricteeListActionNode(node.(googlesql.ResolvedRemoveFromRestricteeListActionNode))
	case googlesql.ResolvedNodeKindResolvedFilterUsingAction:
		return newFilterUsingActionNode(node.(googlesql.ResolvedFilterUsingActionNode))
	case googlesql.ResolvedNodeKindResolvedRevokeFromAction:
		return newRevokeFromActionNode(node.(googlesql.ResolvedRevokeFromActionNode))
	case googlesql.ResolvedNodeKindResolvedRenameToAction:
		return newRenameToActionNode(node.(googlesql.ResolvedRenameToActionNode))
	case googlesql.ResolvedNodeKindResolvedAlterPrivilegeRestrictionStmt:
		return newAlterPrivilegeRestrictionStmtNode(node.(googlesql.ResolvedAlterPrivilegeRestrictionStmtNode))
	case googlesql.ResolvedNodeKindResolvedAlterRowAccessPolicyStmt:
		return newAlterRowAccessPolicyStmtNode(node.(googlesql.ResolvedAlterRowAccessPolicyStmtNode))
	case googlesql.ResolvedNodeKindResolvedAlterAllRowAccessPoliciesStmt:
		return newAlterAllRowAccessPoliciesStmtNode(node.(googlesql.ResolvedAlterAllRowAccessPoliciesStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateConstantStmt:
		return newCreateConstantStmtNode(node.(googlesql.ResolvedCreateConstantStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateFunctionStmt:
		return newCreateFunctionStmtNode(node.(googlesql.ResolvedCreateFunctionStmtNode))
	case googlesql.ResolvedNodeKindResolvedArgumentDef:
		return newArgumentDefNode(node.(googlesql.ResolvedArgumentDefNode))
	case googlesql.ResolvedNodeKindResolvedArgumentRef:
		return newArgumentRefNode(node.(googlesql.ResolvedArgumentRefNode))
	case googlesql.ResolvedNodeKindResolvedCreateTableFunctionStmt:
		return newCreateTableFunctionStmtNode(node.(googlesql.ResolvedCreateTableFunctionStmtNode))
	case googlesql.ResolvedNodeKindResolvedRelationArgumentScan:
		return newRelationArgumentScanNode(node.(googlesql.ResolvedRelationArgumentScanNode))
	case googlesql.ResolvedNodeKindResolvedArgumentList:
		return newArgumentListNode(node.(googlesql.ResolvedArgumentListNode))
	case googlesql.ResolvedNodeKindResolvedFunctionSignatureHolder:
		return newFunctionSignatureHolderNode(node.(googlesql.ResolvedFunctionSignatureHolderNode))
	case googlesql.ResolvedNodeKindResolvedDropFunctionStmt:
		return newDropFunctionStmtNode(node.(googlesql.ResolvedDropFunctionStmtNode))
	case googlesql.ResolvedNodeKindResolvedDropTableFunctionStmt:
		return newDropTableFunctionStmtNode(node.(googlesql.ResolvedDropTableFunctionStmtNode))
	case googlesql.ResolvedNodeKindResolvedCallStmt:
		return newCallStmtNode(node.(googlesql.ResolvedCallStmtNode))
	case googlesql.ResolvedNodeKindResolvedImportStmt:
		return newImportStmtNode(node.(googlesql.ResolvedImportStmtNode))
	case googlesql.ResolvedNodeKindResolvedModuleStmt:
		return newModuleStmtNode(node.(googlesql.ResolvedModuleStmtNode))
	case googlesql.ResolvedNodeKindResolvedAggregateHavingModifier:
		return newAggregateHavingModifierNode(node.(googlesql.ResolvedAggregateHavingModifierNode))
	case googlesql.ResolvedNodeKindResolvedCreateMaterializedViewStmt:
		return newCreateMaterializedViewStmtNode(node.(googlesql.ResolvedCreateMaterializedViewStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateProcedureStmt:
		return newCreateProcedureStmtNode(node.(googlesql.ResolvedCreateProcedureStmtNode))
	case googlesql.ResolvedNodeKindResolvedExecuteImmediateArgument:
		return newExecuteImmediateArgumentNode(node.(googlesql.ResolvedExecuteImmediateArgumentNode))
	case googlesql.ResolvedNodeKindResolvedExecuteImmediateStmt:
		return newExecuteImmediateStmtNode(node.(googlesql.ResolvedExecuteImmediateStmtNode))
	case googlesql.ResolvedNodeKindResolvedAssignmentStmt:
		return newAssignmentStmtNode(node.(googlesql.ResolvedAssignmentStmtNode))
	case googlesql.ResolvedNodeKindResolvedCreateEntityStmt:
		return newCreateEntityStmtNode(node.(googlesql.ResolvedCreateEntityStmtNode))
	case googlesql.ResolvedNodeKindResolvedAlterEntityStmt:
		return newAlterEntityStmtNode(node.(googlesql.ResolvedAlterEntityStmtNode))
	case googlesql.ResolvedNodeKindResolvedPivotColumn:
		return newPivotColumnNode(node.(googlesql.ResolvedPivotColumnNode))
	case googlesql.ResolvedNodeKindResolvedPivotScan:
		return newPivotScanNode(node.(googlesql.ResolvedPivotScanNode))
	case googlesql.ResolvedNodeKindResolvedReturningClause:
		return newReturningClauseNode(node.(googlesql.ResolvedReturningClauseNode))
	case googlesql.ResolvedNodeKindResolvedUnpivotArg:
		return newUnpivotArgNode(node.(googlesql.ResolvedUnpivotArgNode))
	case googlesql.ResolvedNodeKindResolvedUnpivotScan:
		return newUnpivotScanNode(node.(googlesql.ResolvedUnpivotScanNode))
	case googlesql.ResolvedNodeKindResolvedCloneDataStmt:
		return newCloneDataStmtNode(node.(googlesql.ResolvedCloneDataStmtNode))
	case googlesql.ResolvedNodeKindResolvedTableAndColumnInfo:
		return newTableAndColumnInfoNode(node.(googlesql.ResolvedTableAndColumnInfoNode))
	case googlesql.ResolvedNodeKindResolvedAnalyzeStmt:
		return newAnalyzeStmtNode(node.(googlesql.ResolvedAnalyzeStmtNode))
	case googlesql.ResolvedNodeKindResolvedAuxLoadDataStmt:
		return newAuxLoadDataStmtNode(node.(googlesql.ResolvedAuxLoadDataStmtNode))
	}
	return nil
}

type LiteralNode struct {
	node googlesql.ResolvedLiteralNode
}

type ParameterNode struct {
	node googlesql.ResolvedParameterNode
}

type ExpressionColumnNode struct {
	node googlesql.ResolvedExpressionColumnNode
}

type ColumnRefNode struct {
	node googlesql.ResolvedColumnRefNode
}

type ConstantNode struct {
	node googlesql.ResolvedConstantNode
}

type SystemVariableNode struct {
	node googlesql.ResolvedSystemVariableNode
}

type InlineLambdaNode struct {
	node googlesql.ResolvedInlineLambdaNode
}

type FilterFieldArgNode struct {
	node googlesql.ResolvedFilterFieldArgNode
}

type FilterFieldNode struct {
	node googlesql.ResolvedFilterFieldNode
}

type FunctionCallNode struct {
	node googlesql.ResolvedFunctionCallNode
}

type AggregateFunctionCallNode struct {
	node googlesql.ResolvedAggregateFunctionCallNode
}

type AnalyticFunctionCallNode struct {
	node googlesql.ResolvedAnalyticFunctionCallNode
}

type ExtendedCastElementNode struct {
	node googlesql.ResolvedExtendedCastElementNode
}

type ExtendedCastNode struct {
	node googlesql.ResolvedExtendedCastNode
}

type CastNode struct {
	node googlesql.ResolvedCastNode
}

type MakeStructNode struct {
	node googlesql.ResolvedMakeStructNode
}

type MakeProtoNode struct {
	node googlesql.ResolvedMakeProtoNode
}

type MakeProtoFieldNode struct {
	node googlesql.ResolvedMakeProtoFieldNode
}

type GetStructFieldNode struct {
	node googlesql.ResolvedGetStructFieldNode
}

type GetProtoFieldNode struct {
	node googlesql.ResolvedGetProtoFieldNode
}

type GetJsonFieldNode struct {
	node googlesql.ResolvedGetJsonFieldNode
}

type FlattenNode struct {
	node googlesql.ResolvedFlattenNode
}

type FlattenedArgNode struct {
	node googlesql.ResolvedFlattenedArgNode
}

type ReplaceFieldItemNode struct {
	node googlesql.ResolvedReplaceFieldItemNode
}

type ReplaceFieldNode struct {
	node googlesql.ResolvedReplaceFieldNode
}

type SubqueryExprNode struct {
	node googlesql.ResolvedSubqueryExprNode
}

type LetExprNode struct {
	node *ResolvedLetExprNode
}

type ModelNode struct {
	node googlesql.ResolvedModelNode
}

type ConnectionNode struct {
	node googlesql.ResolvedConnectionNode
}

type DescriptorNode struct {
	node googlesql.ResolvedDescriptorNode
}

type SingleRowScanNode struct {
	node googlesql.ResolvedSingleRowScanNode
}

type TableScanNode struct {
	node googlesql.ResolvedTableScanNode
}

type JoinScanNode struct {
	node googlesql.ResolvedJoinScanNode
}

type ArrayScanNode struct {
	node googlesql.ResolvedArrayScanNode
}

type ColumnHolderNode struct {
	node googlesql.ResolvedColumnHolderNode
}

type FilterScanNode struct {
	node googlesql.ResolvedFilterScanNode
}

type GroupingSetNode struct {
	node googlesql.ResolvedGroupingSetNode
}

type AggregateScanNode struct {
	node googlesql.ResolvedAggregateScanNode
}

type AnonymizedAggregateScanNode struct {
	node googlesql.ResolvedAnonymizedAggregateScanNode
}

type SetOperationItemNode struct {
	node googlesql.ResolvedSetOperationItemNode
}

type SetOperationScanNode struct {
	node googlesql.ResolvedSetOperationScanNode
}

type OrderByScanNode struct {
	node googlesql.ResolvedOrderByScanNode
}

type LimitOffsetScanNode struct {
	node googlesql.ResolvedLimitOffsetScanNode
}

type WithRefScanNode struct {
	node googlesql.ResolvedWithRefScanNode
}

type AnalyticScanNode struct {
	node googlesql.ResolvedAnalyticScanNode
}

type SampleScanNode struct {
	node googlesql.ResolvedSampleScanNode
}

type ComputedColumnNode struct {
	node googlesql.ResolvedComputedColumnNode
}

type OrderByItemNode struct {
	node googlesql.ResolvedOrderByItemNode
}

type ColumnAnnotationsNode struct {
	node googlesql.ResolvedColumnAnnotationsNode
}

type GeneratedColumnInfoNode struct {
	node googlesql.ResolvedGeneratedColumnInfoNode
}

type ColumnDefaultValueNode struct {
	node googlesql.ResolvedColumnDefaultValueNode
}

type ColumnDefinitionNode struct {
	node googlesql.ResolvedColumnDefinitionNode
}

type PrimaryKeyNode struct {
	node googlesql.ResolvedPrimaryKeyNode
}

type ForeignKeyNode struct {
	node googlesql.ResolvedForeignKeyNode
}

type CheckConstraintNode struct {
	node googlesql.ResolvedCheckConstraintNode
}

type OutputColumnNode struct {
	node googlesql.ResolvedOutputColumnNode
}

type ProjectScanNode struct {
	node googlesql.ResolvedProjectScanNode
}

type TVFScanNode struct {
	node googlesql.ResolvedTVFScanNode
}

type GroupRowsScanNode struct {
	node googlesql.ResolvedGroupRowsScanNode
}

type FunctionArgumentNode struct {
	node googlesql.ResolvedFunctionArgumentNode
}

type ExplainStmtNode struct {
	node googlesql.ResolvedExplainStmtNode
}

type QueryStmtNode struct {
	node googlesql.ResolvedQueryStmtNode
}

type CreateDatabaseStmtNode struct {
	node googlesql.ResolvedCreateDatabaseStmtNode
}

type IndexItemNode struct {
	node googlesql.ResolvedIndexItemNode
}

type UnnestItemNode struct {
	node googlesql.ResolvedUnnestItemNode
}

type CreateIndexStmtNode struct {
	node googlesql.ResolvedCreateIndexStmtNode
}

type CreateSchemaStmtNode struct {
	node googlesql.ResolvedCreateSchemaStmtNode
}

type CreateTableStmtNode struct {
	node googlesql.ResolvedCreateTableStmtNode
}

type CreateTableAsSelectStmtNode struct {
	node googlesql.ResolvedCreateTableAsSelectStmtNode
}

type CreateModelStmtNode struct {
	node googlesql.ResolvedCreateModelStmtNode
}

type CreateViewStmtNode struct {
	node googlesql.ResolvedCreateViewStmtNode
}

type WithPartitionColumnsNode struct {
	node googlesql.ResolvedWithPartitionColumnsNode
}

type CreateSnapshotTableStmtNode struct {
	node googlesql.ResolvedCreateSnapshotTableStmtNode
}

type CreateExternalTableStmtNode struct {
	node googlesql.ResolvedCreateExternalTableStmtNode
}

type ExportModelStmtNode struct {
	node googlesql.ResolvedExportModelStmtNode
}

type ExportDataStmtNode struct {
	node googlesql.ResolvedExportDataStmtNode
}

type DefineTableStmtNode struct {
	node googlesql.ResolvedDefineTableStmtNode
}

type DescribeStmtNode struct {
	node googlesql.ResolvedDescribeStmtNode
}

type ShowStmtNode struct {
	node googlesql.ResolvedShowStmtNode
}

type BeginStmtNode struct {
	node googlesql.ResolvedBeginStmtNode
}

type SetTransactionStmtNode struct {
	node googlesql.ResolvedSetTransactionStmtNode
}

type CommitStmtNode struct {
	node googlesql.ResolvedCommitStmtNode
}

type RollbackStmtNode struct {
	node googlesql.ResolvedRollbackStmtNode
}

type StartBatchStmtNode struct {
	node googlesql.ResolvedStartBatchStmtNode
}

type RunBatchStmtNode struct {
	node googlesql.ResolvedRunBatchStmtNode
}

type AbortBatchStmtNode struct {
	node googlesql.ResolvedAbortBatchStmtNode
}

type DropStmtNode struct {
	node googlesql.ResolvedDropStmtNode
}

type DropMaterializedViewStmtNode struct {
	node googlesql.ResolvedDropMaterializedViewStmtNode
}

type DropSnapshotTableStmtNode struct {
	node googlesql.ResolvedDropSnapshotTableStmtNode
}

type RecursiveRefScanNode struct {
	node googlesql.ResolvedRecursiveRefScanNode
}

type RecursiveScanNode struct {
	node googlesql.ResolvedRecursiveScanNode
}

type WithScanNode struct {
	node googlesql.ResolvedWithScanNode
}

type WithEntryNode struct {
	node googlesql.ResolvedWithEntryNode
}

type OptionNode struct {
	node googlesql.ResolvedOptionNode
}

type WindowPartitioningNode struct {
	node googlesql.ResolvedWindowPartitioningNode
}

type WindowOrderingNode struct {
	node googlesql.ResolvedWindowOrderingNode
}

type WindowFrameNode struct {
	node googlesql.ResolvedWindowFrameNode
}

type AnalyticFunctionGroupNode struct {
	node googlesql.ResolvedAnalyticFunctionGroupNode
}

type WindowFrameExprNode struct {
	node googlesql.ResolvedWindowFrameExprNode
}

type DMLValueNode struct {
	node googlesql.ResolvedDMLValueNode
}

type DMLDefaultNode struct {
	node googlesql.ResolvedDMLDefaultNode
}

type AssertStmtNode struct {
	node googlesql.ResolvedAssertStmtNode
}

type AssertRowsModifiedNode struct {
	node googlesql.ResolvedAssertRowsModifiedNode
}

type InsertRowNode struct {
	node googlesql.ResolvedInsertRowNode
}

type InsertStmtNode struct {
	node googlesql.ResolvedInsertStmtNode
}

type DeleteStmtNode struct {
	node googlesql.ResolvedDeleteStmtNode
}

type UpdateItemNode struct {
	node googlesql.ResolvedUpdateItemNode
}

type UpdateArrayItemNode struct {
	node *ResolvedUpdateArrayItemNode
}

type UpdateStmtNode struct {
	node googlesql.ResolvedUpdateStmtNode
}

type MergeWhenNode struct {
	node googlesql.ResolvedMergeWhenNode
}

type MergeStmtNode struct {
	node googlesql.ResolvedMergeStmtNode
}

type TruncateStmtNode struct {
	node googlesql.ResolvedTruncateStmtNode
}

type ObjectUnitNode struct {
	node googlesql.ResolvedObjectUnitNode
}

type PrivilegeNode struct {
	node googlesql.ResolvedPrivilegeNode
}

type GrantStmtNode struct {
	node googlesql.ResolvedGrantStmtNode
}

type RevokeStmtNode struct {
	node googlesql.ResolvedRevokeStmtNode
}

type AlterDatabaseStmtNode struct {
	node googlesql.ResolvedAlterDatabaseStmtNode
}

type AlterMaterializedViewStmtNode struct {
	node googlesql.ResolvedAlterMaterializedViewStmtNode
}

type AlterSchemaStmtNode struct {
	node googlesql.ResolvedAlterSchemaStmtNode
}

type AlterTableStmtNode struct {
	node googlesql.ResolvedAlterTableStmtNode
}

type AlterViewStmtNode struct {
	node googlesql.ResolvedAlterViewStmtNode
}

type SetOptionsActionNode struct {
	node googlesql.ResolvedSetOptionsActionNode
}

type AddColumnActionNode struct {
	node googlesql.ResolvedAddColumnActionNode
}

type AddConstraintActionNode struct {
	node googlesql.ResolvedAddConstraintActionNode
}

type DropConstraintActionNode struct {
	node googlesql.ResolvedDropConstraintActionNode
}

type DropPrimaryKeyActionNode struct {
	node googlesql.ResolvedDropPrimaryKeyActionNode
}

type AlterColumnOptionsActionNode struct {
	node googlesql.ResolvedAlterColumnOptionsActionNode
}

type AlterColumnDropNotNullActionNode struct {
	node googlesql.ResolvedAlterColumnDropNotNullActionNode
}

type AlterColumnSetDataTypeActionNode struct {
	node googlesql.ResolvedAlterColumnSetDataTypeActionNode
}

type AlterColumnSetDefaultActionNode struct {
	node googlesql.ResolvedAlterColumnSetDefaultActionNode
}

type AlterColumnDropDefaultActionNode struct {
	node googlesql.ResolvedAlterColumnDropDefaultActionNode
}

type DropColumnActionNode struct {
	node googlesql.ResolvedDropColumnActionNode
}

type RenameColumnActionNode struct {
	node googlesql.ResolvedRenameColumnActionNode
}

type SetAsActionNode struct {
	node googlesql.ResolvedSetAsActionNode
}

type SetCollateClauseNode struct {
	node googlesql.ResolvedSetCollateClauseNode
}

type AlterTableSetOptionsStmtNode struct {
	node googlesql.ResolvedAlterTableSetOptionsStmtNode
}

type RenameStmtNode struct {
	node googlesql.ResolvedRenameStmtNode
}

type CreatePrivilegeRestrictionStmtNode struct {
	node googlesql.ResolvedCreatePrivilegeRestrictionStmtNode
}

type CreateRowAccessPolicyStmtNode struct {
	node googlesql.ResolvedCreateRowAccessPolicyStmtNode
}

type DropPrivilegeRestrictionStmtNode struct {
	node googlesql.ResolvedDropPrivilegeRestrictionStmtNode
}

type DropRowAccessPolicyStmtNode struct {
	node googlesql.ResolvedDropRowAccessPolicyStmtNode
}

type DropSearchIndexStmtNode struct {
	node *ResolvedDropSearchIndexStmtNode
}

type GrantToActionNode struct {
	node googlesql.ResolvedGrantToActionNode
}

type RestrictToActionNode struct {
	node googlesql.ResolvedRestrictToActionNode
}

type AddToRestricteeListActionNode struct {
	node googlesql.ResolvedAddToRestricteeListActionNode
}

type RemoveFromRestricteeListActionNode struct {
	node googlesql.ResolvedRemoveFromRestricteeListActionNode
}

type FilterUsingActionNode struct {
	node googlesql.ResolvedFilterUsingActionNode
}

type RevokeFromActionNode struct {
	node googlesql.ResolvedRevokeFromActionNode
}

type RenameToActionNode struct {
	node googlesql.ResolvedRenameToActionNode
}

type AlterPrivilegeRestrictionStmtNode struct {
	node googlesql.ResolvedAlterPrivilegeRestrictionStmtNode
}

type AlterRowAccessPolicyStmtNode struct {
	node googlesql.ResolvedAlterRowAccessPolicyStmtNode
}

type AlterAllRowAccessPoliciesStmtNode struct {
	node googlesql.ResolvedAlterAllRowAccessPoliciesStmtNode
}

type CreateConstantStmtNode struct {
	node googlesql.ResolvedCreateConstantStmtNode
}

type CreateFunctionStmtNode struct {
	node googlesql.ResolvedCreateFunctionStmtNode
}

type ArgumentDefNode struct {
	node googlesql.ResolvedArgumentDefNode
}

type ArgumentRefNode struct {
	node googlesql.ResolvedArgumentRefNode
}

type CreateTableFunctionStmtNode struct {
	node googlesql.ResolvedCreateTableFunctionStmtNode
}

type RelationArgumentScanNode struct {
	node googlesql.ResolvedRelationArgumentScanNode
}

type ArgumentListNode struct {
	node googlesql.ResolvedArgumentListNode
}

type FunctionSignatureHolderNode struct {
	node googlesql.ResolvedFunctionSignatureHolderNode
}

type DropFunctionStmtNode struct {
	node googlesql.ResolvedDropFunctionStmtNode
}

type DropTableFunctionStmtNode struct {
	node googlesql.ResolvedDropTableFunctionStmtNode
}

type CallStmtNode struct {
	node googlesql.ResolvedCallStmtNode
}

type ImportStmtNode struct {
	node googlesql.ResolvedImportStmtNode
}

type ModuleStmtNode struct {
	node googlesql.ResolvedModuleStmtNode
}

type AggregateHavingModifierNode struct {
	node googlesql.ResolvedAggregateHavingModifierNode
}

type CreateMaterializedViewStmtNode struct {
	node googlesql.ResolvedCreateMaterializedViewStmtNode
}

type CreateProcedureStmtNode struct {
	node googlesql.ResolvedCreateProcedureStmtNode
}

type ExecuteImmediateArgumentNode struct {
	node googlesql.ResolvedExecuteImmediateArgumentNode
}

type ExecuteImmediateStmtNode struct {
	node googlesql.ResolvedExecuteImmediateStmtNode
}

type AssignmentStmtNode struct {
	node googlesql.ResolvedAssignmentStmtNode
}

type CreateEntityStmtNode struct {
	node googlesql.ResolvedCreateEntityStmtNode
}

type AlterEntityStmtNode struct {
	node googlesql.ResolvedAlterEntityStmtNode
}

type PivotColumnNode struct {
	node googlesql.ResolvedPivotColumnNode
}

type PivotScanNode struct {
	node googlesql.ResolvedPivotScanNode
}

type ReturningClauseNode struct {
	node googlesql.ResolvedReturningClauseNode
}

type UnpivotArgNode struct {
	node googlesql.ResolvedUnpivotArgNode
}

type UnpivotScanNode struct {
	node googlesql.ResolvedUnpivotScanNode
}

type CloneDataStmtNode struct {
	node googlesql.ResolvedCloneDataStmtNode
}

type TableAndColumnInfoNode struct {
	node googlesql.ResolvedTableAndColumnInfoNode
}

type AnalyzeStmtNode struct {
	node googlesql.ResolvedAnalyzeStmtNode
}

type AuxLoadDataStmtNode struct {
	node googlesql.ResolvedAuxLoadDataStmtNode
}

func newLiteralNode(n googlesql.ResolvedLiteralNode) *LiteralNode {
	return &LiteralNode{node: n}
}

func newParameterNode(n googlesql.ResolvedParameterNode) *ParameterNode {
	return &ParameterNode{node: n}
}

func newExpressionColumnNode(n googlesql.ResolvedExpressionColumnNode) *ExpressionColumnNode {
	return &ExpressionColumnNode{node: n}
}

func newColumnRefNode(n googlesql.ResolvedColumnRefNode) *ColumnRefNode {
	return &ColumnRefNode{node: n}
}

func newConstantNode(n googlesql.ResolvedConstantNode) *ConstantNode {
	return &ConstantNode{node: n}
}

func newSystemVariableNode(n googlesql.ResolvedSystemVariableNode) *SystemVariableNode {
	return &SystemVariableNode{node: n}
}

func newInlineLambdaNode(n googlesql.ResolvedInlineLambdaNode) *InlineLambdaNode {
	return &InlineLambdaNode{node: n}
}

func newFilterFieldArgNode(n googlesql.ResolvedFilterFieldArgNode) *FilterFieldArgNode {
	return &FilterFieldArgNode{node: n}
}

func newFilterFieldNode(n googlesql.ResolvedFilterFieldNode) *FilterFieldNode {
	return &FilterFieldNode{node: n}
}

func newFunctionCallNode(n googlesql.ResolvedFunctionCallNode) *FunctionCallNode {
	return &FunctionCallNode{node: n}
}

func newAggregateFunctionCallNode(n googlesql.ResolvedAggregateFunctionCallNode) *AggregateFunctionCallNode {
	return &AggregateFunctionCallNode{node: n}
}

func newAnalyticFunctionCallNode(n googlesql.ResolvedAnalyticFunctionCallNode) *AnalyticFunctionCallNode {
	return &AnalyticFunctionCallNode{node: n}
}

func newExtendedCastElementNode(n googlesql.ResolvedExtendedCastElementNode) *ExtendedCastElementNode {
	return &ExtendedCastElementNode{node: n}
}

func newExtendedCastNode(n googlesql.ResolvedExtendedCastNode) *ExtendedCastNode {
	return &ExtendedCastNode{node: n}
}

func newCastNode(n googlesql.ResolvedCastNode) *CastNode {
	return &CastNode{node: n}
}

func newMakeStructNode(n googlesql.ResolvedMakeStructNode) *MakeStructNode {
	return &MakeStructNode{node: n}
}

func newMakeProtoNode(n googlesql.ResolvedMakeProtoNode) *MakeProtoNode {
	return &MakeProtoNode{node: n}
}

func newMakeProtoFieldNode(n googlesql.ResolvedMakeProtoFieldNode) *MakeProtoFieldNode {
	return &MakeProtoFieldNode{node: n}
}

func newGetStructFieldNode(n googlesql.ResolvedGetStructFieldNode) *GetStructFieldNode {
	return &GetStructFieldNode{node: n}
}

func newGetProtoFieldNode(n googlesql.ResolvedGetProtoFieldNode) *GetProtoFieldNode {
	return &GetProtoFieldNode{node: n}
}

func newGetJsonFieldNode(n googlesql.ResolvedGetJsonFieldNode) *GetJsonFieldNode {
	return &GetJsonFieldNode{node: n}
}

func newFlattenNode(n googlesql.ResolvedFlattenNode) *FlattenNode {
	return &FlattenNode{node: n}
}

func newFlattenedArgNode(n googlesql.ResolvedFlattenedArgNode) *FlattenedArgNode {
	return &FlattenedArgNode{node: n}
}

func newReplaceFieldItemNode(n googlesql.ResolvedReplaceFieldItemNode) *ReplaceFieldItemNode {
	return &ReplaceFieldItemNode{node: n}
}

func newReplaceFieldNode(n googlesql.ResolvedReplaceFieldNode) *ReplaceFieldNode {
	return &ReplaceFieldNode{node: n}
}

func newSubqueryExprNode(n googlesql.ResolvedSubqueryExprNode) *SubqueryExprNode {
	return &SubqueryExprNode{node: n}
}

func newLetExprNode(n *ResolvedLetExprNode) *LetExprNode {
	return &LetExprNode{node: n}
}

func newModelNode(n googlesql.ResolvedModelNode) *ModelNode {
	return &ModelNode{node: n}
}

func newConnectionNode(n googlesql.ResolvedConnectionNode) *ConnectionNode {
	return &ConnectionNode{node: n}
}

func newDescriptorNode(n googlesql.ResolvedDescriptorNode) *DescriptorNode {
	return &DescriptorNode{node: n}
}

func newSingleRowScanNode(n googlesql.ResolvedSingleRowScanNode) *SingleRowScanNode {
	return &SingleRowScanNode{node: n}
}

func newTableScanNode(n googlesql.ResolvedTableScanNode) *TableScanNode {
	return &TableScanNode{node: n}
}

func newJoinScanNode(n googlesql.ResolvedJoinScanNode) *JoinScanNode {
	return &JoinScanNode{node: n}
}

func newArrayScanNode(n googlesql.ResolvedArrayScanNode) *ArrayScanNode {
	return &ArrayScanNode{node: n}
}

func newColumnHolderNode(n googlesql.ResolvedColumnHolderNode) *ColumnHolderNode {
	return &ColumnHolderNode{node: n}
}

func newFilterScanNode(n googlesql.ResolvedFilterScanNode) *FilterScanNode {
	return &FilterScanNode{node: n}
}

func newGroupingSetNode(n googlesql.ResolvedGroupingSetNode) *GroupingSetNode {
	return &GroupingSetNode{node: n}
}

func newAggregateScanNode(n googlesql.ResolvedAggregateScanNode) *AggregateScanNode {
	return &AggregateScanNode{node: n}
}

func newAnonymizedAggregateScanNode(n googlesql.ResolvedAnonymizedAggregateScanNode) *AnonymizedAggregateScanNode {
	return &AnonymizedAggregateScanNode{node: n}
}

func newSetOperationItemNode(n googlesql.ResolvedSetOperationItemNode) *SetOperationItemNode {
	return &SetOperationItemNode{node: n}
}

func newSetOperationScanNode(n googlesql.ResolvedSetOperationScanNode) *SetOperationScanNode {
	return &SetOperationScanNode{node: n}
}

func newOrderByScanNode(n googlesql.ResolvedOrderByScanNode) *OrderByScanNode {
	return &OrderByScanNode{node: n}
}

func newLimitOffsetScanNode(n googlesql.ResolvedLimitOffsetScanNode) *LimitOffsetScanNode {
	return &LimitOffsetScanNode{node: n}
}

func newWithRefScanNode(n googlesql.ResolvedWithRefScanNode) *WithRefScanNode {
	return &WithRefScanNode{node: n}
}

func newAnalyticScanNode(n googlesql.ResolvedAnalyticScanNode) *AnalyticScanNode {
	return &AnalyticScanNode{node: n}
}

func newSampleScanNode(n googlesql.ResolvedSampleScanNode) *SampleScanNode {
	return &SampleScanNode{node: n}
}

func newComputedColumnNode(n googlesql.ResolvedComputedColumnNode) *ComputedColumnNode {
	return &ComputedColumnNode{node: n}
}

func newOrderByItemNode(n googlesql.ResolvedOrderByItemNode) *OrderByItemNode {
	return &OrderByItemNode{node: n}
}

func newColumnAnnotationsNode(n googlesql.ResolvedColumnAnnotationsNode) *ColumnAnnotationsNode {
	return &ColumnAnnotationsNode{node: n}
}

func newGeneratedColumnInfoNode(n googlesql.ResolvedGeneratedColumnInfoNode) *GeneratedColumnInfoNode {
	return &GeneratedColumnInfoNode{node: n}
}

func newColumnDefaultValueNode(n googlesql.ResolvedColumnDefaultValueNode) *ColumnDefaultValueNode {
	return &ColumnDefaultValueNode{node: n}
}

func newColumnDefinitionNode(n googlesql.ResolvedColumnDefinitionNode) *ColumnDefinitionNode {
	return &ColumnDefinitionNode{node: n}
}

func newPrimaryKeyNode(n googlesql.ResolvedPrimaryKeyNode) *PrimaryKeyNode {
	return &PrimaryKeyNode{node: n}
}

func newForeignKeyNode(n googlesql.ResolvedForeignKeyNode) *ForeignKeyNode {
	return &ForeignKeyNode{node: n}
}

func newCheckConstraintNode(n googlesql.ResolvedCheckConstraintNode) *CheckConstraintNode {
	return &CheckConstraintNode{node: n}
}

func newOutputColumnNode(n googlesql.ResolvedOutputColumnNode) *OutputColumnNode {
	return &OutputColumnNode{node: n}
}

func newProjectScanNode(n googlesql.ResolvedProjectScanNode) *ProjectScanNode {
	return &ProjectScanNode{node: n}
}

func newTVFScanNode(n googlesql.ResolvedTVFScanNode) *TVFScanNode {
	return &TVFScanNode{node: n}
}

func newGroupRowsScanNode(n googlesql.ResolvedGroupRowsScanNode) *GroupRowsScanNode {
	return &GroupRowsScanNode{node: n}
}

func newFunctionArgumentNode(n googlesql.ResolvedFunctionArgumentNode) *FunctionArgumentNode {
	return &FunctionArgumentNode{node: n}
}

func newExplainStmtNode(n googlesql.ResolvedExplainStmtNode) *ExplainStmtNode {
	return &ExplainStmtNode{node: n}
}

func newQueryStmtNode(n googlesql.ResolvedQueryStmtNode) *QueryStmtNode {
	return &QueryStmtNode{node: n}
}

func newCreateDatabaseStmtNode(n googlesql.ResolvedCreateDatabaseStmtNode) *CreateDatabaseStmtNode {
	return &CreateDatabaseStmtNode{node: n}
}

func newIndexItemNode(n googlesql.ResolvedIndexItemNode) *IndexItemNode {
	return &IndexItemNode{node: n}
}

func newUnnestItemNode(n googlesql.ResolvedUnnestItemNode) *UnnestItemNode {
	return &UnnestItemNode{node: n}
}

func newCreateIndexStmtNode(n googlesql.ResolvedCreateIndexStmtNode) *CreateIndexStmtNode {
	return &CreateIndexStmtNode{node: n}
}

func newCreateSchemaStmtNode(n googlesql.ResolvedCreateSchemaStmtNode) *CreateSchemaStmtNode {
	return &CreateSchemaStmtNode{node: n}
}

func newCreateTableStmtNode(n googlesql.ResolvedCreateTableStmtNode) *CreateTableStmtNode {
	return &CreateTableStmtNode{node: n}
}

func newCreateTableAsSelectStmtNode(n googlesql.ResolvedCreateTableAsSelectStmtNode) *CreateTableAsSelectStmtNode {
	return &CreateTableAsSelectStmtNode{node: n}
}

func newCreateModelStmtNode(n googlesql.ResolvedCreateModelStmtNode) *CreateModelStmtNode {
	return &CreateModelStmtNode{node: n}
}

func newCreateViewStmtNode(n googlesql.ResolvedCreateViewStmtNode) *CreateViewStmtNode {
	return &CreateViewStmtNode{node: n}
}

func newWithPartitionColumnsNode(n googlesql.ResolvedWithPartitionColumnsNode) *WithPartitionColumnsNode {
	return &WithPartitionColumnsNode{node: n}
}

func newCreateSnapshotTableStmtNode(n googlesql.ResolvedCreateSnapshotTableStmtNode) *CreateSnapshotTableStmtNode {
	return &CreateSnapshotTableStmtNode{node: n}
}

func newCreateExternalTableStmtNode(n googlesql.ResolvedCreateExternalTableStmtNode) *CreateExternalTableStmtNode {
	return &CreateExternalTableStmtNode{node: n}
}

func newExportModelStmtNode(n googlesql.ResolvedExportModelStmtNode) *ExportModelStmtNode {
	return &ExportModelStmtNode{node: n}
}

func newExportDataStmtNode(n googlesql.ResolvedExportDataStmtNode) *ExportDataStmtNode {
	return &ExportDataStmtNode{node: n}
}

func newDefineTableStmtNode(n googlesql.ResolvedDefineTableStmtNode) *DefineTableStmtNode {
	return &DefineTableStmtNode{node: n}
}

func newDescribeStmtNode(n googlesql.ResolvedDescribeStmtNode) *DescribeStmtNode {
	return &DescribeStmtNode{node: n}
}

func newShowStmtNode(n googlesql.ResolvedShowStmtNode) *ShowStmtNode {
	return &ShowStmtNode{node: n}
}

func newBeginStmtNode(n googlesql.ResolvedBeginStmtNode) *BeginStmtNode {
	return &BeginStmtNode{node: n}
}

func newSetTransactionStmtNode(n googlesql.ResolvedSetTransactionStmtNode) *SetTransactionStmtNode {
	return &SetTransactionStmtNode{node: n}
}

func newCommitStmtNode(n googlesql.ResolvedCommitStmtNode) *CommitStmtNode {
	return &CommitStmtNode{node: n}
}

func newRollbackStmtNode(n googlesql.ResolvedRollbackStmtNode) *RollbackStmtNode {
	return &RollbackStmtNode{node: n}
}

func newStartBatchStmtNode(n googlesql.ResolvedStartBatchStmtNode) *StartBatchStmtNode {
	return &StartBatchStmtNode{node: n}
}

func newRunBatchStmtNode(n googlesql.ResolvedRunBatchStmtNode) *RunBatchStmtNode {
	return &RunBatchStmtNode{node: n}
}

func newAbortBatchStmtNode(n googlesql.ResolvedAbortBatchStmtNode) *AbortBatchStmtNode {
	return &AbortBatchStmtNode{node: n}
}

func newDropStmtNode(n googlesql.ResolvedDropStmtNode) *DropStmtNode {
	return &DropStmtNode{node: n}
}

func newDropMaterializedViewStmtNode(n googlesql.ResolvedDropMaterializedViewStmtNode) *DropMaterializedViewStmtNode {
	return &DropMaterializedViewStmtNode{node: n}
}

func newDropSnapshotTableStmtNode(n googlesql.ResolvedDropSnapshotTableStmtNode) *DropSnapshotTableStmtNode {
	return &DropSnapshotTableStmtNode{node: n}
}

func newRecursiveRefScanNode(n googlesql.ResolvedRecursiveRefScanNode) *RecursiveRefScanNode {
	return &RecursiveRefScanNode{node: n}
}

func newRecursiveScanNode(n googlesql.ResolvedRecursiveScanNode) *RecursiveScanNode {
	return &RecursiveScanNode{node: n}
}

func newWithScanNode(n googlesql.ResolvedWithScanNode) *WithScanNode {
	return &WithScanNode{node: n}
}

func newWithEntryNode(n googlesql.ResolvedWithEntryNode) *WithEntryNode {
	return &WithEntryNode{node: n}
}

func newOptionNode(n googlesql.ResolvedOptionNode) *OptionNode {
	return &OptionNode{node: n}
}

func newWindowPartitioningNode(n googlesql.ResolvedWindowPartitioningNode) *WindowPartitioningNode {
	return &WindowPartitioningNode{node: n}
}

func newWindowOrderingNode(n googlesql.ResolvedWindowOrderingNode) *WindowOrderingNode {
	return &WindowOrderingNode{node: n}
}

func newWindowFrameNode(n googlesql.ResolvedWindowFrameNode) *WindowFrameNode {
	return &WindowFrameNode{node: n}
}

func newAnalyticFunctionGroupNode(n googlesql.ResolvedAnalyticFunctionGroupNode) *AnalyticFunctionGroupNode {
	return &AnalyticFunctionGroupNode{node: n}
}

func newWindowFrameExprNode(n googlesql.ResolvedWindowFrameExprNode) *WindowFrameExprNode {
	return &WindowFrameExprNode{node: n}
}

func newDMLValueNode(n googlesql.ResolvedDMLValueNode) *DMLValueNode {
	return &DMLValueNode{node: n}
}

func newDMLDefaultNode(n googlesql.ResolvedDMLDefaultNode) *DMLDefaultNode {
	return &DMLDefaultNode{node: n}
}

func newAssertStmtNode(n googlesql.ResolvedAssertStmtNode) *AssertStmtNode {
	return &AssertStmtNode{node: n}
}

func newAssertRowsModifiedNode(n googlesql.ResolvedAssertRowsModifiedNode) *AssertRowsModifiedNode {
	return &AssertRowsModifiedNode{node: n}
}

func newInsertRowNode(n googlesql.ResolvedInsertRowNode) *InsertRowNode {
	return &InsertRowNode{node: n}
}

func newInsertStmtNode(n googlesql.ResolvedInsertStmtNode) *InsertStmtNode {
	return &InsertStmtNode{node: n}
}

func newDeleteStmtNode(n googlesql.ResolvedDeleteStmtNode) *DeleteStmtNode {
	return &DeleteStmtNode{node: n}
}

func newUpdateItemNode(n googlesql.ResolvedUpdateItemNode) *UpdateItemNode {
	return &UpdateItemNode{node: n}
}

func newUpdateArrayItemNode(n *ResolvedUpdateArrayItemNode) *UpdateArrayItemNode {
	return &UpdateArrayItemNode{node: n}
}

func newUpdateStmtNode(n googlesql.ResolvedUpdateStmtNode) *UpdateStmtNode {
	return &UpdateStmtNode{node: n}
}

func newMergeWhenNode(n googlesql.ResolvedMergeWhenNode) *MergeWhenNode {
	return &MergeWhenNode{node: n}
}

func newMergeStmtNode(n googlesql.ResolvedMergeStmtNode) *MergeStmtNode {
	return &MergeStmtNode{node: n}
}

func newTruncateStmtNode(n googlesql.ResolvedTruncateStmtNode) *TruncateStmtNode {
	return &TruncateStmtNode{node: n}
}

func newObjectUnitNode(n googlesql.ResolvedObjectUnitNode) *ObjectUnitNode {
	return &ObjectUnitNode{node: n}
}

func newPrivilegeNode(n googlesql.ResolvedPrivilegeNode) *PrivilegeNode {
	return &PrivilegeNode{node: n}
}

func newGrantStmtNode(n googlesql.ResolvedGrantStmtNode) *GrantStmtNode {
	return &GrantStmtNode{node: n}
}

func newRevokeStmtNode(n googlesql.ResolvedRevokeStmtNode) *RevokeStmtNode {
	return &RevokeStmtNode{node: n}
}

func newAlterDatabaseStmtNode(n googlesql.ResolvedAlterDatabaseStmtNode) *AlterDatabaseStmtNode {
	return &AlterDatabaseStmtNode{node: n}
}

func newAlterMaterializedViewStmtNode(n googlesql.ResolvedAlterMaterializedViewStmtNode) *AlterMaterializedViewStmtNode {
	return &AlterMaterializedViewStmtNode{node: n}
}

func newAlterSchemaStmtNode(n googlesql.ResolvedAlterSchemaStmtNode) *AlterSchemaStmtNode {
	return &AlterSchemaStmtNode{node: n}
}

func newAlterTableStmtNode(n googlesql.ResolvedAlterTableStmtNode) *AlterTableStmtNode {
	return &AlterTableStmtNode{node: n}
}

func newAlterViewStmtNode(n googlesql.ResolvedAlterViewStmtNode) *AlterViewStmtNode {
	return &AlterViewStmtNode{node: n}
}

func newSetOptionsActionNode(n googlesql.ResolvedSetOptionsActionNode) *SetOptionsActionNode {
	return &SetOptionsActionNode{node: n}
}

func newAddColumnActionNode(n googlesql.ResolvedAddColumnActionNode) *AddColumnActionNode {
	return &AddColumnActionNode{node: n}
}

func newAddConstraintActionNode(n googlesql.ResolvedAddConstraintActionNode) *AddConstraintActionNode {
	return &AddConstraintActionNode{node: n}
}

func newDropConstraintActionNode(n googlesql.ResolvedDropConstraintActionNode) *DropConstraintActionNode {
	return &DropConstraintActionNode{node: n}
}

func newDropPrimaryKeyActionNode(n googlesql.ResolvedDropPrimaryKeyActionNode) *DropPrimaryKeyActionNode {
	return &DropPrimaryKeyActionNode{node: n}
}

func newAlterColumnOptionsActionNode(n googlesql.ResolvedAlterColumnOptionsActionNode) *AlterColumnOptionsActionNode {
	return &AlterColumnOptionsActionNode{node: n}
}

func newAlterColumnDropNotNullActionNode(n googlesql.ResolvedAlterColumnDropNotNullActionNode) *AlterColumnDropNotNullActionNode {
	return &AlterColumnDropNotNullActionNode{node: n}
}

func newAlterColumnSetDataTypeActionNode(n googlesql.ResolvedAlterColumnSetDataTypeActionNode) *AlterColumnSetDataTypeActionNode {
	return &AlterColumnSetDataTypeActionNode{node: n}
}

func newAlterColumnSetDefaultActionNode(n googlesql.ResolvedAlterColumnSetDefaultActionNode) *AlterColumnSetDefaultActionNode {
	return &AlterColumnSetDefaultActionNode{node: n}
}

func newAlterColumnDropDefaultActionNode(n googlesql.ResolvedAlterColumnDropDefaultActionNode) *AlterColumnDropDefaultActionNode {
	return &AlterColumnDropDefaultActionNode{node: n}
}

func newDropColumnActionNode(n googlesql.ResolvedDropColumnActionNode) *DropColumnActionNode {
	return &DropColumnActionNode{node: n}
}

func newRenameColumnActionNode(n googlesql.ResolvedRenameColumnActionNode) *RenameColumnActionNode {
	return &RenameColumnActionNode{node: n}
}

func newSetAsActionNode(n googlesql.ResolvedSetAsActionNode) *SetAsActionNode {
	return &SetAsActionNode{node: n}
}

func newSetCollateClauseNode(n googlesql.ResolvedSetCollateClauseNode) *SetCollateClauseNode {
	return &SetCollateClauseNode{node: n}
}

func newAlterTableSetOptionsStmtNode(n googlesql.ResolvedAlterTableSetOptionsStmtNode) *AlterTableSetOptionsStmtNode {
	return &AlterTableSetOptionsStmtNode{node: n}
}

func newRenameStmtNode(n googlesql.ResolvedRenameStmtNode) *RenameStmtNode {
	return &RenameStmtNode{node: n}
}

func newCreatePrivilegeRestrictionStmtNode(n googlesql.ResolvedCreatePrivilegeRestrictionStmtNode) *CreatePrivilegeRestrictionStmtNode {
	return &CreatePrivilegeRestrictionStmtNode{node: n}
}

func newCreateRowAccessPolicyStmtNode(n googlesql.ResolvedCreateRowAccessPolicyStmtNode) *CreateRowAccessPolicyStmtNode {
	return &CreateRowAccessPolicyStmtNode{node: n}
}

func newDropPrivilegeRestrictionStmtNode(n googlesql.ResolvedDropPrivilegeRestrictionStmtNode) *DropPrivilegeRestrictionStmtNode {
	return &DropPrivilegeRestrictionStmtNode{node: n}
}

func newDropRowAccessPolicyStmtNode(n googlesql.ResolvedDropRowAccessPolicyStmtNode) *DropRowAccessPolicyStmtNode {
	return &DropRowAccessPolicyStmtNode{node: n}
}

func newDropSearchIndexStmtNode(n *ResolvedDropSearchIndexStmtNode) *DropSearchIndexStmtNode {
	return &DropSearchIndexStmtNode{node: n}
}

func newGrantToActionNode(n googlesql.ResolvedGrantToActionNode) *GrantToActionNode {
	return &GrantToActionNode{node: n}
}

func newRestrictToActionNode(n googlesql.ResolvedRestrictToActionNode) *RestrictToActionNode {
	return &RestrictToActionNode{node: n}
}

func newAddToRestricteeListActionNode(n googlesql.ResolvedAddToRestricteeListActionNode) *AddToRestricteeListActionNode {
	return &AddToRestricteeListActionNode{node: n}
}

func newRemoveFromRestricteeListActionNode(n googlesql.ResolvedRemoveFromRestricteeListActionNode) *RemoveFromRestricteeListActionNode {
	return &RemoveFromRestricteeListActionNode{node: n}
}

func newFilterUsingActionNode(n googlesql.ResolvedFilterUsingActionNode) *FilterUsingActionNode {
	return &FilterUsingActionNode{node: n}
}

func newRevokeFromActionNode(n googlesql.ResolvedRevokeFromActionNode) *RevokeFromActionNode {
	return &RevokeFromActionNode{node: n}
}

func newRenameToActionNode(n googlesql.ResolvedRenameToActionNode) *RenameToActionNode {
	return &RenameToActionNode{node: n}
}

func newAlterPrivilegeRestrictionStmtNode(n googlesql.ResolvedAlterPrivilegeRestrictionStmtNode) *AlterPrivilegeRestrictionStmtNode {
	return &AlterPrivilegeRestrictionStmtNode{node: n}
}

func newAlterRowAccessPolicyStmtNode(n googlesql.ResolvedAlterRowAccessPolicyStmtNode) *AlterRowAccessPolicyStmtNode {
	return &AlterRowAccessPolicyStmtNode{node: n}
}

func newAlterAllRowAccessPoliciesStmtNode(n googlesql.ResolvedAlterAllRowAccessPoliciesStmtNode) *AlterAllRowAccessPoliciesStmtNode {
	return &AlterAllRowAccessPoliciesStmtNode{node: n}
}

func newCreateConstantStmtNode(n googlesql.ResolvedCreateConstantStmtNode) *CreateConstantStmtNode {
	return &CreateConstantStmtNode{node: n}
}

func newCreateFunctionStmtNode(n googlesql.ResolvedCreateFunctionStmtNode) *CreateFunctionStmtNode {
	return &CreateFunctionStmtNode{node: n}
}

func newArgumentDefNode(n googlesql.ResolvedArgumentDefNode) *ArgumentDefNode {
	return &ArgumentDefNode{node: n}
}

func newArgumentRefNode(n googlesql.ResolvedArgumentRefNode) *ArgumentRefNode {
	return &ArgumentRefNode{node: n}
}

func newCreateTableFunctionStmtNode(n googlesql.ResolvedCreateTableFunctionStmtNode) *CreateTableFunctionStmtNode {
	return &CreateTableFunctionStmtNode{node: n}
}

func newRelationArgumentScanNode(n googlesql.ResolvedRelationArgumentScanNode) *RelationArgumentScanNode {
	return &RelationArgumentScanNode{node: n}
}

func newArgumentListNode(n googlesql.ResolvedArgumentListNode) *ArgumentListNode {
	return &ArgumentListNode{node: n}
}

func newFunctionSignatureHolderNode(n googlesql.ResolvedFunctionSignatureHolderNode) *FunctionSignatureHolderNode {
	return &FunctionSignatureHolderNode{node: n}
}

func newDropFunctionStmtNode(n googlesql.ResolvedDropFunctionStmtNode) *DropFunctionStmtNode {
	return &DropFunctionStmtNode{node: n}
}

func newDropTableFunctionStmtNode(n googlesql.ResolvedDropTableFunctionStmtNode) *DropTableFunctionStmtNode {
	return &DropTableFunctionStmtNode{node: n}
}

func newCallStmtNode(n googlesql.ResolvedCallStmtNode) *CallStmtNode {
	return &CallStmtNode{node: n}
}

func newImportStmtNode(n googlesql.ResolvedImportStmtNode) *ImportStmtNode {
	return &ImportStmtNode{node: n}
}

func newModuleStmtNode(n googlesql.ResolvedModuleStmtNode) *ModuleStmtNode {
	return &ModuleStmtNode{node: n}
}

func newAggregateHavingModifierNode(n googlesql.ResolvedAggregateHavingModifierNode) *AggregateHavingModifierNode {
	return &AggregateHavingModifierNode{node: n}
}

func newCreateMaterializedViewStmtNode(n googlesql.ResolvedCreateMaterializedViewStmtNode) *CreateMaterializedViewStmtNode {
	return &CreateMaterializedViewStmtNode{node: n}
}

func newCreateProcedureStmtNode(n googlesql.ResolvedCreateProcedureStmtNode) *CreateProcedureStmtNode {
	return &CreateProcedureStmtNode{node: n}
}

func newExecuteImmediateArgumentNode(n googlesql.ResolvedExecuteImmediateArgumentNode) *ExecuteImmediateArgumentNode {
	return &ExecuteImmediateArgumentNode{node: n}
}

func newExecuteImmediateStmtNode(n googlesql.ResolvedExecuteImmediateStmtNode) *ExecuteImmediateStmtNode {
	return &ExecuteImmediateStmtNode{node: n}
}

func newAssignmentStmtNode(n googlesql.ResolvedAssignmentStmtNode) *AssignmentStmtNode {
	return &AssignmentStmtNode{node: n}
}

func newCreateEntityStmtNode(n googlesql.ResolvedCreateEntityStmtNode) *CreateEntityStmtNode {
	return &CreateEntityStmtNode{node: n}
}

func newAlterEntityStmtNode(n googlesql.ResolvedAlterEntityStmtNode) *AlterEntityStmtNode {
	return &AlterEntityStmtNode{node: n}
}

func newPivotColumnNode(n googlesql.ResolvedPivotColumnNode) *PivotColumnNode {
	return &PivotColumnNode{node: n}
}

func newPivotScanNode(n googlesql.ResolvedPivotScanNode) *PivotScanNode {
	return &PivotScanNode{node: n}
}

func newReturningClauseNode(n googlesql.ResolvedReturningClauseNode) *ReturningClauseNode {
	return &ReturningClauseNode{node: n}
}

func newUnpivotArgNode(n googlesql.ResolvedUnpivotArgNode) *UnpivotArgNode {
	return &UnpivotArgNode{node: n}
}

func newUnpivotScanNode(n googlesql.ResolvedUnpivotScanNode) *UnpivotScanNode {
	return &UnpivotScanNode{node: n}
}

func newCloneDataStmtNode(n googlesql.ResolvedCloneDataStmtNode) *CloneDataStmtNode {
	return &CloneDataStmtNode{node: n}
}

func newTableAndColumnInfoNode(n googlesql.ResolvedTableAndColumnInfoNode) *TableAndColumnInfoNode {
	return &TableAndColumnInfoNode{node: n}
}

func newAnalyzeStmtNode(n googlesql.ResolvedAnalyzeStmtNode) *AnalyzeStmtNode {
	return &AnalyzeStmtNode{node: n}
}

func newAuxLoadDataStmtNode(n googlesql.ResolvedAuxLoadDataStmtNode) *AuxLoadDataStmtNode {
	return &AuxLoadDataStmtNode{node: n}
}
