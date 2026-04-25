package internal

import (
	googlesql "github.com/goccy/go-googlesql"
)

func newNode(node googlesql.ResolvedNode) Formatter {
	if node == nil {
		return nil
	}
	switch m1(node.NodeKind()) {
	case googlesql.ResolvedNodeKindResolvedLiteral:
		return newLiteralNode(node.(*googlesql.ResolvedLiteral))
	case googlesql.ResolvedNodeKindResolvedParameter:
		return newParameterNode(node.(*googlesql.ResolvedParameter))
	case googlesql.ResolvedNodeKindResolvedExpressionColumn:
		return newExpressionColumnNode(node.(*googlesql.ResolvedExpressionColumn))
	case googlesql.ResolvedNodeKindResolvedColumnRef:
		return newColumnRefNode(node.(*googlesql.ResolvedColumnRef))
	case googlesql.ResolvedNodeKindResolvedConstant:
		return newConstantNode(node.(*googlesql.ResolvedConstant))
	case googlesql.ResolvedNodeKindResolvedSystemVariable:
		return newSystemVariableNode(node.(*googlesql.ResolvedSystemVariable))
	case googlesql.ResolvedNodeKindResolvedInlineLambda:
		return newInlineLambdaNode(node.(*googlesql.ResolvedInlineLambda))
	case googlesql.ResolvedNodeKindResolvedFilterFieldArg:
		return newFilterFieldArgNode(node.(*googlesql.ResolvedFilterFieldArg))
	case googlesql.ResolvedNodeKindResolvedFilterField:
		return newFilterFieldNode(node.(*googlesql.ResolvedFilterField))
	case googlesql.ResolvedNodeKindResolvedFunctionCall:
		return newFunctionCallNode(node.(*googlesql.ResolvedFunctionCall))
	case googlesql.ResolvedNodeKindResolvedAggregateFunctionCall:
		return newAggregateFunctionCallNode(node.(*googlesql.ResolvedAggregateFunctionCall))
	case googlesql.ResolvedNodeKindResolvedAnalyticFunctionCall:
		return newAnalyticFunctionCallNode(node.(*googlesql.ResolvedAnalyticFunctionCall))
	case googlesql.ResolvedNodeKindResolvedExtendedCastElement:
		return newExtendedCastElementNode(node.(*googlesql.ResolvedExtendedCastElement))
	case googlesql.ResolvedNodeKindResolvedExtendedCast:
		return newExtendedCastNode(node.(*googlesql.ResolvedExtendedCast))
	case googlesql.ResolvedNodeKindResolvedCast:
		return newCastNode(node.(*googlesql.ResolvedCast))
	case googlesql.ResolvedNodeKindResolvedMakeStruct:
		return newMakeStructNode(node.(*googlesql.ResolvedMakeStruct))
	case googlesql.ResolvedNodeKindResolvedMakeProto:
		return newMakeProtoNode(node.(*googlesql.ResolvedMakeProto))
	case googlesql.ResolvedNodeKindResolvedMakeProtoField:
		return newMakeProtoFieldNode(node.(*googlesql.ResolvedMakeProtoField))
	case googlesql.ResolvedNodeKindResolvedGetStructField:
		return newGetStructFieldNode(node.(*googlesql.ResolvedGetStructField))
	case googlesql.ResolvedNodeKindResolvedGetProtoField:
		return newGetProtoFieldNode(node.(*googlesql.ResolvedGetProtoField))
	case googlesql.ResolvedNodeKindResolvedGetJsonField:
		return newGetJsonFieldNode(node.(*googlesql.ResolvedGetJsonField))
	case googlesql.ResolvedNodeKindResolvedFlatten:
		return newFlattenNode(node.(*googlesql.ResolvedFlatten))
	case googlesql.ResolvedNodeKindResolvedFlattenedArg:
		return newFlattenedArgNode(node.(*googlesql.ResolvedFlattenedArg))
	case googlesql.ResolvedNodeKindResolvedReplaceFieldItem:
		return newReplaceFieldItemNode(node.(*googlesql.ResolvedReplaceFieldItem))
	case googlesql.ResolvedNodeKindResolvedReplaceField:
		return newReplaceFieldNode(node.(*googlesql.ResolvedReplaceField))
	case googlesql.ResolvedNodeKindResolvedSubqueryExpr:
		return newSubqueryExprNode(node.(*googlesql.ResolvedSubqueryExpr))
	// ResolvedLetExpr isn't in the current googlesql export set; the
	// case used to dispatch to newLetExprNode. Reinstate when the
	// corresponding node kind re-enters the bridge.
	case googlesql.ResolvedNodeKindResolvedModel:
		return newModelNode(node.(*googlesql.ResolvedModel))
	case googlesql.ResolvedNodeKindResolvedConnection:
		return newConnectionNode(node.(*googlesql.ResolvedConnection))
	case googlesql.ResolvedNodeKindResolvedDescriptor:
		return newDescriptorNode(node.(*googlesql.ResolvedDescriptor))
	case googlesql.ResolvedNodeKindResolvedSingleRowScan:
		return newSingleRowScanNode(node.(*googlesql.ResolvedSingleRowScan))
	case googlesql.ResolvedNodeKindResolvedTableScan:
		return newTableScanNode(node.(*googlesql.ResolvedTableScan))
	case googlesql.ResolvedNodeKindResolvedJoinScan:
		return newJoinScanNode(node.(*googlesql.ResolvedJoinScan))
	case googlesql.ResolvedNodeKindResolvedArrayScan:
		return newArrayScanNode(node.(*googlesql.ResolvedArrayScan))
	case googlesql.ResolvedNodeKindResolvedColumnHolder:
		return newColumnHolderNode(node.(*googlesql.ResolvedColumnHolder))
	case googlesql.ResolvedNodeKindResolvedFilterScan:
		return newFilterScanNode(node.(*googlesql.ResolvedFilterScan))
	case googlesql.ResolvedNodeKindResolvedGroupingSet:
		return newGroupingSetNode(node.(*googlesql.ResolvedGroupingSet))
	case googlesql.ResolvedNodeKindResolvedAggregateScan:
		return newAggregateScanNode(node.(*googlesql.ResolvedAggregateScan))
	case googlesql.ResolvedNodeKindResolvedAnonymizedAggregateScan:
		return newAnonymizedAggregateScanNode(node.(*googlesql.ResolvedAnonymizedAggregateScan))
	case googlesql.ResolvedNodeKindResolvedSetOperationItem:
		return newSetOperationItemNode(node.(*googlesql.ResolvedSetOperationItem))
	case googlesql.ResolvedNodeKindResolvedSetOperationScan:
		return newSetOperationScanNode(node.(*googlesql.ResolvedSetOperationScan))
	case googlesql.ResolvedNodeKindResolvedOrderByScan:
		return newOrderByScanNode(node.(*googlesql.ResolvedOrderByScan))
	case googlesql.ResolvedNodeKindResolvedLimitOffsetScan:
		return newLimitOffsetScanNode(node.(*googlesql.ResolvedLimitOffsetScan))
	case googlesql.ResolvedNodeKindResolvedWithRefScan:
		return newWithRefScanNode(node.(*googlesql.ResolvedWithRefScan))
	case googlesql.ResolvedNodeKindResolvedAnalyticScan:
		return newAnalyticScanNode(node.(*googlesql.ResolvedAnalyticScan))
	case googlesql.ResolvedNodeKindResolvedSampleScan:
		return newSampleScanNode(node.(*googlesql.ResolvedSampleScan))
	case googlesql.ResolvedNodeKindResolvedComputedColumn:
		return newComputedColumnNode(node.(*googlesql.ResolvedComputedColumn))
	case googlesql.ResolvedNodeKindResolvedOrderByItem:
		return newOrderByItemNode(node.(*googlesql.ResolvedOrderByItem))
	case googlesql.ResolvedNodeKindResolvedColumnAnnotations:
		return newColumnAnnotationsNode(node.(*googlesql.ResolvedColumnAnnotations))
	case googlesql.ResolvedNodeKindResolvedGeneratedColumnInfo:
		return newGeneratedColumnInfoNode(node.(*googlesql.ResolvedGeneratedColumnInfo))
	case googlesql.ResolvedNodeKindResolvedColumnDefaultValue:
		return newColumnDefaultValueNode(node.(*googlesql.ResolvedColumnDefaultValue))
	case googlesql.ResolvedNodeKindResolvedColumnDefinition:
		return newColumnDefinitionNode(node.(*googlesql.ResolvedColumnDefinition))
	case googlesql.ResolvedNodeKindResolvedPrimaryKey:
		return newPrimaryKeyNode(node.(*googlesql.ResolvedPrimaryKey))
	case googlesql.ResolvedNodeKindResolvedForeignKey:
		return newForeignKeyNode(node.(*googlesql.ResolvedForeignKey))
	case googlesql.ResolvedNodeKindResolvedCheckConstraint:
		return newCheckConstraintNode(node.(*googlesql.ResolvedCheckConstraint))
	case googlesql.ResolvedNodeKindResolvedOutputColumn:
		return newOutputColumnNode(node.(*googlesql.ResolvedOutputColumn))
	case googlesql.ResolvedNodeKindResolvedProjectScan:
		return newProjectScanNode(node.(*googlesql.ResolvedProjectScan))
	case ResolvedNodeKindResolvedTVFScan:
		return newTVFScanNode(node.(*googlesql.ResolvedTVFScan))
	case googlesql.ResolvedNodeKindResolvedGroupRowsScan:
		return newGroupRowsScanNode(node.(*googlesql.ResolvedGroupRowsScan))
	case googlesql.ResolvedNodeKindResolvedFunctionArgument:
		return newFunctionArgumentNode(node.(*googlesql.ResolvedFunctionArgument))
	case googlesql.ResolvedNodeKindResolvedExplainStmt:
		return newExplainStmtNode(node.(*googlesql.ResolvedExplainStmt))
	case googlesql.ResolvedNodeKindResolvedQueryStmt:
		return newQueryStmtNode(node.(*googlesql.ResolvedQueryStmt))
	case googlesql.ResolvedNodeKindResolvedCreateDatabaseStmt:
		return newCreateDatabaseStmtNode(node.(*googlesql.ResolvedCreateDatabaseStmt))
	case googlesql.ResolvedNodeKindResolvedIndexItem:
		return newIndexItemNode(node.(*googlesql.ResolvedIndexItem))
	case googlesql.ResolvedNodeKindResolvedUnnestItem:
		return newUnnestItemNode(node.(*googlesql.ResolvedUnnestItem))
	case googlesql.ResolvedNodeKindResolvedCreateIndexStmt:
		return newCreateIndexStmtNode(node.(*googlesql.ResolvedCreateIndexStmt))
	case googlesql.ResolvedNodeKindResolvedCreateSchemaStmt:
		return newCreateSchemaStmtNode(node.(*googlesql.ResolvedCreateSchemaStmt))
	case googlesql.ResolvedNodeKindResolvedCreateTableStmt:
		return newCreateTableStmtNode(node.(*googlesql.ResolvedCreateTableStmt))
	case googlesql.ResolvedNodeKindResolvedCreateTableAsSelectStmt:
		return newCreateTableAsSelectStmtNode(node.(*googlesql.ResolvedCreateTableAsSelectStmt))
	case googlesql.ResolvedNodeKindResolvedCreateModelStmt:
		return newCreateModelStmtNode(node.(*googlesql.ResolvedCreateModelStmt))
	case googlesql.ResolvedNodeKindResolvedCreateViewStmt:
		return newCreateViewStmtNode(node.(*googlesql.ResolvedCreateViewStmt))
	case googlesql.ResolvedNodeKindResolvedWithPartitionColumns:
		return newWithPartitionColumnsNode(node.(*googlesql.ResolvedWithPartitionColumns))
	case googlesql.ResolvedNodeKindResolvedCreateSnapshotTableStmt:
		return newCreateSnapshotTableStmtNode(node.(*googlesql.ResolvedCreateSnapshotTableStmt))
	case googlesql.ResolvedNodeKindResolvedCreateExternalTableStmt:
		return newCreateExternalTableStmtNode(node.(*googlesql.ResolvedCreateExternalTableStmt))
	case googlesql.ResolvedNodeKindResolvedExportModelStmt:
		return newExportModelStmtNode(node.(*googlesql.ResolvedExportModelStmt))
	case googlesql.ResolvedNodeKindResolvedExportDataStmt:
		return newExportDataStmtNode(node.(*googlesql.ResolvedExportDataStmt))
	case googlesql.ResolvedNodeKindResolvedDefineTableStmt:
		return newDefineTableStmtNode(node.(*googlesql.ResolvedDefineTableStmt))
	case googlesql.ResolvedNodeKindResolvedDescribeStmt:
		return newDescribeStmtNode(node.(*googlesql.ResolvedDescribeStmt))
	case googlesql.ResolvedNodeKindResolvedShowStmt:
		return newShowStmtNode(node.(*googlesql.ResolvedShowStmt))
	case googlesql.ResolvedNodeKindResolvedBeginStmt:
		return newBeginStmtNode(node.(*googlesql.ResolvedBeginStmt))
	case googlesql.ResolvedNodeKindResolvedSetTransactionStmt:
		return newSetTransactionStmtNode(node.(*googlesql.ResolvedSetTransactionStmt))
	case googlesql.ResolvedNodeKindResolvedCommitStmt:
		return newCommitStmtNode(node.(*googlesql.ResolvedCommitStmt))
	case googlesql.ResolvedNodeKindResolvedRollbackStmt:
		return newRollbackStmtNode(node.(*googlesql.ResolvedRollbackStmt))
	case googlesql.ResolvedNodeKindResolvedStartBatchStmt:
		return newStartBatchStmtNode(node.(*googlesql.ResolvedStartBatchStmt))
	case googlesql.ResolvedNodeKindResolvedRunBatchStmt:
		return newRunBatchStmtNode(node.(*googlesql.ResolvedRunBatchStmt))
	case googlesql.ResolvedNodeKindResolvedAbortBatchStmt:
		return newAbortBatchStmtNode(node.(*googlesql.ResolvedAbortBatchStmt))
	case googlesql.ResolvedNodeKindResolvedDropStmt:
		return newDropStmtNode(node.(*googlesql.ResolvedDropStmt))
	case googlesql.ResolvedNodeKindResolvedDropMaterializedViewStmt:
		return newDropMaterializedViewStmtNode(node.(*googlesql.ResolvedDropMaterializedViewStmt))
	case googlesql.ResolvedNodeKindResolvedDropSnapshotTableStmt:
		return newDropSnapshotTableStmtNode(node.(*googlesql.ResolvedDropSnapshotTableStmt))
	case googlesql.ResolvedNodeKindResolvedRecursiveRefScan:
		return newRecursiveRefScanNode(node.(*googlesql.ResolvedRecursiveRefScan))
	case googlesql.ResolvedNodeKindResolvedRecursiveScan:
		return newRecursiveScanNode(node.(*googlesql.ResolvedRecursiveScan))
	case googlesql.ResolvedNodeKindResolvedWithScan:
		return newWithScanNode(node.(*googlesql.ResolvedWithScan))
	case googlesql.ResolvedNodeKindResolvedWithEntry:
		return newWithEntryNode(node.(*googlesql.ResolvedWithEntry))
	case googlesql.ResolvedNodeKindResolvedOption:
		return newOptionNode(node.(*googlesql.ResolvedOption))
	case googlesql.ResolvedNodeKindResolvedWindowPartitioning:
		return newWindowPartitioningNode(node.(*googlesql.ResolvedWindowPartitioning))
	case googlesql.ResolvedNodeKindResolvedWindowOrdering:
		return newWindowOrderingNode(node.(*googlesql.ResolvedWindowOrdering))
	case googlesql.ResolvedNodeKindResolvedWindowFrame:
		return newWindowFrameNode(node.(*googlesql.ResolvedWindowFrame))
	case googlesql.ResolvedNodeKindResolvedAnalyticFunctionGroup:
		return newAnalyticFunctionGroupNode(node.(*googlesql.ResolvedAnalyticFunctionGroup))
	case googlesql.ResolvedNodeKindResolvedWindowFrameExpr:
		return newWindowFrameExprNode(node.(*googlesql.ResolvedWindowFrameExpr))
	case ResolvedNodeKindResolvedDMLValue:
		return newDMLValueNode(node.(*googlesql.ResolvedDMLValue))
	case ResolvedNodeKindResolvedDMLDefault:
		return newDMLDefaultNode(node.(*googlesql.ResolvedDMLDefault))
	case googlesql.ResolvedNodeKindResolvedAssertStmt:
		return newAssertStmtNode(node.(*googlesql.ResolvedAssertStmt))
	case googlesql.ResolvedNodeKindResolvedAssertRowsModified:
		return newAssertRowsModifiedNode(node.(*googlesql.ResolvedAssertRowsModified))
	case googlesql.ResolvedNodeKindResolvedInsertRow:
		return newInsertRowNode(node.(*googlesql.ResolvedInsertRow))
	case googlesql.ResolvedNodeKindResolvedInsertStmt:
		return newInsertStmtNode(node.(*googlesql.ResolvedInsertStmt))
	case googlesql.ResolvedNodeKindResolvedDeleteStmt:
		return newDeleteStmtNode(node.(*googlesql.ResolvedDeleteStmt))
	case googlesql.ResolvedNodeKindResolvedUpdateItem:
		return newUpdateItemNode(node.(*googlesql.ResolvedUpdateItem))
	// ResolvedUpdateArrayItem isn't in the current googlesql export set.
	case googlesql.ResolvedNodeKindResolvedUpdateStmt:
		return newUpdateStmtNode(node.(*googlesql.ResolvedUpdateStmt))
	case googlesql.ResolvedNodeKindResolvedMergeWhen:
		return newMergeWhenNode(node.(*googlesql.ResolvedMergeWhen))
	case googlesql.ResolvedNodeKindResolvedMergeStmt:
		return newMergeStmtNode(node.(*googlesql.ResolvedMergeStmt))
	case googlesql.ResolvedNodeKindResolvedTruncateStmt:
		return newTruncateStmtNode(node.(*googlesql.ResolvedTruncateStmt))
	case googlesql.ResolvedNodeKindResolvedObjectUnit:
		return newObjectUnitNode(node.(*googlesql.ResolvedObjectUnit))
	case googlesql.ResolvedNodeKindResolvedPrivilege:
		return newPrivilegeNode(node.(*googlesql.ResolvedPrivilege))
	case googlesql.ResolvedNodeKindResolvedGrantStmt:
		return newGrantStmtNode(node.(*googlesql.ResolvedGrantStmt))
	case googlesql.ResolvedNodeKindResolvedRevokeStmt:
		return newRevokeStmtNode(node.(*googlesql.ResolvedRevokeStmt))
	case googlesql.ResolvedNodeKindResolvedAlterDatabaseStmt:
		return newAlterDatabaseStmtNode(node.(*googlesql.ResolvedAlterDatabaseStmt))
	case googlesql.ResolvedNodeKindResolvedAlterMaterializedViewStmt:
		return newAlterMaterializedViewStmtNode(node.(*googlesql.ResolvedAlterMaterializedViewStmt))
	case googlesql.ResolvedNodeKindResolvedAlterSchemaStmt:
		return newAlterSchemaStmtNode(node.(*googlesql.ResolvedAlterSchemaStmt))
	case googlesql.ResolvedNodeKindResolvedAlterTableStmt:
		return newAlterTableStmtNode(node.(*googlesql.ResolvedAlterTableStmt))
	case googlesql.ResolvedNodeKindResolvedAlterViewStmt:
		return newAlterViewStmtNode(node.(*googlesql.ResolvedAlterViewStmt))
	case googlesql.ResolvedNodeKindResolvedSetOptionsAction:
		return newSetOptionsActionNode(node.(*googlesql.ResolvedSetOptionsAction))
	case googlesql.ResolvedNodeKindResolvedAddColumnAction:
		return newAddColumnActionNode(node.(*googlesql.ResolvedAddColumnAction))
	case googlesql.ResolvedNodeKindResolvedAddConstraintAction:
		return newAddConstraintActionNode(node.(*googlesql.ResolvedAddConstraintAction))
	case googlesql.ResolvedNodeKindResolvedDropConstraintAction:
		return newDropConstraintActionNode(node.(*googlesql.ResolvedDropConstraintAction))
	case googlesql.ResolvedNodeKindResolvedDropPrimaryKeyAction:
		return newDropPrimaryKeyActionNode(node.(*googlesql.ResolvedDropPrimaryKeyAction))
	case googlesql.ResolvedNodeKindResolvedAlterColumnOptionsAction:
		return newAlterColumnOptionsActionNode(node.(*googlesql.ResolvedAlterColumnOptionsAction))
	case googlesql.ResolvedNodeKindResolvedAlterColumnDropNotNullAction:
		return newAlterColumnDropNotNullActionNode(node.(*googlesql.ResolvedAlterColumnDropNotNullAction))
	case googlesql.ResolvedNodeKindResolvedAlterColumnSetDataTypeAction:
		return newAlterColumnSetDataTypeActionNode(node.(*googlesql.ResolvedAlterColumnSetDataTypeAction))
	case googlesql.ResolvedNodeKindResolvedAlterColumnSetDefaultAction:
		return newAlterColumnSetDefaultActionNode(node.(*googlesql.ResolvedAlterColumnSetDefaultAction))
	case googlesql.ResolvedNodeKindResolvedAlterColumnDropDefaultAction:
		return newAlterColumnDropDefaultActionNode(node.(*googlesql.ResolvedAlterColumnDropDefaultAction))
	case googlesql.ResolvedNodeKindResolvedDropColumnAction:
		return newDropColumnActionNode(node.(*googlesql.ResolvedDropColumnAction))
	case googlesql.ResolvedNodeKindResolvedRenameColumnAction:
		return newRenameColumnActionNode(node.(*googlesql.ResolvedRenameColumnAction))
	case googlesql.ResolvedNodeKindResolvedSetAsAction:
		return newSetAsActionNode(node.(*googlesql.ResolvedSetAsAction))
	case googlesql.ResolvedNodeKindResolvedSetCollateClause:
		return newSetCollateClauseNode(node.(*googlesql.ResolvedSetCollateClause))
	case googlesql.ResolvedNodeKindResolvedAlterTableSetOptionsStmt:
		return newAlterTableSetOptionsStmtNode(node.(*googlesql.ResolvedAlterTableSetOptionsStmt))
	case googlesql.ResolvedNodeKindResolvedRenameStmt:
		return newRenameStmtNode(node.(*googlesql.ResolvedRenameStmt))
	case googlesql.ResolvedNodeKindResolvedCreatePrivilegeRestrictionStmt:
		return newCreatePrivilegeRestrictionStmtNode(node.(*googlesql.ResolvedCreatePrivilegeRestrictionStmt))
	case googlesql.ResolvedNodeKindResolvedCreateRowAccessPolicyStmt:
		return newCreateRowAccessPolicyStmtNode(node.(*googlesql.ResolvedCreateRowAccessPolicyStmt))
	case googlesql.ResolvedNodeKindResolvedDropPrivilegeRestrictionStmt:
		return newDropPrivilegeRestrictionStmtNode(node.(*googlesql.ResolvedDropPrivilegeRestrictionStmt))
	case googlesql.ResolvedNodeKindResolvedDropRowAccessPolicyStmt:
		return newDropRowAccessPolicyStmtNode(node.(*googlesql.ResolvedDropRowAccessPolicyStmt))
	// ResolvedDropSearchIndexStmt isn't in the current googlesql export set.
	case googlesql.ResolvedNodeKindResolvedGrantToAction:
		return newGrantToActionNode(node.(*googlesql.ResolvedGrantToAction))
	case googlesql.ResolvedNodeKindResolvedRestrictToAction:
		return newRestrictToActionNode(node.(*googlesql.ResolvedRestrictToAction))
	case googlesql.ResolvedNodeKindResolvedAddToRestricteeListAction:
		return newAddToRestricteeListActionNode(node.(*googlesql.ResolvedAddToRestricteeListAction))
	case googlesql.ResolvedNodeKindResolvedRemoveFromRestricteeListAction:
		return newRemoveFromRestricteeListActionNode(node.(*googlesql.ResolvedRemoveFromRestricteeListAction))
	case googlesql.ResolvedNodeKindResolvedFilterUsingAction:
		return newFilterUsingActionNode(node.(*googlesql.ResolvedFilterUsingAction))
	case googlesql.ResolvedNodeKindResolvedRevokeFromAction:
		return newRevokeFromActionNode(node.(*googlesql.ResolvedRevokeFromAction))
	case googlesql.ResolvedNodeKindResolvedRenameToAction:
		return newRenameToActionNode(node.(*googlesql.ResolvedRenameToAction))
	case googlesql.ResolvedNodeKindResolvedAlterPrivilegeRestrictionStmt:
		return newAlterPrivilegeRestrictionStmtNode(node.(*googlesql.ResolvedAlterPrivilegeRestrictionStmt))
	case googlesql.ResolvedNodeKindResolvedAlterRowAccessPolicyStmt:
		return newAlterRowAccessPolicyStmtNode(node.(*googlesql.ResolvedAlterRowAccessPolicyStmt))
	case googlesql.ResolvedNodeKindResolvedAlterAllRowAccessPoliciesStmt:
		return newAlterAllRowAccessPoliciesStmtNode(node.(*googlesql.ResolvedAlterAllRowAccessPoliciesStmt))
	case googlesql.ResolvedNodeKindResolvedCreateConstantStmt:
		return newCreateConstantStmtNode(node.(*googlesql.ResolvedCreateConstantStmt))
	case googlesql.ResolvedNodeKindResolvedCreateFunctionStmt:
		return newCreateFunctionStmtNode(node.(*googlesql.ResolvedCreateFunctionStmt))
	case googlesql.ResolvedNodeKindResolvedArgumentDef:
		return newArgumentDefNode(node.(*googlesql.ResolvedArgumentDef))
	case googlesql.ResolvedNodeKindResolvedArgumentRef:
		return newArgumentRefNode(node.(*googlesql.ResolvedArgumentRef))
	case googlesql.ResolvedNodeKindResolvedCreateTableFunctionStmt:
		return newCreateTableFunctionStmtNode(node.(*googlesql.ResolvedCreateTableFunctionStmt))
	case googlesql.ResolvedNodeKindResolvedRelationArgumentScan:
		return newRelationArgumentScanNode(node.(*googlesql.ResolvedRelationArgumentScan))
	case googlesql.ResolvedNodeKindResolvedArgumentList:
		return newArgumentListNode(node.(*googlesql.ResolvedArgumentList))
	case googlesql.ResolvedNodeKindResolvedFunctionSignatureHolder:
		return newFunctionSignatureHolderNode(node.(*googlesql.ResolvedFunctionSignatureHolder))
	case googlesql.ResolvedNodeKindResolvedDropFunctionStmt:
		return newDropFunctionStmtNode(node.(*googlesql.ResolvedDropFunctionStmt))
	case googlesql.ResolvedNodeKindResolvedDropTableFunctionStmt:
		return newDropTableFunctionStmtNode(node.(*googlesql.ResolvedDropTableFunctionStmt))
	case googlesql.ResolvedNodeKindResolvedCallStmt:
		return newCallStmtNode(node.(*googlesql.ResolvedCallStmt))
	case googlesql.ResolvedNodeKindResolvedImportStmt:
		return newImportStmtNode(node.(*googlesql.ResolvedImportStmt))
	case googlesql.ResolvedNodeKindResolvedModuleStmt:
		return newModuleStmtNode(node.(*googlesql.ResolvedModuleStmt))
	case googlesql.ResolvedNodeKindResolvedAggregateHavingModifier:
		return newAggregateHavingModifierNode(node.(*googlesql.ResolvedAggregateHavingModifier))
	case googlesql.ResolvedNodeKindResolvedCreateMaterializedViewStmt:
		return newCreateMaterializedViewStmtNode(node.(*googlesql.ResolvedCreateMaterializedViewStmt))
	case googlesql.ResolvedNodeKindResolvedCreateProcedureStmt:
		return newCreateProcedureStmtNode(node.(*googlesql.ResolvedCreateProcedureStmt))
	case googlesql.ResolvedNodeKindResolvedExecuteImmediateArgument:
		return newExecuteImmediateArgumentNode(node.(*googlesql.ResolvedExecuteImmediateArgument))
	case googlesql.ResolvedNodeKindResolvedExecuteImmediateStmt:
		return newExecuteImmediateStmtNode(node.(*googlesql.ResolvedExecuteImmediateStmt))
	case googlesql.ResolvedNodeKindResolvedAssignmentStmt:
		return newAssignmentStmtNode(node.(*googlesql.ResolvedAssignmentStmt))
	case googlesql.ResolvedNodeKindResolvedCreateEntityStmt:
		return newCreateEntityStmtNode(node.(*googlesql.ResolvedCreateEntityStmt))
	case googlesql.ResolvedNodeKindResolvedAlterEntityStmt:
		return newAlterEntityStmtNode(node.(*googlesql.ResolvedAlterEntityStmt))
	case googlesql.ResolvedNodeKindResolvedPivotColumn:
		return newPivotColumnNode(node.(*googlesql.ResolvedPivotColumn))
	case googlesql.ResolvedNodeKindResolvedPivotScan:
		return newPivotScanNode(node.(*googlesql.ResolvedPivotScan))
	case googlesql.ResolvedNodeKindResolvedReturningClause:
		return newReturningClauseNode(node.(*googlesql.ResolvedReturningClause))
	case googlesql.ResolvedNodeKindResolvedUnpivotArg:
		return newUnpivotArgNode(node.(*googlesql.ResolvedUnpivotArg))
	case googlesql.ResolvedNodeKindResolvedUnpivotScan:
		return newUnpivotScanNode(node.(*googlesql.ResolvedUnpivotScan))
	case googlesql.ResolvedNodeKindResolvedCloneDataStmt:
		return newCloneDataStmtNode(node.(*googlesql.ResolvedCloneDataStmt))
	case googlesql.ResolvedNodeKindResolvedTableAndColumnInfo:
		return newTableAndColumnInfoNode(node.(*googlesql.ResolvedTableAndColumnInfo))
	case googlesql.ResolvedNodeKindResolvedAnalyzeStmt:
		return newAnalyzeStmtNode(node.(*googlesql.ResolvedAnalyzeStmt))
	case googlesql.ResolvedNodeKindResolvedAuxLoadDataStmt:
		return newAuxLoadDataStmtNode(node.(*googlesql.ResolvedAuxLoadDataStmt))
	}
	return nil
}

type LiteralNode struct {
	node *googlesql.ResolvedLiteral
}

type ParameterNode struct {
	node *googlesql.ResolvedParameter
}

type ExpressionColumnNode struct {
	node *googlesql.ResolvedExpressionColumn
}

type ColumnRefNode struct {
	node *googlesql.ResolvedColumnRef
}

type ConstantNode struct {
	node *googlesql.ResolvedConstant
}

type SystemVariableNode struct {
	node *googlesql.ResolvedSystemVariable
}

type InlineLambdaNode struct {
	node *googlesql.ResolvedInlineLambda
}

type FilterFieldArgNode struct {
	node *googlesql.ResolvedFilterFieldArg
}

type FilterFieldNode struct {
	node *googlesql.ResolvedFilterField
}

type FunctionCallNode struct {
	node *googlesql.ResolvedFunctionCall
}

type AggregateFunctionCallNode struct {
	node *googlesql.ResolvedAggregateFunctionCall
}

type AnalyticFunctionCallNode struct {
	node *googlesql.ResolvedAnalyticFunctionCall
}

type ExtendedCastElementNode struct {
	node *googlesql.ResolvedExtendedCastElement
}

type ExtendedCastNode struct {
	node *googlesql.ResolvedExtendedCast
}

type CastNode struct {
	node *googlesql.ResolvedCast
}

type MakeStructNode struct {
	node *googlesql.ResolvedMakeStruct
}

type MakeProtoNode struct {
	node *googlesql.ResolvedMakeProto
}

type MakeProtoFieldNode struct {
	node *googlesql.ResolvedMakeProtoField
}

type GetStructFieldNode struct {
	node *googlesql.ResolvedGetStructField
}

type GetProtoFieldNode struct {
	node *googlesql.ResolvedGetProtoField
}

type GetJsonFieldNode struct {
	node *googlesql.ResolvedGetJsonField
}

type FlattenNode struct {
	node *googlesql.ResolvedFlatten
}

type FlattenedArgNode struct {
	node *googlesql.ResolvedFlattenedArg
}

type ReplaceFieldItemNode struct {
	node *googlesql.ResolvedReplaceFieldItem
}

type ReplaceFieldNode struct {
	node *googlesql.ResolvedReplaceField
}

type SubqueryExprNode struct {
	node *googlesql.ResolvedSubqueryExpr
}

type LetExprNode struct {
	node *ResolvedLetExprNode
}

type ModelNode struct {
	node *googlesql.ResolvedModel
}

type ConnectionNode struct {
	node *googlesql.ResolvedConnection
}

type DescriptorNode struct {
	node *googlesql.ResolvedDescriptor
}

type SingleRowScanNode struct {
	node *googlesql.ResolvedSingleRowScan
}

type TableScanNode struct {
	node *googlesql.ResolvedTableScan
}

type JoinScanNode struct {
	node *googlesql.ResolvedJoinScan
}

type ArrayScanNode struct {
	node *googlesql.ResolvedArrayScan
}

type ColumnHolderNode struct {
	node *googlesql.ResolvedColumnHolder
}

type FilterScanNode struct {
	node *googlesql.ResolvedFilterScan
}

type GroupingSetNode struct {
	node *googlesql.ResolvedGroupingSet
}

type AggregateScanNode struct {
	node *googlesql.ResolvedAggregateScan
}

type AnonymizedAggregateScanNode struct {
	node *googlesql.ResolvedAnonymizedAggregateScan
}

type SetOperationItemNode struct {
	node *googlesql.ResolvedSetOperationItem
}

type SetOperationScanNode struct {
	node *googlesql.ResolvedSetOperationScan
}

type OrderByScanNode struct {
	node *googlesql.ResolvedOrderByScan
}

type LimitOffsetScanNode struct {
	node *googlesql.ResolvedLimitOffsetScan
}

type WithRefScanNode struct {
	node *googlesql.ResolvedWithRefScan
}

type AnalyticScanNode struct {
	node *googlesql.ResolvedAnalyticScan
}

type SampleScanNode struct {
	node *googlesql.ResolvedSampleScan
}

type ComputedColumnNode struct {
	node *googlesql.ResolvedComputedColumn
}

type OrderByItemNode struct {
	node *googlesql.ResolvedOrderByItem
}

type ColumnAnnotationsNode struct {
	node *googlesql.ResolvedColumnAnnotations
}

type GeneratedColumnInfoNode struct {
	node *googlesql.ResolvedGeneratedColumnInfo
}

type ColumnDefaultValueNode struct {
	node *googlesql.ResolvedColumnDefaultValue
}

type ColumnDefinitionNode struct {
	node *googlesql.ResolvedColumnDefinition
}

type PrimaryKeyNode struct {
	node *googlesql.ResolvedPrimaryKey
}

type ForeignKeyNode struct {
	node *googlesql.ResolvedForeignKey
}

type CheckConstraintNode struct {
	node *googlesql.ResolvedCheckConstraint
}

type OutputColumnNode struct {
	node *googlesql.ResolvedOutputColumn
}

type ProjectScanNode struct {
	node *googlesql.ResolvedProjectScan
}

type TVFScanNode struct {
	node *googlesql.ResolvedTVFScan
}

type GroupRowsScanNode struct {
	node *googlesql.ResolvedGroupRowsScan
}

type FunctionArgumentNode struct {
	node *googlesql.ResolvedFunctionArgument
}

type ExplainStmtNode struct {
	node *googlesql.ResolvedExplainStmt
}

type QueryStmtNode struct {
	node *googlesql.ResolvedQueryStmt
}

type CreateDatabaseStmtNode struct {
	node *googlesql.ResolvedCreateDatabaseStmt
}

type IndexItemNode struct {
	node *googlesql.ResolvedIndexItem
}

type UnnestItemNode struct {
	node *googlesql.ResolvedUnnestItem
}

type CreateIndexStmtNode struct {
	node *googlesql.ResolvedCreateIndexStmt
}

type CreateSchemaStmtNode struct {
	node *googlesql.ResolvedCreateSchemaStmt
}

type CreateTableStmtNode struct {
	node *googlesql.ResolvedCreateTableStmt
}

type CreateTableAsSelectStmtNode struct {
	node *googlesql.ResolvedCreateTableAsSelectStmt
}

type CreateModelStmtNode struct {
	node *googlesql.ResolvedCreateModelStmt
}

type CreateViewStmtNode struct {
	node *googlesql.ResolvedCreateViewStmt
}

type WithPartitionColumnsNode struct {
	node *googlesql.ResolvedWithPartitionColumns
}

type CreateSnapshotTableStmtNode struct {
	node *googlesql.ResolvedCreateSnapshotTableStmt
}

type CreateExternalTableStmtNode struct {
	node *googlesql.ResolvedCreateExternalTableStmt
}

type ExportModelStmtNode struct {
	node *googlesql.ResolvedExportModelStmt
}

type ExportDataStmtNode struct {
	node *googlesql.ResolvedExportDataStmt
}

type DefineTableStmtNode struct {
	node *googlesql.ResolvedDefineTableStmt
}

type DescribeStmtNode struct {
	node *googlesql.ResolvedDescribeStmt
}

type ShowStmtNode struct {
	node *googlesql.ResolvedShowStmt
}

type BeginStmtNode struct {
	node *googlesql.ResolvedBeginStmt
}

type SetTransactionStmtNode struct {
	node *googlesql.ResolvedSetTransactionStmt
}

type CommitStmtNode struct {
	node *googlesql.ResolvedCommitStmt
}

type RollbackStmtNode struct {
	node *googlesql.ResolvedRollbackStmt
}

type StartBatchStmtNode struct {
	node *googlesql.ResolvedStartBatchStmt
}

type RunBatchStmtNode struct {
	node *googlesql.ResolvedRunBatchStmt
}

type AbortBatchStmtNode struct {
	node *googlesql.ResolvedAbortBatchStmt
}

type DropStmtNode struct {
	node *googlesql.ResolvedDropStmt
}

type DropMaterializedViewStmtNode struct {
	node *googlesql.ResolvedDropMaterializedViewStmt
}

type DropSnapshotTableStmtNode struct {
	node *googlesql.ResolvedDropSnapshotTableStmt
}

type RecursiveRefScanNode struct {
	node *googlesql.ResolvedRecursiveRefScan
}

type RecursiveScanNode struct {
	node *googlesql.ResolvedRecursiveScan
}

type WithScanNode struct {
	node *googlesql.ResolvedWithScan
}

type WithEntryNode struct {
	node *googlesql.ResolvedWithEntry
}

type OptionNode struct {
	node *googlesql.ResolvedOption
}

type WindowPartitioningNode struct {
	node *googlesql.ResolvedWindowPartitioning
}

type WindowOrderingNode struct {
	node *googlesql.ResolvedWindowOrdering
}

type WindowFrameNode struct {
	node *googlesql.ResolvedWindowFrame
}

type AnalyticFunctionGroupNode struct {
	node *googlesql.ResolvedAnalyticFunctionGroup
}

type WindowFrameExprNode struct {
	node *googlesql.ResolvedWindowFrameExpr
}

type DMLValueNode struct {
	node *googlesql.ResolvedDMLValue
}

type DMLDefaultNode struct {
	node *googlesql.ResolvedDMLDefault
}

type AssertStmtNode struct {
	node *googlesql.ResolvedAssertStmt
}

type AssertRowsModifiedNode struct {
	node *googlesql.ResolvedAssertRowsModified
}

type InsertRowNode struct {
	node *googlesql.ResolvedInsertRow
}

type InsertStmtNode struct {
	node *googlesql.ResolvedInsertStmt
}

type DeleteStmtNode struct {
	node *googlesql.ResolvedDeleteStmt
}

type UpdateItemNode struct {
	node *googlesql.ResolvedUpdateItem
}

type UpdateArrayItemNode struct {
	node *ResolvedUpdateArrayItemNode
}

type UpdateStmtNode struct {
	node *googlesql.ResolvedUpdateStmt
}

type MergeWhenNode struct {
	node *googlesql.ResolvedMergeWhen
}

type MergeStmtNode struct {
	node *googlesql.ResolvedMergeStmt
}

type TruncateStmtNode struct {
	node *googlesql.ResolvedTruncateStmt
}

type ObjectUnitNode struct {
	node *googlesql.ResolvedObjectUnit
}

type PrivilegeNode struct {
	node *googlesql.ResolvedPrivilege
}

type GrantStmtNode struct {
	node *googlesql.ResolvedGrantStmt
}

type RevokeStmtNode struct {
	node *googlesql.ResolvedRevokeStmt
}

type AlterDatabaseStmtNode struct {
	node *googlesql.ResolvedAlterDatabaseStmt
}

type AlterMaterializedViewStmtNode struct {
	node *googlesql.ResolvedAlterMaterializedViewStmt
}

type AlterSchemaStmtNode struct {
	node *googlesql.ResolvedAlterSchemaStmt
}

type AlterTableStmtNode struct {
	node *googlesql.ResolvedAlterTableStmt
}

type AlterViewStmtNode struct {
	node *googlesql.ResolvedAlterViewStmt
}

type SetOptionsActionNode struct {
	node *googlesql.ResolvedSetOptionsAction
}

type AddColumnActionNode struct {
	node *googlesql.ResolvedAddColumnAction
}

type AddConstraintActionNode struct {
	node *googlesql.ResolvedAddConstraintAction
}

type DropConstraintActionNode struct {
	node *googlesql.ResolvedDropConstraintAction
}

type DropPrimaryKeyActionNode struct {
	node *googlesql.ResolvedDropPrimaryKeyAction
}

type AlterColumnOptionsActionNode struct {
	node *googlesql.ResolvedAlterColumnOptionsAction
}

type AlterColumnDropNotNullActionNode struct {
	node *googlesql.ResolvedAlterColumnDropNotNullAction
}

type AlterColumnSetDataTypeActionNode struct {
	node *googlesql.ResolvedAlterColumnSetDataTypeAction
}

type AlterColumnSetDefaultActionNode struct {
	node *googlesql.ResolvedAlterColumnSetDefaultAction
}

type AlterColumnDropDefaultActionNode struct {
	node *googlesql.ResolvedAlterColumnDropDefaultAction
}

type DropColumnActionNode struct {
	node *googlesql.ResolvedDropColumnAction
}

type RenameColumnActionNode struct {
	node *googlesql.ResolvedRenameColumnAction
}

type SetAsActionNode struct {
	node *googlesql.ResolvedSetAsAction
}

type SetCollateClauseNode struct {
	node *googlesql.ResolvedSetCollateClause
}

type AlterTableSetOptionsStmtNode struct {
	node *googlesql.ResolvedAlterTableSetOptionsStmt
}

type RenameStmtNode struct {
	node *googlesql.ResolvedRenameStmt
}

type CreatePrivilegeRestrictionStmtNode struct {
	node *googlesql.ResolvedCreatePrivilegeRestrictionStmt
}

type CreateRowAccessPolicyStmtNode struct {
	node *googlesql.ResolvedCreateRowAccessPolicyStmt
}

type DropPrivilegeRestrictionStmtNode struct {
	node *googlesql.ResolvedDropPrivilegeRestrictionStmt
}

type DropRowAccessPolicyStmtNode struct {
	node *googlesql.ResolvedDropRowAccessPolicyStmt
}

type DropSearchIndexStmtNode struct {
	node *ResolvedDropSearchIndexStmtNode
}

type GrantToActionNode struct {
	node *googlesql.ResolvedGrantToAction
}

type RestrictToActionNode struct {
	node *googlesql.ResolvedRestrictToAction
}

type AddToRestricteeListActionNode struct {
	node *googlesql.ResolvedAddToRestricteeListAction
}

type RemoveFromRestricteeListActionNode struct {
	node *googlesql.ResolvedRemoveFromRestricteeListAction
}

type FilterUsingActionNode struct {
	node *googlesql.ResolvedFilterUsingAction
}

type RevokeFromActionNode struct {
	node *googlesql.ResolvedRevokeFromAction
}

type RenameToActionNode struct {
	node *googlesql.ResolvedRenameToAction
}

type AlterPrivilegeRestrictionStmtNode struct {
	node *googlesql.ResolvedAlterPrivilegeRestrictionStmt
}

type AlterRowAccessPolicyStmtNode struct {
	node *googlesql.ResolvedAlterRowAccessPolicyStmt
}

type AlterAllRowAccessPoliciesStmtNode struct {
	node *googlesql.ResolvedAlterAllRowAccessPoliciesStmt
}

type CreateConstantStmtNode struct {
	node *googlesql.ResolvedCreateConstantStmt
}

type CreateFunctionStmtNode struct {
	node *googlesql.ResolvedCreateFunctionStmt
}

type ArgumentDefNode struct {
	node *googlesql.ResolvedArgumentDef
}

type ArgumentRefNode struct {
	node *googlesql.ResolvedArgumentRef
}

type CreateTableFunctionStmtNode struct {
	node *googlesql.ResolvedCreateTableFunctionStmt
}

type RelationArgumentScanNode struct {
	node *googlesql.ResolvedRelationArgumentScan
}

type ArgumentListNode struct {
	node *googlesql.ResolvedArgumentList
}

type FunctionSignatureHolderNode struct {
	node *googlesql.ResolvedFunctionSignatureHolder
}

type DropFunctionStmtNode struct {
	node *googlesql.ResolvedDropFunctionStmt
}

type DropTableFunctionStmtNode struct {
	node *googlesql.ResolvedDropTableFunctionStmt
}

type CallStmtNode struct {
	node *googlesql.ResolvedCallStmt
}

type ImportStmtNode struct {
	node *googlesql.ResolvedImportStmt
}

type ModuleStmtNode struct {
	node *googlesql.ResolvedModuleStmt
}

type AggregateHavingModifierNode struct {
	node *googlesql.ResolvedAggregateHavingModifier
}

type CreateMaterializedViewStmtNode struct {
	node *googlesql.ResolvedCreateMaterializedViewStmt
}

type CreateProcedureStmtNode struct {
	node *googlesql.ResolvedCreateProcedureStmt
}

type ExecuteImmediateArgumentNode struct {
	node *googlesql.ResolvedExecuteImmediateArgument
}

type ExecuteImmediateStmtNode struct {
	node *googlesql.ResolvedExecuteImmediateStmt
}

type AssignmentStmtNode struct {
	node *googlesql.ResolvedAssignmentStmt
}

type CreateEntityStmtNode struct {
	node *googlesql.ResolvedCreateEntityStmt
}

type AlterEntityStmtNode struct {
	node *googlesql.ResolvedAlterEntityStmt
}

type PivotColumnNode struct {
	node *googlesql.ResolvedPivotColumn
}

type PivotScanNode struct {
	node *googlesql.ResolvedPivotScan
}

type ReturningClauseNode struct {
	node *googlesql.ResolvedReturningClause
}

type UnpivotArgNode struct {
	node *googlesql.ResolvedUnpivotArg
}

type UnpivotScanNode struct {
	node *googlesql.ResolvedUnpivotScan
}

type CloneDataStmtNode struct {
	node *googlesql.ResolvedCloneDataStmt
}

type TableAndColumnInfoNode struct {
	node *googlesql.ResolvedTableAndColumnInfo
}

type AnalyzeStmtNode struct {
	node *googlesql.ResolvedAnalyzeStmt
}

type AuxLoadDataStmtNode struct {
	node *googlesql.ResolvedAuxLoadDataStmt
}

func newLiteralNode(n *googlesql.ResolvedLiteral) *LiteralNode {
	return &LiteralNode{node: n}
}

func newParameterNode(n *googlesql.ResolvedParameter) *ParameterNode {
	return &ParameterNode{node: n}
}

func newExpressionColumnNode(n *googlesql.ResolvedExpressionColumn) *ExpressionColumnNode {
	return &ExpressionColumnNode{node: n}
}

func newColumnRefNode(n *googlesql.ResolvedColumnRef) *ColumnRefNode {
	return &ColumnRefNode{node: n}
}

func newConstantNode(n *googlesql.ResolvedConstant) *ConstantNode {
	return &ConstantNode{node: n}
}

func newSystemVariableNode(n *googlesql.ResolvedSystemVariable) *SystemVariableNode {
	return &SystemVariableNode{node: n}
}

func newInlineLambdaNode(n *googlesql.ResolvedInlineLambda) *InlineLambdaNode {
	return &InlineLambdaNode{node: n}
}

func newFilterFieldArgNode(n *googlesql.ResolvedFilterFieldArg) *FilterFieldArgNode {
	return &FilterFieldArgNode{node: n}
}

func newFilterFieldNode(n *googlesql.ResolvedFilterField) *FilterFieldNode {
	return &FilterFieldNode{node: n}
}

func newFunctionCallNode(n *googlesql.ResolvedFunctionCall) *FunctionCallNode {
	return &FunctionCallNode{node: n}
}

func newAggregateFunctionCallNode(n *googlesql.ResolvedAggregateFunctionCall) *AggregateFunctionCallNode {
	return &AggregateFunctionCallNode{node: n}
}

func newAnalyticFunctionCallNode(n *googlesql.ResolvedAnalyticFunctionCall) *AnalyticFunctionCallNode {
	return &AnalyticFunctionCallNode{node: n}
}

func newExtendedCastElementNode(n *googlesql.ResolvedExtendedCastElement) *ExtendedCastElementNode {
	return &ExtendedCastElementNode{node: n}
}

func newExtendedCastNode(n *googlesql.ResolvedExtendedCast) *ExtendedCastNode {
	return &ExtendedCastNode{node: n}
}

func newCastNode(n *googlesql.ResolvedCast) *CastNode {
	return &CastNode{node: n}
}

func newMakeStructNode(n *googlesql.ResolvedMakeStruct) *MakeStructNode {
	return &MakeStructNode{node: n}
}

func newMakeProtoNode(n *googlesql.ResolvedMakeProto) *MakeProtoNode {
	return &MakeProtoNode{node: n}
}

func newMakeProtoFieldNode(n *googlesql.ResolvedMakeProtoField) *MakeProtoFieldNode {
	return &MakeProtoFieldNode{node: n}
}

func newGetStructFieldNode(n *googlesql.ResolvedGetStructField) *GetStructFieldNode {
	return &GetStructFieldNode{node: n}
}

func newGetProtoFieldNode(n *googlesql.ResolvedGetProtoField) *GetProtoFieldNode {
	return &GetProtoFieldNode{node: n}
}

func newGetJsonFieldNode(n *googlesql.ResolvedGetJsonField) *GetJsonFieldNode {
	return &GetJsonFieldNode{node: n}
}

func newFlattenNode(n *googlesql.ResolvedFlatten) *FlattenNode {
	return &FlattenNode{node: n}
}

func newFlattenedArgNode(n *googlesql.ResolvedFlattenedArg) *FlattenedArgNode {
	return &FlattenedArgNode{node: n}
}

func newReplaceFieldItemNode(n *googlesql.ResolvedReplaceFieldItem) *ReplaceFieldItemNode {
	return &ReplaceFieldItemNode{node: n}
}

func newReplaceFieldNode(n *googlesql.ResolvedReplaceField) *ReplaceFieldNode {
	return &ReplaceFieldNode{node: n}
}

func newSubqueryExprNode(n *googlesql.ResolvedSubqueryExpr) *SubqueryExprNode {
	return &SubqueryExprNode{node: n}
}

func newLetExprNode(n *ResolvedLetExprNode) *LetExprNode {
	return &LetExprNode{node: n}
}

func newModelNode(n *googlesql.ResolvedModel) *ModelNode {
	return &ModelNode{node: n}
}

func newConnectionNode(n *googlesql.ResolvedConnection) *ConnectionNode {
	return &ConnectionNode{node: n}
}

func newDescriptorNode(n *googlesql.ResolvedDescriptor) *DescriptorNode {
	return &DescriptorNode{node: n}
}

func newSingleRowScanNode(n *googlesql.ResolvedSingleRowScan) *SingleRowScanNode {
	return &SingleRowScanNode{node: n}
}

func newTableScanNode(n *googlesql.ResolvedTableScan) *TableScanNode {
	return &TableScanNode{node: n}
}

func newJoinScanNode(n *googlesql.ResolvedJoinScan) *JoinScanNode {
	return &JoinScanNode{node: n}
}

func newArrayScanNode(n *googlesql.ResolvedArrayScan) *ArrayScanNode {
	return &ArrayScanNode{node: n}
}

func newColumnHolderNode(n *googlesql.ResolvedColumnHolder) *ColumnHolderNode {
	return &ColumnHolderNode{node: n}
}

func newFilterScanNode(n *googlesql.ResolvedFilterScan) *FilterScanNode {
	return &FilterScanNode{node: n}
}

func newGroupingSetNode(n *googlesql.ResolvedGroupingSet) *GroupingSetNode {
	return &GroupingSetNode{node: n}
}

func newAggregateScanNode(n *googlesql.ResolvedAggregateScan) *AggregateScanNode {
	return &AggregateScanNode{node: n}
}

func newAnonymizedAggregateScanNode(n *googlesql.ResolvedAnonymizedAggregateScan) *AnonymizedAggregateScanNode {
	return &AnonymizedAggregateScanNode{node: n}
}

func newSetOperationItemNode(n *googlesql.ResolvedSetOperationItem) *SetOperationItemNode {
	return &SetOperationItemNode{node: n}
}

func newSetOperationScanNode(n *googlesql.ResolvedSetOperationScan) *SetOperationScanNode {
	return &SetOperationScanNode{node: n}
}

func newOrderByScanNode(n *googlesql.ResolvedOrderByScan) *OrderByScanNode {
	return &OrderByScanNode{node: n}
}

func newLimitOffsetScanNode(n *googlesql.ResolvedLimitOffsetScan) *LimitOffsetScanNode {
	return &LimitOffsetScanNode{node: n}
}

func newWithRefScanNode(n *googlesql.ResolvedWithRefScan) *WithRefScanNode {
	return &WithRefScanNode{node: n}
}

func newAnalyticScanNode(n *googlesql.ResolvedAnalyticScan) *AnalyticScanNode {
	return &AnalyticScanNode{node: n}
}

func newSampleScanNode(n *googlesql.ResolvedSampleScan) *SampleScanNode {
	return &SampleScanNode{node: n}
}

func newComputedColumnNode(n *googlesql.ResolvedComputedColumn) *ComputedColumnNode {
	return &ComputedColumnNode{node: n}
}

func newOrderByItemNode(n *googlesql.ResolvedOrderByItem) *OrderByItemNode {
	return &OrderByItemNode{node: n}
}

func newColumnAnnotationsNode(n *googlesql.ResolvedColumnAnnotations) *ColumnAnnotationsNode {
	return &ColumnAnnotationsNode{node: n}
}

func newGeneratedColumnInfoNode(n *googlesql.ResolvedGeneratedColumnInfo) *GeneratedColumnInfoNode {
	return &GeneratedColumnInfoNode{node: n}
}

func newColumnDefaultValueNode(n *googlesql.ResolvedColumnDefaultValue) *ColumnDefaultValueNode {
	return &ColumnDefaultValueNode{node: n}
}

func newColumnDefinitionNode(n *googlesql.ResolvedColumnDefinition) *ColumnDefinitionNode {
	return &ColumnDefinitionNode{node: n}
}

func newPrimaryKeyNode(n *googlesql.ResolvedPrimaryKey) *PrimaryKeyNode {
	return &PrimaryKeyNode{node: n}
}

func newForeignKeyNode(n *googlesql.ResolvedForeignKey) *ForeignKeyNode {
	return &ForeignKeyNode{node: n}
}

func newCheckConstraintNode(n *googlesql.ResolvedCheckConstraint) *CheckConstraintNode {
	return &CheckConstraintNode{node: n}
}

func newOutputColumnNode(n *googlesql.ResolvedOutputColumn) *OutputColumnNode {
	return &OutputColumnNode{node: n}
}

func newProjectScanNode(n *googlesql.ResolvedProjectScan) *ProjectScanNode {
	return &ProjectScanNode{node: n}
}

func newTVFScanNode(n *googlesql.ResolvedTVFScan) *TVFScanNode {
	return &TVFScanNode{node: n}
}

func newGroupRowsScanNode(n *googlesql.ResolvedGroupRowsScan) *GroupRowsScanNode {
	return &GroupRowsScanNode{node: n}
}

func newFunctionArgumentNode(n *googlesql.ResolvedFunctionArgument) *FunctionArgumentNode {
	return &FunctionArgumentNode{node: n}
}

func newExplainStmtNode(n *googlesql.ResolvedExplainStmt) *ExplainStmtNode {
	return &ExplainStmtNode{node: n}
}

func newQueryStmtNode(n *googlesql.ResolvedQueryStmt) *QueryStmtNode {
	return &QueryStmtNode{node: n}
}

func newCreateDatabaseStmtNode(n *googlesql.ResolvedCreateDatabaseStmt) *CreateDatabaseStmtNode {
	return &CreateDatabaseStmtNode{node: n}
}

func newIndexItemNode(n *googlesql.ResolvedIndexItem) *IndexItemNode {
	return &IndexItemNode{node: n}
}

func newUnnestItemNode(n *googlesql.ResolvedUnnestItem) *UnnestItemNode {
	return &UnnestItemNode{node: n}
}

func newCreateIndexStmtNode(n *googlesql.ResolvedCreateIndexStmt) *CreateIndexStmtNode {
	return &CreateIndexStmtNode{node: n}
}

func newCreateSchemaStmtNode(n *googlesql.ResolvedCreateSchemaStmt) *CreateSchemaStmtNode {
	return &CreateSchemaStmtNode{node: n}
}

func newCreateTableStmtNode(n *googlesql.ResolvedCreateTableStmt) *CreateTableStmtNode {
	return &CreateTableStmtNode{node: n}
}

func newCreateTableAsSelectStmtNode(n *googlesql.ResolvedCreateTableAsSelectStmt) *CreateTableAsSelectStmtNode {
	return &CreateTableAsSelectStmtNode{node: n}
}

func newCreateModelStmtNode(n *googlesql.ResolvedCreateModelStmt) *CreateModelStmtNode {
	return &CreateModelStmtNode{node: n}
}

func newCreateViewStmtNode(n *googlesql.ResolvedCreateViewStmt) *CreateViewStmtNode {
	return &CreateViewStmtNode{node: n}
}

func newWithPartitionColumnsNode(n *googlesql.ResolvedWithPartitionColumns) *WithPartitionColumnsNode {
	return &WithPartitionColumnsNode{node: n}
}

func newCreateSnapshotTableStmtNode(n *googlesql.ResolvedCreateSnapshotTableStmt) *CreateSnapshotTableStmtNode {
	return &CreateSnapshotTableStmtNode{node: n}
}

func newCreateExternalTableStmtNode(n *googlesql.ResolvedCreateExternalTableStmt) *CreateExternalTableStmtNode {
	return &CreateExternalTableStmtNode{node: n}
}

func newExportModelStmtNode(n *googlesql.ResolvedExportModelStmt) *ExportModelStmtNode {
	return &ExportModelStmtNode{node: n}
}

func newExportDataStmtNode(n *googlesql.ResolvedExportDataStmt) *ExportDataStmtNode {
	return &ExportDataStmtNode{node: n}
}

func newDefineTableStmtNode(n *googlesql.ResolvedDefineTableStmt) *DefineTableStmtNode {
	return &DefineTableStmtNode{node: n}
}

func newDescribeStmtNode(n *googlesql.ResolvedDescribeStmt) *DescribeStmtNode {
	return &DescribeStmtNode{node: n}
}

func newShowStmtNode(n *googlesql.ResolvedShowStmt) *ShowStmtNode {
	return &ShowStmtNode{node: n}
}

func newBeginStmtNode(n *googlesql.ResolvedBeginStmt) *BeginStmtNode {
	return &BeginStmtNode{node: n}
}

func newSetTransactionStmtNode(n *googlesql.ResolvedSetTransactionStmt) *SetTransactionStmtNode {
	return &SetTransactionStmtNode{node: n}
}

func newCommitStmtNode(n *googlesql.ResolvedCommitStmt) *CommitStmtNode {
	return &CommitStmtNode{node: n}
}

func newRollbackStmtNode(n *googlesql.ResolvedRollbackStmt) *RollbackStmtNode {
	return &RollbackStmtNode{node: n}
}

func newStartBatchStmtNode(n *googlesql.ResolvedStartBatchStmt) *StartBatchStmtNode {
	return &StartBatchStmtNode{node: n}
}

func newRunBatchStmtNode(n *googlesql.ResolvedRunBatchStmt) *RunBatchStmtNode {
	return &RunBatchStmtNode{node: n}
}

func newAbortBatchStmtNode(n *googlesql.ResolvedAbortBatchStmt) *AbortBatchStmtNode {
	return &AbortBatchStmtNode{node: n}
}

func newDropStmtNode(n *googlesql.ResolvedDropStmt) *DropStmtNode {
	return &DropStmtNode{node: n}
}

func newDropMaterializedViewStmtNode(n *googlesql.ResolvedDropMaterializedViewStmt) *DropMaterializedViewStmtNode {
	return &DropMaterializedViewStmtNode{node: n}
}

func newDropSnapshotTableStmtNode(n *googlesql.ResolvedDropSnapshotTableStmt) *DropSnapshotTableStmtNode {
	return &DropSnapshotTableStmtNode{node: n}
}

func newRecursiveRefScanNode(n *googlesql.ResolvedRecursiveRefScan) *RecursiveRefScanNode {
	return &RecursiveRefScanNode{node: n}
}

func newRecursiveScanNode(n *googlesql.ResolvedRecursiveScan) *RecursiveScanNode {
	return &RecursiveScanNode{node: n}
}

func newWithScanNode(n *googlesql.ResolvedWithScan) *WithScanNode {
	return &WithScanNode{node: n}
}

func newWithEntryNode(n *googlesql.ResolvedWithEntry) *WithEntryNode {
	return &WithEntryNode{node: n}
}

func newOptionNode(n *googlesql.ResolvedOption) *OptionNode {
	return &OptionNode{node: n}
}

func newWindowPartitioningNode(n *googlesql.ResolvedWindowPartitioning) *WindowPartitioningNode {
	return &WindowPartitioningNode{node: n}
}

func newWindowOrderingNode(n *googlesql.ResolvedWindowOrdering) *WindowOrderingNode {
	return &WindowOrderingNode{node: n}
}

func newWindowFrameNode(n *googlesql.ResolvedWindowFrame) *WindowFrameNode {
	return &WindowFrameNode{node: n}
}

func newAnalyticFunctionGroupNode(n *googlesql.ResolvedAnalyticFunctionGroup) *AnalyticFunctionGroupNode {
	return &AnalyticFunctionGroupNode{node: n}
}

func newWindowFrameExprNode(n *googlesql.ResolvedWindowFrameExpr) *WindowFrameExprNode {
	return &WindowFrameExprNode{node: n}
}

func newDMLValueNode(n *googlesql.ResolvedDMLValue) *DMLValueNode {
	return &DMLValueNode{node: n}
}

func newDMLDefaultNode(n *googlesql.ResolvedDMLDefault) *DMLDefaultNode {
	return &DMLDefaultNode{node: n}
}

func newAssertStmtNode(n *googlesql.ResolvedAssertStmt) *AssertStmtNode {
	return &AssertStmtNode{node: n}
}

func newAssertRowsModifiedNode(n *googlesql.ResolvedAssertRowsModified) *AssertRowsModifiedNode {
	return &AssertRowsModifiedNode{node: n}
}

func newInsertRowNode(n *googlesql.ResolvedInsertRow) *InsertRowNode {
	return &InsertRowNode{node: n}
}

func newInsertStmtNode(n *googlesql.ResolvedInsertStmt) *InsertStmtNode {
	return &InsertStmtNode{node: n}
}

func newDeleteStmtNode(n *googlesql.ResolvedDeleteStmt) *DeleteStmtNode {
	return &DeleteStmtNode{node: n}
}

func newUpdateItemNode(n *googlesql.ResolvedUpdateItem) *UpdateItemNode {
	return &UpdateItemNode{node: n}
}

func newUpdateArrayItemNode(n *ResolvedUpdateArrayItemNode) *UpdateArrayItemNode {
	return &UpdateArrayItemNode{node: n}
}

func newUpdateStmtNode(n *googlesql.ResolvedUpdateStmt) *UpdateStmtNode {
	return &UpdateStmtNode{node: n}
}

func newMergeWhenNode(n *googlesql.ResolvedMergeWhen) *MergeWhenNode {
	return &MergeWhenNode{node: n}
}

func newMergeStmtNode(n *googlesql.ResolvedMergeStmt) *MergeStmtNode {
	return &MergeStmtNode{node: n}
}

func newTruncateStmtNode(n *googlesql.ResolvedTruncateStmt) *TruncateStmtNode {
	return &TruncateStmtNode{node: n}
}

func newObjectUnitNode(n *googlesql.ResolvedObjectUnit) *ObjectUnitNode {
	return &ObjectUnitNode{node: n}
}

func newPrivilegeNode(n *googlesql.ResolvedPrivilege) *PrivilegeNode {
	return &PrivilegeNode{node: n}
}

func newGrantStmtNode(n *googlesql.ResolvedGrantStmt) *GrantStmtNode {
	return &GrantStmtNode{node: n}
}

func newRevokeStmtNode(n *googlesql.ResolvedRevokeStmt) *RevokeStmtNode {
	return &RevokeStmtNode{node: n}
}

func newAlterDatabaseStmtNode(n *googlesql.ResolvedAlterDatabaseStmt) *AlterDatabaseStmtNode {
	return &AlterDatabaseStmtNode{node: n}
}

func newAlterMaterializedViewStmtNode(n *googlesql.ResolvedAlterMaterializedViewStmt) *AlterMaterializedViewStmtNode {
	return &AlterMaterializedViewStmtNode{node: n}
}

func newAlterSchemaStmtNode(n *googlesql.ResolvedAlterSchemaStmt) *AlterSchemaStmtNode {
	return &AlterSchemaStmtNode{node: n}
}

func newAlterTableStmtNode(n *googlesql.ResolvedAlterTableStmt) *AlterTableStmtNode {
	return &AlterTableStmtNode{node: n}
}

func newAlterViewStmtNode(n *googlesql.ResolvedAlterViewStmt) *AlterViewStmtNode {
	return &AlterViewStmtNode{node: n}
}

func newSetOptionsActionNode(n *googlesql.ResolvedSetOptionsAction) *SetOptionsActionNode {
	return &SetOptionsActionNode{node: n}
}

func newAddColumnActionNode(n *googlesql.ResolvedAddColumnAction) *AddColumnActionNode {
	return &AddColumnActionNode{node: n}
}

func newAddConstraintActionNode(n *googlesql.ResolvedAddConstraintAction) *AddConstraintActionNode {
	return &AddConstraintActionNode{node: n}
}

func newDropConstraintActionNode(n *googlesql.ResolvedDropConstraintAction) *DropConstraintActionNode {
	return &DropConstraintActionNode{node: n}
}

func newDropPrimaryKeyActionNode(n *googlesql.ResolvedDropPrimaryKeyAction) *DropPrimaryKeyActionNode {
	return &DropPrimaryKeyActionNode{node: n}
}

func newAlterColumnOptionsActionNode(n *googlesql.ResolvedAlterColumnOptionsAction) *AlterColumnOptionsActionNode {
	return &AlterColumnOptionsActionNode{node: n}
}

func newAlterColumnDropNotNullActionNode(n *googlesql.ResolvedAlterColumnDropNotNullAction) *AlterColumnDropNotNullActionNode {
	return &AlterColumnDropNotNullActionNode{node: n}
}

func newAlterColumnSetDataTypeActionNode(n *googlesql.ResolvedAlterColumnSetDataTypeAction) *AlterColumnSetDataTypeActionNode {
	return &AlterColumnSetDataTypeActionNode{node: n}
}

func newAlterColumnSetDefaultActionNode(n *googlesql.ResolvedAlterColumnSetDefaultAction) *AlterColumnSetDefaultActionNode {
	return &AlterColumnSetDefaultActionNode{node: n}
}

func newAlterColumnDropDefaultActionNode(n *googlesql.ResolvedAlterColumnDropDefaultAction) *AlterColumnDropDefaultActionNode {
	return &AlterColumnDropDefaultActionNode{node: n}
}

func newDropColumnActionNode(n *googlesql.ResolvedDropColumnAction) *DropColumnActionNode {
	return &DropColumnActionNode{node: n}
}

func newRenameColumnActionNode(n *googlesql.ResolvedRenameColumnAction) *RenameColumnActionNode {
	return &RenameColumnActionNode{node: n}
}

func newSetAsActionNode(n *googlesql.ResolvedSetAsAction) *SetAsActionNode {
	return &SetAsActionNode{node: n}
}

func newSetCollateClauseNode(n *googlesql.ResolvedSetCollateClause) *SetCollateClauseNode {
	return &SetCollateClauseNode{node: n}
}

func newAlterTableSetOptionsStmtNode(n *googlesql.ResolvedAlterTableSetOptionsStmt) *AlterTableSetOptionsStmtNode {
	return &AlterTableSetOptionsStmtNode{node: n}
}

func newRenameStmtNode(n *googlesql.ResolvedRenameStmt) *RenameStmtNode {
	return &RenameStmtNode{node: n}
}

func newCreatePrivilegeRestrictionStmtNode(n *googlesql.ResolvedCreatePrivilegeRestrictionStmt) *CreatePrivilegeRestrictionStmtNode {
	return &CreatePrivilegeRestrictionStmtNode{node: n}
}

func newCreateRowAccessPolicyStmtNode(n *googlesql.ResolvedCreateRowAccessPolicyStmt) *CreateRowAccessPolicyStmtNode {
	return &CreateRowAccessPolicyStmtNode{node: n}
}

func newDropPrivilegeRestrictionStmtNode(n *googlesql.ResolvedDropPrivilegeRestrictionStmt) *DropPrivilegeRestrictionStmtNode {
	return &DropPrivilegeRestrictionStmtNode{node: n}
}

func newDropRowAccessPolicyStmtNode(n *googlesql.ResolvedDropRowAccessPolicyStmt) *DropRowAccessPolicyStmtNode {
	return &DropRowAccessPolicyStmtNode{node: n}
}

func newDropSearchIndexStmtNode(n *ResolvedDropSearchIndexStmtNode) *DropSearchIndexStmtNode {
	return &DropSearchIndexStmtNode{node: n}
}

func newGrantToActionNode(n *googlesql.ResolvedGrantToAction) *GrantToActionNode {
	return &GrantToActionNode{node: n}
}

func newRestrictToActionNode(n *googlesql.ResolvedRestrictToAction) *RestrictToActionNode {
	return &RestrictToActionNode{node: n}
}

func newAddToRestricteeListActionNode(n *googlesql.ResolvedAddToRestricteeListAction) *AddToRestricteeListActionNode {
	return &AddToRestricteeListActionNode{node: n}
}

func newRemoveFromRestricteeListActionNode(n *googlesql.ResolvedRemoveFromRestricteeListAction) *RemoveFromRestricteeListActionNode {
	return &RemoveFromRestricteeListActionNode{node: n}
}

func newFilterUsingActionNode(n *googlesql.ResolvedFilterUsingAction) *FilterUsingActionNode {
	return &FilterUsingActionNode{node: n}
}

func newRevokeFromActionNode(n *googlesql.ResolvedRevokeFromAction) *RevokeFromActionNode {
	return &RevokeFromActionNode{node: n}
}

func newRenameToActionNode(n *googlesql.ResolvedRenameToAction) *RenameToActionNode {
	return &RenameToActionNode{node: n}
}

func newAlterPrivilegeRestrictionStmtNode(n *googlesql.ResolvedAlterPrivilegeRestrictionStmt) *AlterPrivilegeRestrictionStmtNode {
	return &AlterPrivilegeRestrictionStmtNode{node: n}
}

func newAlterRowAccessPolicyStmtNode(n *googlesql.ResolvedAlterRowAccessPolicyStmt) *AlterRowAccessPolicyStmtNode {
	return &AlterRowAccessPolicyStmtNode{node: n}
}

func newAlterAllRowAccessPoliciesStmtNode(n *googlesql.ResolvedAlterAllRowAccessPoliciesStmt) *AlterAllRowAccessPoliciesStmtNode {
	return &AlterAllRowAccessPoliciesStmtNode{node: n}
}

func newCreateConstantStmtNode(n *googlesql.ResolvedCreateConstantStmt) *CreateConstantStmtNode {
	return &CreateConstantStmtNode{node: n}
}

func newCreateFunctionStmtNode(n *googlesql.ResolvedCreateFunctionStmt) *CreateFunctionStmtNode {
	return &CreateFunctionStmtNode{node: n}
}

func newArgumentDefNode(n *googlesql.ResolvedArgumentDef) *ArgumentDefNode {
	return &ArgumentDefNode{node: n}
}

func newArgumentRefNode(n *googlesql.ResolvedArgumentRef) *ArgumentRefNode {
	return &ArgumentRefNode{node: n}
}

func newCreateTableFunctionStmtNode(n *googlesql.ResolvedCreateTableFunctionStmt) *CreateTableFunctionStmtNode {
	return &CreateTableFunctionStmtNode{node: n}
}

func newRelationArgumentScanNode(n *googlesql.ResolvedRelationArgumentScan) *RelationArgumentScanNode {
	return &RelationArgumentScanNode{node: n}
}

func newArgumentListNode(n *googlesql.ResolvedArgumentList) *ArgumentListNode {
	return &ArgumentListNode{node: n}
}

func newFunctionSignatureHolderNode(n *googlesql.ResolvedFunctionSignatureHolder) *FunctionSignatureHolderNode {
	return &FunctionSignatureHolderNode{node: n}
}

func newDropFunctionStmtNode(n *googlesql.ResolvedDropFunctionStmt) *DropFunctionStmtNode {
	return &DropFunctionStmtNode{node: n}
}

func newDropTableFunctionStmtNode(n *googlesql.ResolvedDropTableFunctionStmt) *DropTableFunctionStmtNode {
	return &DropTableFunctionStmtNode{node: n}
}

func newCallStmtNode(n *googlesql.ResolvedCallStmt) *CallStmtNode {
	return &CallStmtNode{node: n}
}

func newImportStmtNode(n *googlesql.ResolvedImportStmt) *ImportStmtNode {
	return &ImportStmtNode{node: n}
}

func newModuleStmtNode(n *googlesql.ResolvedModuleStmt) *ModuleStmtNode {
	return &ModuleStmtNode{node: n}
}

func newAggregateHavingModifierNode(n *googlesql.ResolvedAggregateHavingModifier) *AggregateHavingModifierNode {
	return &AggregateHavingModifierNode{node: n}
}

func newCreateMaterializedViewStmtNode(n *googlesql.ResolvedCreateMaterializedViewStmt) *CreateMaterializedViewStmtNode {
	return &CreateMaterializedViewStmtNode{node: n}
}

func newCreateProcedureStmtNode(n *googlesql.ResolvedCreateProcedureStmt) *CreateProcedureStmtNode {
	return &CreateProcedureStmtNode{node: n}
}

func newExecuteImmediateArgumentNode(n *googlesql.ResolvedExecuteImmediateArgument) *ExecuteImmediateArgumentNode {
	return &ExecuteImmediateArgumentNode{node: n}
}

func newExecuteImmediateStmtNode(n *googlesql.ResolvedExecuteImmediateStmt) *ExecuteImmediateStmtNode {
	return &ExecuteImmediateStmtNode{node: n}
}

func newAssignmentStmtNode(n *googlesql.ResolvedAssignmentStmt) *AssignmentStmtNode {
	return &AssignmentStmtNode{node: n}
}

func newCreateEntityStmtNode(n *googlesql.ResolvedCreateEntityStmt) *CreateEntityStmtNode {
	return &CreateEntityStmtNode{node: n}
}

func newAlterEntityStmtNode(n *googlesql.ResolvedAlterEntityStmt) *AlterEntityStmtNode {
	return &AlterEntityStmtNode{node: n}
}

func newPivotColumnNode(n *googlesql.ResolvedPivotColumn) *PivotColumnNode {
	return &PivotColumnNode{node: n}
}

func newPivotScanNode(n *googlesql.ResolvedPivotScan) *PivotScanNode {
	return &PivotScanNode{node: n}
}

func newReturningClauseNode(n *googlesql.ResolvedReturningClause) *ReturningClauseNode {
	return &ReturningClauseNode{node: n}
}

func newUnpivotArgNode(n *googlesql.ResolvedUnpivotArg) *UnpivotArgNode {
	return &UnpivotArgNode{node: n}
}

func newUnpivotScanNode(n *googlesql.ResolvedUnpivotScan) *UnpivotScanNode {
	return &UnpivotScanNode{node: n}
}

func newCloneDataStmtNode(n *googlesql.ResolvedCloneDataStmt) *CloneDataStmtNode {
	return &CloneDataStmtNode{node: n}
}

func newTableAndColumnInfoNode(n *googlesql.ResolvedTableAndColumnInfo) *TableAndColumnInfoNode {
	return &TableAndColumnInfoNode{node: n}
}

func newAnalyzeStmtNode(n *googlesql.ResolvedAnalyzeStmt) *AnalyzeStmtNode {
	return &AnalyzeStmtNode{node: n}
}

func newAuxLoadDataStmtNode(n *googlesql.ResolvedAuxLoadDataStmt) *AuxLoadDataStmtNode {
	return &AuxLoadDataStmtNode{node: n}
}
