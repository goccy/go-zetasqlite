package internal

import (
	"fmt"
)

// DropStmtTransformer handles transformation of DROP statement data to DropStatement fragments.
//
// In BigQuery/ZetaSQL, DROP statements are used to remove database objects like tables, views,
// indexes, schemas, and functions. These statements are typically simple and don't require
// complex recursive transformation.
//
// The transformer converts extracted DropData by:
// - Validating the input data type is StatementTypeDrop
// - Creating a DropStatement SQLFragment with the extracted object information
// - No recursive transformation is needed since DROP statements are leaf-level operations
//
// This transformer bridges the gap between the extracted DropData and the
// DropStatement SQL generation, ensuring proper object type handling and name formatting.
type DropStmtTransformer struct {
	coordinator Coordinator // For any future recursive transformation needs (currently unused)
}

// NewDropStmtTransformer creates a new DROP statement transformer
func NewDropStmtTransformer(coordinator Coordinator) *DropStmtTransformer {
	return &DropStmtTransformer{
		coordinator: coordinator,
	}
}

// Transform converts DROP statement data to DropStatement
func (t *DropStmtTransformer) Transform(data StatementData, ctx TransformContext) (SQLFragment, error) {
	if data.Type != StatementTypeDrop || data.Drop == nil {
		return nil, fmt.Errorf("expected drop statement data for drop stmt, got type %v", data.Type)
	}

	dropData := data.Drop

	// Create the DropStatement directly from the extracted data
	// No recursive transformation needed since DROP statements are simple
	return &DropStatement{
		IfExists:   dropData.IfExists,
		ObjectType: dropData.ObjectType,
		ObjectName: dropData.ObjectName,
	}, nil
}
