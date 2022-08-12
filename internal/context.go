package internal

import (
	"context"
	"time"

	ast "github.com/goccy/go-zetasql/resolved_ast"
)

type (
	namePathKey                     struct{}
	fullNamePathMapKey              struct{}
	columnRefMapKey                 struct{}
	funcMapKey                      struct{}
	analyticOrderColumnNamesKey     struct{}
	analyticPartitionColumnNamesKey struct{}
	analyticTableNameKey            struct{}
	analyticInputScanKey            struct{}
	arraySubqueryColumnNameKey      struct{}
	currentTimeKey                  struct{}
	existsGroupByKey                struct{}
	needsTableNameForColumnKey      struct{}
	tableNameToColumnListMapKey     struct{}
	useColumnIDKey                  struct{}
	rowIDColumnKey                  struct{}
	useTableNameForColumnKey        struct{}
)

func namePathFromContext(ctx context.Context) []string {
	value := ctx.Value(namePathKey{})
	if value == nil {
		return nil
	}
	return value.([]string)
}

func withNamePath(ctx context.Context, namePath []string) context.Context {
	return context.WithValue(ctx, namePathKey{}, namePath)
}

func withFullNamePathMap(ctx context.Context, v map[string][]string) context.Context {
	return context.WithValue(ctx, fullNamePathMapKey{}, v)
}

func fullNamePathMapFromContext(ctx context.Context) map[string][]string {
	value := ctx.Value(fullNamePathMapKey{})
	if value == nil {
		return nil
	}
	return value.(map[string][]string)
}

func withColumnRefMap(ctx context.Context, m map[string]string) context.Context {
	return context.WithValue(ctx, columnRefMapKey{}, m)
}

func columnRefMap(ctx context.Context) map[string]string {
	value := ctx.Value(columnRefMapKey{})
	if value == nil {
		return nil
	}
	return value.(map[string]string)
}

func withFuncMap(ctx context.Context, m map[string]*FunctionSpec) context.Context {
	return context.WithValue(ctx, funcMapKey{}, m)
}

func funcMapFromContext(ctx context.Context) map[string]*FunctionSpec {
	value := ctx.Value(funcMapKey{})
	if value == nil {
		return nil
	}
	return value.(map[string]*FunctionSpec)
}

type analyticOrderBy struct {
	column string
	isAsc  bool
}

type analyticOrderColumnNames struct {
	values []*analyticOrderBy
}

func withAnalyticOrderColumnNames(ctx context.Context, v *analyticOrderColumnNames) context.Context {
	return context.WithValue(ctx, analyticOrderColumnNamesKey{}, v)
}

func analyticOrderColumnNamesFromContext(ctx context.Context) *analyticOrderColumnNames {
	value := ctx.Value(analyticOrderColumnNamesKey{})
	if value == nil {
		return nil
	}
	return value.(*analyticOrderColumnNames)
}

func withAnalyticPartitionColumnNames(ctx context.Context, names []string) context.Context {
	return context.WithValue(ctx, analyticPartitionColumnNamesKey{}, names)
}

func analyticPartitionColumnNamesFromContext(ctx context.Context) []string {
	value := ctx.Value(analyticPartitionColumnNamesKey{})
	if value == nil {
		return nil
	}
	return value.([]string)
}

func withAnalyticTableName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, analyticTableNameKey{}, name)
}

func analyticTableNameFromContext(ctx context.Context) string {
	value := ctx.Value(analyticTableNameKey{})
	if value == nil {
		return ""
	}
	return value.(string)
}

func withAnalyticInputScan(ctx context.Context, input string) context.Context {
	return context.WithValue(ctx, analyticInputScanKey{}, input)
}

func analyticInputScanFromContext(ctx context.Context) string {
	value := ctx.Value(analyticInputScanKey{})
	if value == nil {
		return ""
	}
	return value.(string)
}

type arraySubqueryColumnNames struct {
	names []string
}

func withArraySubqueryColumnName(ctx context.Context, v *arraySubqueryColumnNames) context.Context {
	return context.WithValue(ctx, arraySubqueryColumnNameKey{}, v)
}

func arraySubqueryColumnNameFromContext(ctx context.Context) *arraySubqueryColumnNames {
	value := ctx.Value(arraySubqueryColumnNameKey{})
	if value == nil {
		return nil
	}
	return value.(*arraySubqueryColumnNames)
}

type existsGroupBy struct {
	exists bool
}

func withExistsGroupBy(ctx context.Context, v *existsGroupBy) context.Context {
	return context.WithValue(ctx, existsGroupByKey{}, v)
}

func existsGroupByFromContext(ctx context.Context) *existsGroupBy {
	value := ctx.Value(existsGroupByKey{})
	if value == nil {
		return nil
	}
	return value.(*existsGroupBy)
}

func withNeedsTableNameForColumn(ctx context.Context) context.Context {
	return context.WithValue(ctx, needsTableNameForColumnKey{}, true)
}

func needsTableNameForColumn(ctx context.Context) bool {
	value := ctx.Value(needsTableNameForColumnKey{})
	if value == nil {
		return false
	}
	return value.(bool)
}

func withUseColumnID(ctx context.Context) context.Context {
	return context.WithValue(ctx, useColumnIDKey{}, true)
}

func useColumnID(ctx context.Context) bool {
	value := ctx.Value(useColumnIDKey{})
	if value == nil {
		return false
	}
	return value.(bool)
}

func unuseColumnID(ctx context.Context) context.Context {
	return context.WithValue(ctx, useColumnIDKey{}, false)
}

func withUseTableNameForColumn(ctx context.Context) context.Context {
	return context.WithValue(ctx, useTableNameForColumnKey{}, true)
}

func withoutUseTableNameForColumn(ctx context.Context) context.Context {
	return context.WithValue(ctx, useTableNameForColumnKey{}, false)
}

func useTableNameForColumn(ctx context.Context) bool {
	value := ctx.Value(useTableNameForColumnKey{})
	if value == nil {
		return false
	}
	return value.(bool)
}

func withTableNameToColumnListMap(ctx context.Context, v map[string][]*ast.Column) context.Context {
	return context.WithValue(ctx, tableNameToColumnListMapKey{}, v)
}

func tableNameToColumnListMap(ctx context.Context) map[string][]*ast.Column {
	value := ctx.Value(tableNameToColumnListMapKey{})
	if value == nil {
		return nil
	}
	return value.(map[string][]*ast.Column)
}

func withRowIDColumn(ctx context.Context) context.Context {
	return context.WithValue(ctx, rowIDColumnKey{}, true)
}

func needsRowIDColumn(ctx context.Context) bool {
	value := ctx.Value(rowIDColumnKey{})
	if value == nil {
		return false
	}
	return value.(bool)
}

func WithCurrentTime(ctx context.Context, now time.Time) context.Context {
	return context.WithValue(ctx, currentTimeKey{}, &now)
}

func CurrentTime(ctx context.Context) *time.Time {
	value := ctx.Value(currentTimeKey{})
	if value == nil {
		return nil
	}
	return value.(*time.Time)
}
