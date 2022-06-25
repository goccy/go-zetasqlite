package zetasqlite

import "context"

type (
	namePathKey     struct{}
	fullNamePathKey struct{}
	columnRefMapKey struct{}
	funcMapKey      struct{}
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

type fullNamePath struct {
	paths [][]string
	idx   int
}

func withFullNamePath(ctx context.Context, fullpath *fullNamePath) context.Context {
	return context.WithValue(ctx, fullNamePathKey{}, fullpath)
}

func fullNamePathFromContext(ctx context.Context) *fullNamePath {
	value := ctx.Value(fullNamePathKey{})
	if value == nil {
		return nil
	}
	return value.(*fullNamePath)
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
