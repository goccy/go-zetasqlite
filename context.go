package zetasqlite

import "context"

type (
	namePathKey     struct{}
	fullNamePathKey struct{}
	columnRefMapKey struct{}
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
