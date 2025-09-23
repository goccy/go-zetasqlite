package internal

import (
	"context"
	"time"

	"github.com/goccy/go-zetasql"
)

type (
	analyzerKey    struct{}
	namePathKey    struct{}
	nodeMapKey     struct{}
	funcMapKey     struct{}
	currentTimeKey struct{}
)

func analyzerFromContext(ctx context.Context) *Analyzer {
	value := ctx.Value(analyzerKey{})
	if value == nil {
		return nil
	}
	return value.(*Analyzer)
}

func withAnalyzer(ctx context.Context, analyzer *Analyzer) context.Context {
	return context.WithValue(ctx, analyzerKey{}, analyzer)
}

func namePathFromContext(ctx context.Context) *NamePath {
	value := ctx.Value(namePathKey{})
	if value == nil {
		return nil
	}
	return value.(*NamePath)
}

func withNamePath(ctx context.Context, namePath *NamePath) context.Context {
	return context.WithValue(ctx, namePathKey{}, namePath)
}

func withNodeMap(ctx context.Context, m *zetasql.NodeMap) context.Context {
	return context.WithValue(ctx, nodeMapKey{}, m)
}

func nodeMapFromContext(ctx context.Context) *zetasql.NodeMap {
	value := ctx.Value(nodeMapKey{})
	if value == nil {
		return nil
	}
	return value.(*zetasql.NodeMap)
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
