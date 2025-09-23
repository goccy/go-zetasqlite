package internal

import (
	"context"
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"sync"
)

// Global singleton for performance
var (
	globalQueryTransformFactory *QueryTransformFactory
	queryTransformFactoryOnce   sync.Once
)

// GetGlobalQueryTransformFactory returns the query transform factory coordinator instance
func GetGlobalQueryTransformFactory() *QueryTransformFactory {
	queryTransformFactoryOnce.Do(func() {
		globalQueryTransformFactory = NewQueryTransformFactory(nil, nil)
	})
	return globalQueryTransformFactory
}

// QueryTransformFactory creates and configures the complete transformation pipeline
type QueryTransformFactory struct {
	config      *TransformConfig
	coordinator Coordinator
}

// NewQueryTransformFactory creates a new factory with the given configuration
func NewQueryTransformFactory(config *TransformConfig, coordinator Coordinator) *QueryTransformFactory {
	if config == nil {
		config = DefaultTransformConfig()
	}

	if coordinator == nil {
		coordinator = GetGlobalCoordinator()
	}

	return &QueryTransformFactory{
		config:      config,
		coordinator: coordinator,
	}
}

// CreateTransformContext creates a transform context with the factory's configuration
func (f *QueryTransformFactory) CreateTransformContext(ctx context.Context) TransformContext {
	return NewDefaultTransformContext(ctx, f.config)
}

// TransformQuery is a convenience method that transforms a complete query
func (f *QueryTransformFactory) TransformQuery(ctx context.Context, queryNode ast.Node) (*TransformResult, error) {
	transformCtx := f.CreateTransformContext(ctx)

	// Transform the query
	fragment, err := f.coordinator.TransformStatementNode(queryNode, transformCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform query: %w", err)
	}

	result := NewTransformResult(fragment)

	return result, nil
}
