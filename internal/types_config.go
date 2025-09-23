package internal

import "os"

// TransformConfig provides configuration for transformations
type TransformConfig struct {
	Debug bool
}

// DefaultTransformConfig returns a default configuration
func DefaultTransformConfig() *TransformConfig {
	debug := os.Getenv("ZETASQLITE_DEBUG") == "true"
	return &TransformConfig{
		Debug: debug,
	}
}
