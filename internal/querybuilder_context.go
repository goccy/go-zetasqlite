package internal

import (
	"context"
	"fmt"
	"sync"
)

// DefaultTransformContext provides a default implementation of TransformContext
type DefaultTransformContext struct {
	ctx              context.Context
	fragmentContext  FragmentContextProvider
	config           *TransformConfig
	scope            *ScopeManager
	withMappings     map[string][]string // WITH query name -> position-based column mappings
	recursiveCTEName string              // Current recursive CTE being defined
}

// NewDefaultTransformContext creates a new transform context
func NewDefaultTransformContext(ctx context.Context, config *TransformConfig) *DefaultTransformContext {
	return &DefaultTransformContext{
		ctx:             ctx,
		fragmentContext: NewDefaultFragmentContext(),
		config:          config,
		scope:           NewScopeManager(),
		withMappings:    make(map[string][]string),
	}
}

// Context returns the underlying Go context
func (c *DefaultTransformContext) Context() context.Context {
	return c.ctx
}

// FragmentContext returns the fragment context provider
func (c *DefaultTransformContext) FragmentContext() FragmentContextProvider {
	return c.fragmentContext
}

// Config returns the transformation configuration
func (c *DefaultTransformContext) Config() *TransformConfig {
	return c.config
}

// WithFragmentContext returns a new context with updated fragment context
func (c *DefaultTransformContext) WithFragmentContext(fc FragmentContextProvider) TransformContext {
	newCtx := *c // Copy
	newCtx.fragmentContext = fc
	return &newCtx
}

// AddWithEntryColumnMapping adds column mappings for a WITH query
func (c *DefaultTransformContext) AddWithEntryColumnMapping(name string, columns []*ColumnData) {
	mapping := make([]string, 0, len(columns))
	for _, col := range columns {
		mapping = append(mapping, generateIDBasedAlias(col.Name, col.ID))
	}
	c.withMappings[name] = mapping
}

// GetWithEntryMapping retrieves column mappings for a WITH query
func (c *DefaultTransformContext) GetWithEntryMapping(name string) []string {
	if mapping, exists := c.withMappings[name]; exists {
		return mapping
	}
	return nil
}

// SetRecursiveCTEName sets the name of the CTE currently being recursively defined
func (c *DefaultTransformContext) SetRecursiveCTEName(name string) {
	c.recursiveCTEName = name
}

// GetRecursiveCTEName returns the name of the CTE currently being recursively defined
func (c *DefaultTransformContext) GetRecursiveCTEName() string {
	return c.recursiveCTEName
}

// ColumnInfo stores metadata about available columns
type ColumnInfo struct {
	Name       string
	Type       string
	TableAlias string
	Expression *SQLExpression
	ID         int
}

// DefaultFragmentContext provides fragment context functionality
type DefaultFragmentContext struct {
	mu               sync.RWMutex
	columnMap        map[int]*SQLExpression // Column ID -> Expression
	availableColumns map[int]*ColumnInfo    // Column ID -> ColumnInfo
	columnData       map[int]*ColumnData    // Column ID -> ColumnData for reverse lookup
	scopes           []ScopeToken
	count            int
	columnIDToScope  map[int]string // Maps column ID (like "A.id#1") to scope alias
}

// NewDefaultFragmentContext creates a new fragment context
func NewDefaultFragmentContext() *DefaultFragmentContext {
	return &DefaultFragmentContext{
		columnMap:        make(map[int]*SQLExpression),
		availableColumns: make(map[int]*ColumnInfo),
		columnData:       make(map[int]*ColumnData),
		scopes:           make([]ScopeToken, 0),
		columnIDToScope:  make(map[int]string),
	}
}

// AddAvailableColumn adds a column to the available columns map
func (fc *DefaultFragmentContext) AddAvailableColumn(columnID int, info *ColumnInfo) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	// Store by column ID for proper disambiguation
	fc.availableColumns[columnID] = info

	// Create column expression if info has one
	if info.Expression != nil {
		fc.columnMap[columnID] = info.Expression
	} else {
		// Create default column reference
		fc.columnMap[columnID] = &SQLExpression{
			Type:  ExpressionTypeColumn,
			Value: info.Name,
		}
	}
}

// AddAvailableColumnsForDML When transforming the columns for the base table of a DML statement,
// do not use aliases, instead use the underlying SQLite column names
func (fc *DefaultFragmentContext) AddAvailableColumnsForDML(scanData *ScanData) {
	for _, col := range scanData.ColumnList {
		// Register scope alias
		fc.RegisterColumnScope(col.ID, scanData.TableScan.TableName)

		// Add to available columns
		fc.AddAvailableColumn(col.ID, &ColumnInfo{
			Name:       col.Name, // Use simple name, not qualified
			Expression: NewColumnExpression(col.Name),
		})
	}
}

func (fc *DefaultFragmentContext) GetID() string {
	if len(fc.scopes) == 0 {
		return "0"
	}
	return fc.scopes[len(fc.scopes)-1].ID()
}

// EnterScope enters a new scope
func (fc *DefaultFragmentContext) EnterScope() ScopeToken {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.count += 1
	token := &DefaultScopeToken{
		id:        fc.count,
		startSize: len(fc.columnMap),
	}

	fc.scopes = append(fc.scopes, token)
	return token
}

// ExitScope exits the current scope
func (fc *DefaultFragmentContext) ExitScope(token ScopeToken) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	// Remove scopes until we find the matching one
	for i := len(fc.scopes) - 1; i >= 0; i-- {
		if fc.scopes[i].ID() == token.ID() {
			fc.scopes = fc.scopes[:i]
			break
		}
	}
}

// GetQualifiedColumnRef returns the qualified column reference for a column ID
func (fc *DefaultFragmentContext) GetQualifiedColumnRef(columnID int) (string, string) {
	fc.mu.RLock()
	defer fc.mu.RUnlock()

	if scopeAlias, exists := fc.columnIDToScope[columnID]; exists {
		info := fc.availableColumns[columnID]
		if info == nil {
			return "", ""
		}
		// Return column name and scope alias as table alias
		return info.Name, scopeAlias
	}

	panic(fmt.Sprintf("column id %d not found in fragment context. transformers must add columns before getting qualified refs", columnID))
}

func (fc *DefaultFragmentContext) GetQualifiedColumnExpression(columnID int) *SQLExpression {
	name, _ := fc.GetQualifiedColumnRef(columnID)
	return NewColumnExpression(name)
}

// RegisterColumnScope registers a mapping from column ID to scope alias
func (fc *DefaultFragmentContext) RegisterColumnScope(columnID int, scopeAlias string) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.columnIDToScope[columnID] = scopeAlias
}

// RegisterColumnScopeMapping registers scope mappings for a list of columns
func (fc *DefaultFragmentContext) RegisterColumnScopeMapping(scopeAlias string, columns []*ColumnData) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	for _, col := range columns {
		fc.columnIDToScope[col.ID] = scopeAlias
		fc.availableColumns[col.ID] = &ColumnInfo{
			Name: generateIDBasedAlias(col.Name, col.ID),
			ID:   col.ID,
		}
	}
}

// generateIDBasedAlias creates a unique column alias using the column ID
func generateIDBasedAlias(columnName string, columnID int) string {
	return fmt.Sprintf("%s__%d", columnName, columnID)
}

// DefaultScopeToken implements ScopeToken
type DefaultScopeToken struct {
	id        int
	startSize int
}

// ID returns the scope identifier
func (t *DefaultScopeToken) ID() string {
	return fmt.Sprintf("%d", t.id)
}

// ScopeManager manages nested scopes
type ScopeManager struct {
	mu     sync.RWMutex
	count  int
	scopes []*Scope
}

// Scope represents a context scope
type Scope struct {
	ID        int
	Variables map[string]*SQLExpression
	Parent    *Scope
}

// NewScopeManager creates a new scope manager
func NewScopeManager() *ScopeManager {
	return &ScopeManager{
		count:  0,
		scopes: make([]*Scope, 0),
	}
}

// EnterScope enters a new scope
func (sm *ScopeManager) EnterScope() *Scope {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var parent *Scope
	if len(sm.scopes) > 0 {
		parent = sm.scopes[len(sm.scopes)-1]
	}

	sm.count += 1
	scope := &Scope{
		ID:        int(sm.count),
		Variables: make(map[string]*SQLExpression),
		Parent:    parent,
	}

	sm.scopes = append(sm.scopes, scope)
	return scope
}

// ExitScope exits the current scope
func (sm *ScopeManager) ExitScope() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(sm.scopes) > 0 {
		sm.scopes = sm.scopes[:len(sm.scopes)-1]
	}
}

// CurrentScope returns the current scope
func (sm *ScopeManager) CurrentScope() *Scope {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if len(sm.scopes) == 0 {
		return nil
	}

	return sm.scopes[len(sm.scopes)-1]
}

// TransformResult represents the result of a transformation
type TransformResult struct {
	Fragment SQLFragment
}

// NewTransformResult creates a new transform result
func NewTransformResult(fragment SQLFragment) *TransformResult {
	return &TransformResult{
		Fragment: fragment,
	}
}
