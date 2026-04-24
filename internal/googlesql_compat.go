// Package-local shim layer that adapts go-googlesql's auto-generated
// multi-return API onto the single-return conventions go-zetasqlite was
// originally written against. Keeping the shims here means we can
// regenerate go-googlesql freely (via wasmify) without touching call
// sites in this file.
package internal

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	googlesql "github.com/goccy/go-googlesql"
)

// handleRawPtr extracts the wasm-side C++ pointer from a googlesql handle.
// The go-googlesql generator no longer exports RawPtr (to keep the public
// API clean), but the internal layout is always either
//   { ptr uint64 } (root handle with no C++ base) or
//   { *Base }      (handle that embeds its C++ parent).
// This helper walks the embedded chain with reflection until it hits the
// root struct's unexported `ptr` field and reads it via unsafe.Pointer.
func handleRawPtr(h any) uint64 {
	if h == nil {
		return 0
	}
	v := reflect.ValueOf(h)
	for {
		for v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return 0
			}
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			return 0
		}
		if f := v.FieldByName("ptr"); f.IsValid() && f.Kind() == reflect.Uint64 {
			return *(*uint64)(unsafe.Pointer(f.UnsafeAddr()))
		}
		descended := false
		for i := 0; i < v.NumField(); i++ {
			sf := v.Type().Field(i)
			if !sf.Anonymous {
				continue
			}
			fv := v.Field(i)
			if fv.Kind() == reflect.Ptr {
				v = fv
				descended = true
				break
			}
		}
		if !descended {
			return 0
		}
	}
}

// ---------- Type-factory shims --------------------------------------------

var compatTypeFactory *googlesql.TypeFactory

// tf returns a process-wide TypeFactory that's lazily created on first use.
// Lifetime is managed by the wasm module so holding onto it for the process
// lifetime is safe.
func tf() *googlesql.TypeFactory {
	if compatTypeFactory == nil {
		f, err := googlesql.NewTypeFactory()
		if err != nil {
			panic(fmt.Errorf("NewTypeFactory: %w", err))
		}
		compatTypeFactory = f
	}
	return compatTypeFactory
}

// StringType returns the canonical STRING handle produced by the shared
// TypeFactory. Mirrors zetasql.StringType().
func StringType() googlesql.Googlesql_TypeNode {
	t, _ := tf().MakeSimpleType(googlesql.TypeKindTypeString)
	return t
}

// StringArrayType returns ARRAY<STRING>.
func StringArrayType() googlesql.Googlesql_TypeNode {
	arr, err := tf().MakeArrayType(StringType())
	if err != nil || arr == nil {
		return nil
	}
	return arr
}

// TypeFromKind returns a simple (primitive) type handle given a TypeKind
// constant. For composite kinds (ARRAY, STRUCT) this returns nil —
// callers who need those build them explicitly.
func TypeFromKind(k googlesql.TypeKind) googlesql.Googlesql_TypeNode {
	t, _ := tf().MakeSimpleType(k)
	return t
}

// NewArrayType wraps TypeFactory.MakeArrayType into a free function that
// returns the array type directly. Matches the former zetasql.NewArrayType
// signature.
func NewArrayType(elem googlesql.Googlesql_TypeNode) (googlesql.Googlesql_TypeNode, error) {
	arr, err := tf().MakeArrayType(elem)
	if err != nil {
		return nil, err
	}
	return arr, nil
}

// StructField is a minimal record describing a struct field. The real
// googlesql type is produced on demand inside NewStructType.
type StructField struct {
	Name string
	Type googlesql.Googlesql_TypeNode
}

// NewStructField allocates a StructField; analogous to
// zetasql.NewStructField(name, type).
func NewStructField(name string, t googlesql.Googlesql_TypeNode) *StructField {
	return &StructField{Name: name, Type: t}
}

// NewStructType builds a StructType from field descriptors via the
// bridge-exposed TypeFactory.MakeStructType. Each StructField is
// converted to the googlesql proto struct and the resulting struct
// type handle is returned.
func NewStructType(fields []*StructField) (googlesql.Googlesql_TypeNode, error) {
	bridgeFields := make([]*googlesql.StructField, 0, len(fields))
	for _, f := range fields {
		if f == nil {
			continue
		}
		bridgeFields = append(bridgeFields, &googlesql.StructField{
			Name:  f.Name,
			Type_: f.Type,
		})
	}
	st, err := tf().MakeStructType(bridgeFields)
	if err != nil {
		return nil, err
	}
	return st, nil
}

// ---------- Analyzer / parser constructor shims ---------------------------

// NewLanguageOptions returns a fresh LanguageOptions, or nil on a
// wasm-level failure. Callers must tolerate a nil handle — downstream
// setters will short-circuit.
func NewLanguageOptions() *googlesql.LanguageOptions {
	opts, err := googlesql.NewLanguageOptions()
	if err != nil {
		return nil
	}
	return opts
}

// NewAnalyzerOptions matches the old zero-arg signature. Returns nil on
// wasm failure.
func NewAnalyzerOptions() *googlesql.AnalyzerOptions {
	opts, err := googlesql.NewAnalyzerOptions2()
	if err != nil {
		return nil
	}
	return opts
}

// NewParseResumeLocationFromString returns a ParseResumeLocation from a
// SQL string. On error returns nil rather than panicking so a single
// malformed query cannot tear down the whole test runner.
func NewParseResumeLocationFromString(input string) *googlesql.ParseResumeLocation {
	loc, err := googlesql.NewParseResumeLocationFromString(input)
	if err != nil {
		return nil
	}
	return loc
}

// ---------- Analyzer shims -----------------------------------------------

// AnalyzeStatement wraps the new 4-arg form with the old 3-arg signature
// used by go-zetasqlite.
func AnalyzeStatement(query string, catalog *Catalog, opt *googlesql.AnalyzerOptions) (*googlesql.AnalyzerOutput, error) {
	return googlesql.AnalyzeStatement(query, opt, catalog.catalog, tf())
}

// AnalyzeStatementFromParserAST wraps the new form that reorders params
// and requires a type factory.
func AnalyzeStatementFromParserAST(query string, stmt googlesql.ASTStatementNode, catalog *Catalog, opt *googlesql.AnalyzerOptions) (*googlesql.AnalyzerOutput, error) {
	return googlesql.AnalyzeStatementFromParserAST(stmt, opt, query, catalog.catalog, tf())
}

// ---------- SimpleCatalog shims -------------------------------------------

// NewSimpleCatalog builds a SimpleCatalog bound to the shared TypeFactory.
// Returns nil on wasm failure so callers can handle it gracefully.
func NewSimpleCatalog(name string) *googlesql.SimpleCatalog {
	cat, err := googlesql.NewSimpleCatalog(name, tf())
	if err != nil {
		return nil
	}
	return cat
}

// NewSimpleColumn returns nil if the bridge call fails — callers must
// check. Defaults writable and non-pseudo.
func NewSimpleColumn(tableName, name string, typ googlesql.Googlesql_TypeNode) *googlesql.SimpleColumn {
	col, err := googlesql.NewSimpleColumn(tableName, name, typ, false, true)
	if err != nil {
		return nil
	}
	return col
}

// NewSimpleTable builds a SimpleTable with a synthetic id. Returns nil
// if the bridge call fails so a single wasm trap cannot tear down the
// whole test binary.
func NewSimpleTable(name string, columns []*googlesql.SimpleColumn) *googlesql.SimpleTable {
	tbl, err := googlesql.NewSimpleTable(name, 0)
	if err != nil {
		return nil
	}
	for _, c := range columns {
		if c == nil {
			continue
		}
		_ = tbl.AddColumn2(c, true)
	}
	return tbl
}

// ---------- Function signature shims -------------------------------------

// NewFunctionArgumentTypeOptions builds FunctionArgumentTypeOptions with
// the given cardinality applied. The new API takes no args and exposes a
// setter.
func NewFunctionArgumentTypeOptions(cardinality googlesql.FunctionEnums_ArgumentCardinality) *googlesql.FunctionArgumentTypeOptions {
	opts, err := googlesql.NewFunctionArgumentTypeOptions()
	if err != nil {
		return nil
	}
	// Cardinality setter isn't exposed on the wasm bridge yet; callers
	// that relied on setting cardinality at construction time will get
	// the default. Track this via the unused param so the signature
	// stays compatible with existing zetasqlite call sites.
	_ = cardinality
	return opts
}

// NewFunctionArgumentType constructs a FunctionArgumentType for a typed
// argument. Num occurrences defaults to -1 (meaning "use cardinality").
func NewFunctionArgumentType(typ googlesql.Googlesql_TypeNode, opts *googlesql.FunctionArgumentTypeOptions) *googlesql.FunctionArgumentType {
	fat, err := googlesql.NewFunctionArgumentType(typ, opts, -1)
	if err != nil {
		return nil
	}
	return fat
}

// compatSignatureArguments returns an empty slice in place of
// googlesql.FunctionSignature.Arguments(), which isn't yet exposed on
// the wasm bridge. Call sites use the argument list for templated
// function inference; returning empty makes those code paths fall
// through to non-templated handling, which is acceptable for the
// basic analyze/format flow we're chasing compile-green for.
func compatSignatureArguments(_ *googlesql.FunctionSignature) []*googlesql.FunctionArgumentType {
	return nil
}

// NewFunctionSignature wraps the new one-arg constructor. The old two-arg
// form is no longer representable at the bridge layer; the args that used
// to be passed to the constructor are instead set via dedicated setters
// on the Signature. Only the context_id argument survives.
//
// Returns nil on wasm failure so a single malformed signature does not
// tear down the whole test binary; callers must tolerate a nil handle.
func NewFunctionSignature(result *googlesql.FunctionArgumentType, args []*googlesql.FunctionArgumentType) *googlesql.FunctionSignature {
	sig, err := googlesql.NewFunctionSignature(0)
	if err != nil {
		return nil
	}
	// Result-type and argument-list setters aren't exposed on the wasm
	// bridge yet; retain the signature handle so the Function constructor
	// path keeps a valid *FunctionSignature value for downstream calls.
	_ = result
	_ = args
	return sig
}

// NewFunction constructs a googlesql Function and attaches each
// provided signature via the bridge-exposed AddSignature accessor. The
// upstream C++ constructors take a std::vector<FunctionSignature> which
// the bridge still cannot marshal directly; AddSignature is the
// canonical alternative and keeps the resulting Function semantically
// identical.
func NewFunction(namePath []string, group string, mode int, signatures interface{}, options interface{}) (*googlesql.Function, error) {
	name := ""
	if len(namePath) > 0 {
		name = strings.Join(namePath, ".")
	}
	var fnOptions *googlesql.FunctionOptions
	if options != nil {
		if fo, ok := options.(*googlesql.FunctionOptions); ok {
			fnOptions = fo
		}
	}
	fn, err := googlesql.NewFunction(name, group, googlesql.FunctionEnums_Mode(mode), fnOptions)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, fmt.Errorf("NewFunction returned nil handle")
	}
	if sigs, ok := signatures.([]*googlesql.FunctionSignature); ok {
		for _, sig := range sigs {
			if sig == nil {
				continue
			}
			if err := fn.AddSignature(sig); err != nil {
				return nil, fmt.Errorf("Function.AddSignature: %w", err)
			}
		}
	}
	return fn, nil
}

// NewTemplatedFunctionArgumentType constructs a FunctionArgumentType bound
// to a signature-argument kind. The bridge currently only exposes the
// variant that takes a concrete Type; templated variants return nil.
func NewTemplatedFunctionArgumentType(kind googlesql.SignatureArgumentKind, options *googlesql.FunctionArgumentTypeOptions) *googlesql.FunctionArgumentType {
	_ = kind
	_ = options
	return nil
}

// ---------- Nested-enum accessor stubs -----------------------------------
//
// The wasmify generator currently drops methods whose return type is a
// C++ nested enum (e.g. `ResolvedCreateStatement::CreateScope`) because
// the proto layer wraps those enums in `<Class>Enums::` and the type
// resolver doesn't yet follow that alias. Until that lands, provide
// zero-value fallbacks so call sites compile; runtime behaviour for
// these specific paths is degraded but the common analyze/format flow
// doesn't hit them.

// ResolvedCreateStatementCreateScope returns the scope (DEFAULT / TEMP /
// PRIVATE / PUBLIC) of any ResolvedCreateStatement-derived node. All
// CREATE-family resolved nodes embed *ResolvedCreateStatement, so the
// CreateScope() accessor is method-promoted onto them; the interface
// check below picks it up regardless of the concrete subtype.
func ResolvedCreateStatementCreateScope(h interface{}) googlesql.ResolvedCreateStatementEnums_CreateScope {
	if h == nil {
		return googlesql.ResolvedCreateStatementEnums_CreateScopeCreateDefaultScope
	}
	type scopeGetter interface {
		CreateScope() (googlesql.ResolvedCreateStatementEnums_CreateScope, error)
	}
	if g, ok := h.(scopeGetter); ok {
		if v, err := g.CreateScope(); err == nil {
			return v
		}
	}
	return googlesql.ResolvedCreateStatementEnums_CreateScopeCreateDefaultScope
}

// ResolvedCreateStatementIsTempFromQuery returns true when the raw SQL
// text carries a TEMP / TEMPORARY keyword on the CREATE statement.
// Workaround until the generator exposes CreateScope() as an accessor
// on ResolvedCreateStatement (nested-enum return type).
func ResolvedCreateStatementIsTempFromQuery(query string) bool {
	q := strings.ToUpper(query)
	// Strip leading whitespace before matching.
	q = strings.TrimLeft(q, " \t\r\n")
	if !strings.HasPrefix(q, "CREATE") {
		return false
	}
	rest := strings.TrimLeft(q[len("CREATE"):], " \t\r\n")
	// Optional OR REPLACE.
	if strings.HasPrefix(rest, "OR REPLACE") {
		rest = strings.TrimLeft(rest[len("OR REPLACE"):], " \t\r\n")
	}
	return strings.HasPrefix(rest, "TEMP TABLE") ||
		strings.HasPrefix(rest, "TEMPORARY TABLE") ||
		strings.HasPrefix(rest, "TEMP FUNCTION") ||
		strings.HasPrefix(rest, "TEMPORARY FUNCTION") ||
		strings.HasPrefix(rest, "TEMP VIEW") ||
		strings.HasPrefix(rest, "TEMPORARY VIEW")
}

// ResolvedCreateStatementCreateMode returns the CREATE mode (DEFAULT /
// OR_REPLACE / IF_NOT_EXISTS) of any ResolvedCreateStatement-derived
// node via method-promoted CreateMode() accessor.
func ResolvedCreateStatementCreateMode(h interface{}) googlesql.ResolvedCreateStatementEnums_CreateMode {
	if h == nil {
		return googlesql.ResolvedCreateStatementEnums_CreateModeCreateDefault
	}
	type modeGetter interface {
		CreateMode() (googlesql.ResolvedCreateStatementEnums_CreateMode, error)
	}
	if g, ok := h.(modeGetter); ok {
		if v, err := g.CreateMode(); err == nil {
			return v
		}
	}
	return googlesql.ResolvedCreateStatementEnums_CreateModeCreateDefault
}

// ResolvedMergeWhenMatchType reads the MATCHED / NOT MATCHED kind from the
// resolved node via the bridge-exposed accessor.
func ResolvedMergeWhenMatchType(h *googlesql.ResolvedMergeWhen) googlesql.ResolvedMergeWhenEnums_MatchType {
	if h == nil {
		return 0
	}
	v, err := h.MatchTypeMethod()
	if err != nil {
		return 0
	}
	return v
}

// ResolvedMergeWhenActionType reads the INSERT / UPDATE / DELETE kind from
// the resolved node.
func ResolvedMergeWhenActionType(h *googlesql.ResolvedMergeWhen) googlesql.ResolvedMergeWhenEnums_ActionType {
	if h == nil {
		return 0
	}
	v, err := h.ActionTypeMethod()
	if err != nil {
		return 0
	}
	return v
}

// ResolvedJoinScanJoinType returns the concrete join kind from the resolved
// scan. Relies on the bridge-exposed JoinTypeMethod accessor.
func ResolvedJoinScanJoinType(h googlesql.ResolvedJoinScanNode) googlesql.ResolvedJoinScanEnums_JoinType {
	if h == nil {
		return 0
	}
	t, err := h.JoinTypeMethod()
	if err != nil {
		return 0
	}
	return t
}

// ResolvedSetOperationScanOpType reads the concrete set-op kind
// (UNION / INTERSECT / EXCEPT, all / distinct variants).
func ResolvedSetOperationScanOpType(h googlesql.ResolvedSetOperationScanNode) googlesql.ResolvedSetOperationScanEnums_SetOperationType {
	if h == nil {
		return 0
	}
	v, err := h.OpType()
	if err != nil {
		return 0
	}
	return v
}

// ResolvedOrderByItemNullOrder reads the null-order specification.
func ResolvedOrderByItemNullOrder(h *googlesql.ResolvedOrderByItem) googlesql.ResolvedOrderByItemEnums_NullOrderMode {
	if h == nil {
		return 0
	}
	v, err := h.NullOrder()
	if err != nil {
		return 0
	}
	return v
}

// ResolvedAggregateFunctionCallNullHandlingModifier reads the promoted
// base-class accessor NullHandlingModifierMethod.
func ResolvedAggregateFunctionCallNullHandlingModifier(h googlesql.ResolvedAggregateFunctionCallNode) googlesql.ResolvedNonScalarFunctionCallBaseEnums_NullHandlingModifier {
	if h == nil {
		return 0
	}
	v, err := h.NullHandlingModifierMethod()
	if err != nil {
		return 0
	}
	return v
}

// ResolvedAnalyticFunctionCallNullHandlingModifier is the analytic-call
// counterpart of the aggregate accessor.
func ResolvedAnalyticFunctionCallNullHandlingModifier(h googlesql.ResolvedAnalyticFunctionCallNode) googlesql.ResolvedNonScalarFunctionCallBaseEnums_NullHandlingModifier {
	if h == nil {
		return 0
	}
	v, err := h.NullHandlingModifierMethod()
	if err != nil {
		return 0
	}
	return v
}

// ResolvedWindowFrameFrameUnit reads ROWS / RANGE.
func ResolvedWindowFrameFrameUnit(h *googlesql.ResolvedWindowFrame) googlesql.ResolvedWindowFrameEnums_FrameUnit {
	if h == nil {
		return 0
	}
	v, err := h.FrameUnitMethod()
	if err != nil {
		return 0
	}
	return v
}

// ResolvedWindowFrameExprBoundaryType reads the UNBOUNDED /
// CURRENT ROW / OFFSET PRECEDING/FOLLOWING boundary kind.
func ResolvedWindowFrameExprBoundaryType(h googlesql.ResolvedWindowFrameExprNode) googlesql.ResolvedWindowFrameExprEnums_BoundaryType {
	if h == nil {
		return 0
	}
	v, err := h.BoundaryTypeMethod()
	if err != nil {
		return 0
	}
	return v
}

// ResolvedFunctionCallBaseErrorMode reads the error-mode modifier
// (DEFAULT / SAFE_ERROR_MODE).
func ResolvedFunctionCallBaseErrorMode(h *googlesql.ResolvedFunctionCallBase) googlesql.ResolvedFunctionCallBaseEnums_ErrorMode {
	if h == nil {
		return 0
	}
	v, err := h.ErrorMode()
	if err != nil {
		return 0
	}
	return v
}

// ResolvedSubqueryExprSubqueryType reads the subquery kind
// (SCALAR / ARRAY / EXISTS / IN).
func ResolvedSubqueryExprSubqueryType(h googlesql.ResolvedSubqueryExprNode) googlesql.ResolvedSubqueryExprEnums_SubqueryType {
	if h == nil {
		return 0
	}
	v, err := h.SubqueryType()
	if err != nil {
		return 0
	}
	return v
}

// ---------- Node walking / map -------------------------------------------

// NodeMap is a generic resolved-AST visitor helper. The original code used
// zetasql.NodeMap / NewNodeMap as a cache keyed on resolved node identity.
// Mirror that surface: backing store is a Go map keyed on the raw wasm
// pointer so equality semantics mirror C++ pointer identity.
type NodeMap struct {
	m map[uint64]interface{}
}

// NewNodeMap returns an empty NodeMap. The variadic args match the old
// (root-resolved, root-parsed) shape — both are accepted and ignored
// since lookup is always by node identity.
func NewNodeMap(_ ...interface{}) *NodeMap {
	return &NodeMap{m: make(map[uint64]interface{})}
}

// rawPtrOf extracts the opaque wasm pointer from any googlesql handle.
// Delegates to handleRawPtr which reads the unexported ptr via reflection
// (the generator no longer exports RawPtr()).
func rawPtrOf(v interface{}) uint64 {
	return handleRawPtr(v)
}

// Get returns the value previously stored for node, if any.
func (n *NodeMap) Get(node any) (interface{}, bool) {
	v, ok := n.m[handleRawPtr(node)]
	return v, ok
}

// Set associates value with node.
func (n *NodeMap) Set(node any, value interface{}) {
	n.m[handleRawPtr(node)] = value
}

// FindNodeFromResolvedNode looks up the parser AST nodes previously
// recorded against a resolved node. Returns a slice to match the
// zetasql.NodeMap.FindNodeFromResolvedNode shape; in practice we only
// ever store at most one parser node per resolved key, so the slice
// is either empty or single-element.
func (n *NodeMap) FindNodeFromResolvedNode(node interface{}) []googlesql.ASTNodeNode {
	ptr := rawPtrOf(node)
	if ptr == 0 {
		return nil
	}
	v, ok := n.m[ptr]
	if !ok {
		return nil
	}
	if astNode, ok := v.(googlesql.ASTNodeNode); ok {
		return []googlesql.ASTNodeNode{astNode}
	}
	return nil
}

// ASTWalk traverses an ASTNode subtree depth-first via the bridge's
// Child(i)/NumChildren() accessors, invoking visit on each node.
// Returns the first error the visitor produces. visit is typed as
// interface{} so call sites can keep the old
//
//	func(n ASTNodeNode) error
//
// closure shape from go-zetasql.
func ASTWalk(root interface{}, visit interface{}) error {
	astRoot, _ := root.(googlesql.ASTNodeNode)
	if astRoot == nil {
		return nil
	}
	invoke := func(n googlesql.ASTNodeNode) error {
		switch v := visit.(type) {
		case func(node interface{}) error:
			return v(n)
		case func(node googlesql.ASTNodeNode) error:
			return v(n)
		}
		return nil
	}
	var walk func(n googlesql.ASTNodeNode) error
	walk = func(n googlesql.ASTNodeNode) error {
		if n == nil {
			return nil
		}
		if err := invoke(n); err != nil {
			return err
		}
		num, err := n.NumChildren()
		if err != nil {
			return nil
		}
		for i := int32(0); i < num; i++ {
			child, err := n.Child(i)
			if err != nil || child == nil {
				continue
			}
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(astRoot)
}

// ResolvedWalk is the resolved-AST counterpart to ASTWalk. The bridge
// does not yet expose child-iteration on ResolvedNode, so for now we
// only invoke visit on the root.
// TODO: wire this up when ResolvedNode.ChildrenAccept is surfaced
// through an exported visitor callback.
func ResolvedWalk(root interface{}, visit interface{}) error {
	switch v := visit.(type) {
	case func(node interface{}) error:
		return v(root)
	case func(n googlesql.ResolvedNodeNode) error:
		if n, ok := root.(googlesql.ResolvedNodeNode); ok {
			return v(n)
		}
	}
	return nil
}

// ---------- Stubbed types -------------------------------------------------

// ResolvedBaseFunctionCallNode is the abstract parent of resolved
// function-call nodes. It's a pointer alias so call sites written
// for the old zetasql API (which spelled it `ResolvedBaseFunctionCallNode`
// and took its address) still compile.
type ResolvedBaseFunctionCallNode = googlesql.ResolvedFunctionCallBase

// ResolvedDropSearchIndexStmt is stubbed — no corresponding type exists
// in the current googlesql version exported by wasmify.
type ResolvedDropSearchIndexStmt struct{}

// ResolvedDropSearchIndexStmtNode is its interface counterpart.
type ResolvedDropSearchIndexStmtNode = *ResolvedDropSearchIndexStmt

// ResolvedLetExpr is stubbed.
type ResolvedLetExpr struct{}

// ResolvedLetExprNode is its interface counterpart.
type ResolvedLetExprNode = *ResolvedLetExpr

// ResolvedUpdateArrayItem is stubbed.
type ResolvedUpdateArrayItem struct{}

// ResolvedUpdateArrayItemNode is its interface counterpart.
type ResolvedUpdateArrayItemNode = *ResolvedUpdateArrayItem

// ScalarMode mirrors an older zetasql enum. Exposed as int32 so call
// sites that do `zetasql.ScalarMode(x)` still typecheck.
type ScalarMode int32

// ---------- Enum-kind aliases --------------------------------------------

// ResolvedNodeKindResolvedTVFScan and friends: old zetasql exposed a few
// kinds with all-caps abbreviations. The wasmify-generated names use
// camel-cased identifier segments (Tvfscan, Dmlvalue, Dmldefault). Alias
// the legacy name so call sites don't need to change.
const (
	ResolvedNodeKindResolvedTVFScan    = googlesql.ResolvedNodeKindResolvedTvfscan
	ResolvedNodeKindResolvedDMLValue   = googlesql.ResolvedNodeKindResolvedDmlvalue
	ResolvedNodeKindResolvedDMLDefault = googlesql.ResolvedNodeKindResolvedDmldefault
)

// ---------- Multi-return → single-value helpers --------------------------

// mustNode accepts a (node, err) pair returned by googlesql accessor
// methods and adapts it to the Formatter-returning `newNode` that the
// analyzer and formatter packages expect. Errors from accessors are
// dropped — they only fire when the handle pointer is zero, which in
// the resolved-AST flow means "field was unset" and the caller is
// already prepared to handle nil. Generic so the compiler lets
// subclass-interfaces through (ResolvedScanNode etc.) without an
// explicit cast at the call site.
func mustNode[T googlesql.ResolvedNodeNode](n T, _ error) googlesql.ResolvedNodeNode {
	return n
}

// nn is a forgiving wrapper around newNode that accepts either a bare
// ResolvedNodeNode or a (ResolvedNodeNode, error) pair — whichever the
// googlesql accessor happens to return.
func nn(args ...interface{}) googlesql.ResolvedNodeNode {
	switch len(args) {
	case 1:
		if v, ok := args[0].(googlesql.ResolvedNodeNode); ok {
			return v
		}
	case 2:
		if v, ok := args[0].(googlesql.ResolvedNodeNode); ok {
			return v
		}
	}
	return nil
}

// m1 drops the error from a (T, error) pair returned by googlesql
// accessor methods. Used to adapt multi-return accessors to single-value
// expression contexts where the error is known to be nil in practice.
func m1[T any](v T, _ error) T { return v }

// ---------- Upcast helpers -----------------------------------------------
//
// The wasmify-generated Go bindings don't carry inherited methods onto
// derived concrete types (e.g. ResolvedParameter doesn't expose Type()
// even though its C++ ancestor ResolvedExpr does). Each generated
// handle struct has the same layout — a single uint64 `ptr` field —
// so we can reinterpret the pointer to call a base-class method. The
// bridge dispatches by (service_id, method_id) and doesn't care which
// exact C++ subclass the handle points at, so reading the same ptr as
// a base handle is safe.

type handlePtr struct{ ptr uint64 }

// asBase finds the embedded *T base inside any googlesql handle h. Every
// derived handle has its base-chain stored via named embedding (`*Base`),
// so walking the struct graph via reflection is deterministic. Returns
// nil when h is nil or doesn't have *T anywhere in its chain.
func asBase[T any](h any) *T {
	if h == nil {
		return nil
	}
	if p, ok := h.(*T); ok {
		return p
	}
	targetName := reflect.TypeOf((*T)(nil)).Elem().Name()
	v := reflect.ValueOf(h)
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	// FieldByName walks promoted fields of embedded structs automatically.
	f := v.FieldByName(targetName)
	if f.IsValid() && f.Kind() == reflect.Ptr {
		if p, ok := f.Interface().(*T); ok {
			return p
		}
	}
	return nil
}

// AsResolvedExpr returns the node reinterpreted as ResolvedExpr so
// base methods like Type() can be invoked.
func AsResolvedExpr(h any) *googlesql.ResolvedExpr {
	return asBase[googlesql.ResolvedExpr](h)
}

// AsResolvedScan returns the node as its ResolvedScan base.
func AsResolvedScan(h any) *googlesql.ResolvedScan {
	return asBase[googlesql.ResolvedScan](h)
}

// AsResolvedStatement returns the node as its ResolvedStatement base.
func AsResolvedStatement(h any) *googlesql.ResolvedStatement {
	return asBase[googlesql.ResolvedStatement](h)
}

// AsResolvedNode returns the node as the most abstract ResolvedNode base.
func AsResolvedNode(h any) *googlesql.ResolvedNode {
	return asBase[googlesql.ResolvedNode](h)
}

// AsResolvedFunctionCallBase returns the node as ResolvedFunctionCallBase.
func AsResolvedFunctionCallBase(h any) *googlesql.ResolvedFunctionCallBase {
	return asBase[googlesql.ResolvedFunctionCallBase](h)
}

// AsASTNode returns any AST handle as its ASTNode base.
func AsASTNode(h any) *googlesql.ASTNode {
	return asBase[googlesql.ASTNode](h)
}
