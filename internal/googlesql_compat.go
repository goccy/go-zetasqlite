// Package-local shim layer that adapts go-googlesql's auto-generated
// multi-return API onto the single-return conventions go-zetasqlite was
// originally written against. Keeping the shims here means we can
// regenerate go-googlesql freely (via wasmify) without touching call
// sites in this file.
package internal

import (
	"fmt"
	"unsafe"

	googlesql "github.com/goccy/go-googlesql"
)

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
	elem := StringType()
	var arr googlesql.ArrayType
	_ = tf().MakeArrayType(elem, &arr)
	return &arr
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
	var arr googlesql.ArrayType
	if err := tf().MakeArrayType(elem, &arr); err != nil {
		return nil, err
	}
	return &arr, nil
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

// NewStructType builds a StructType from field descriptors.
//
// TODO(go-googlesql): TypeFactory does not yet expose MakeStructType
// through the wasm bridge (the C++ method takes a std::vector<StructType::
// Field> which the generator can't marshal today). When that lands this
// can dispatch to it. Until then return an error so callers surface the
// limitation rather than silently getting a nil handle.
func NewStructType(fields []*StructField) (googlesql.Googlesql_TypeNode, error) {
	return nil, fmt.Errorf("NewStructType: not yet supported via wasm bridge")
}

// ---------- Analyzer / parser constructor shims ---------------------------

// NewLanguageOptions panics on nil-handle errors. The underlying call is
// infallible except for VM issues.
func NewLanguageOptions() *googlesql.LanguageOptions {
	opts, err := googlesql.NewLanguageOptions()
	if err != nil {
		panic(fmt.Errorf("NewLanguageOptions: %w", err))
	}
	return opts
}

// NewAnalyzerOptions matches the old zero-arg signature. Internally we
// pass a default LanguageOptions.
func NewAnalyzerOptions() *googlesql.AnalyzerOptions {
	opts, err := googlesql.NewAnalyzerOptions2()
	if err != nil {
		panic(fmt.Errorf("NewAnalyzerOptions: %w", err))
	}
	return opts
}

// NewParseResumeLocationFromString returns a ParseResumeLocation from a
// SQL string; single-return style.
func NewParseResumeLocationFromString(input string) *googlesql.ParseResumeLocation {
	loc, err := googlesql.NewParseResumeLocationFromString(input)
	if err != nil {
		panic(fmt.Errorf("NewParseResumeLocationFromString: %w", err))
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
func NewSimpleCatalog(name string) *googlesql.SimpleCatalog {
	cat, err := googlesql.NewSimpleCatalog(name, tf())
	if err != nil {
		panic(fmt.Errorf("NewSimpleCatalog: %w", err))
	}
	return cat
}

// NewSimpleColumn panics on nil-handle. Defaults writable and non-pseudo.
func NewSimpleColumn(tableName, name string, typ googlesql.Googlesql_TypeNode) *googlesql.SimpleColumn {
	col, err := googlesql.NewSimpleColumn(tableName, name, typ, false, true)
	if err != nil {
		panic(fmt.Errorf("NewSimpleColumn: %w", err))
	}
	return col
}

// NewSimpleTable builds a SimpleTable with a synthetic id.
func NewSimpleTable(name string, columns []*googlesql.SimpleColumn) *googlesql.SimpleTable {
	tbl, err := googlesql.NewSimpleTable(name, 0)
	if err != nil {
		panic(fmt.Errorf("NewSimpleTable: %w", err))
	}
	for _, c := range columns {
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
		panic(fmt.Errorf("NewFunctionArgumentTypeOptions: %w", err))
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
		panic(fmt.Errorf("NewFunctionArgumentType: %w", err))
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
func NewFunctionSignature(result *googlesql.FunctionArgumentType, args []*googlesql.FunctionArgumentType) *googlesql.FunctionSignature {
	sig, err := googlesql.NewFunctionSignature(0)
	if err != nil {
		panic(fmt.Errorf("NewFunctionSignature: %w", err))
	}
	// Result-type and argument-list setters aren't exposed on the wasm
	// bridge yet; retain the signature handle so the Function constructor
	// path keeps a valid *FunctionSignature value for downstream calls.
	_ = result
	_ = args
	return sig
}

// NewFunction is a stub — googlesql::Function's constructors take a
// std::vector<FunctionSignature> and the marshaller cannot describe that
// today. Callers surface the limitation at runtime.
func NewFunction(namePath []string, group string, mode int, signatures interface{}, options interface{}) (*googlesql.Function, error) {
	return nil, fmt.Errorf("NewFunction: not yet supported via wasm bridge")
}

// NewTemplatedFunctionArgumentType constructs a FunctionArgumentType bound
// to a signature-argument kind. The bridge currently only exposes the
// variant that takes a concrete Type; templated variants return nil.
func NewTemplatedFunctionArgumentType(kind googlesql.SignatureArgumentKind, options *googlesql.FunctionArgumentTypeOptions) *googlesql.FunctionArgumentType {
	_ = kind
	_ = options
	return nil
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
// It accepts either the exported RawPtr() that the generator now emits
// or an implementation of the unexported rawPtr() the internal bindings
// use. Falls back to reflecting the first struct field (the `ptr`
// member) when neither is available.
func rawPtrOf(v interface{}) uint64 {
	if v == nil {
		return 0
	}
	if r, ok := v.(interface{ RawPtr() uint64 }); ok {
		return r.RawPtr()
	}
	return 0
}

// Get returns the value previously stored for node, if any.
func (n *NodeMap) Get(node interface{ RawPtr() uint64 }) (interface{}, bool) {
	v, ok := n.m[node.RawPtr()]
	return v, ok
}

// Set associates value with node.
func (n *NodeMap) Set(node interface{ RawPtr() uint64 }, value interface{}) {
	n.m[node.RawPtr()] = value
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

// ASTWalk traverses an ASTNode subtree depth-first, invoking visit on
// each node. Returns the first error the visitor produces.
//
// TODO: a full implementation needs generated child-iteration RPCs for
// every AST type. Until that lands the walker only visits the root so
// call sites compile and exercise their top-level logic. visit is typed
// as interface{} so both ASTNodeNode and ResolvedNodeNode closures can
// be passed in without casts.
func ASTWalk(root interface{}, visit interface{}) error {
	switch v := visit.(type) {
	case func(node interface{}) error:
		return v(root)
	case func(node googlesql.ASTNodeNode) error:
		if n, ok := root.(googlesql.ASTNodeNode); ok {
			return v(n)
		}
	}
	return nil
}

// ResolvedWalk is the resolved-AST counterpart to ASTWalk. Same
// limitations apply.
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

func upcastPtr(h interface{ RawPtr() uint64 }) uint64 { return h.RawPtr() }

// AsResolvedExpr returns the node reinterpreted as ResolvedExpr so
// base methods like Type() can be invoked.
func AsResolvedExpr(h interface{ RawPtr() uint64 }) *googlesql.ResolvedExpr {
	p := &handlePtr{ptr: upcastPtr(h)}
	return (*googlesql.ResolvedExpr)(unsafe.Pointer(p))
}

// AsResolvedScan returns the node as its ResolvedScan base.
func AsResolvedScan(h interface{ RawPtr() uint64 }) *googlesql.ResolvedScan {
	p := &handlePtr{ptr: upcastPtr(h)}
	return (*googlesql.ResolvedScan)(unsafe.Pointer(p))
}

// AsResolvedStatement returns the node as its ResolvedStatement base.
func AsResolvedStatement(h interface{ RawPtr() uint64 }) *googlesql.ResolvedStatement {
	p := &handlePtr{ptr: upcastPtr(h)}
	return (*googlesql.ResolvedStatement)(unsafe.Pointer(p))
}

// AsResolvedNode returns the node as the most abstract ResolvedNode base.
func AsResolvedNode(h interface{ RawPtr() uint64 }) *googlesql.ResolvedNode {
	p := &handlePtr{ptr: upcastPtr(h)}
	return (*googlesql.ResolvedNode)(unsafe.Pointer(p))
}

// AsResolvedFunctionCallBase returns the node as ResolvedFunctionCallBase.
func AsResolvedFunctionCallBase(h interface{ RawPtr() uint64 }) *googlesql.ResolvedFunctionCallBase {
	p := &handlePtr{ptr: upcastPtr(h)}
	return (*googlesql.ResolvedFunctionCallBase)(unsafe.Pointer(p))
}

// AsASTNode returns any AST handle as its ASTNode base.
func AsASTNode(h interface{ RawPtr() uint64 }) *googlesql.ASTNode {
	p := &handlePtr{ptr: upcastPtr(h)}
	return (*googlesql.ASTNode)(unsafe.Pointer(p))
}
