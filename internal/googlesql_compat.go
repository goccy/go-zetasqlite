// Package-local shim layer that adapts go-googlesql's C++-style
// handle/factory API onto the free-function / visitor conventions
// go-zetasqlite originally wrote against the cgo-based go-zetasql.
// Keeping the shims here means we can regenerate go-googlesql
// freely (via wasmify) without touching call sites in this file.
package internal

import (
	"fmt"

	googlesql "github.com/goccy/go-googlesql"
)

// compatTypeFactory caches a single TypeFactory instance the shims
// use for the "new X type" helpers. The factory is created lazily on
// first use; googlesql.NewTypeFactory returns a handle whose lifetime
// is managed by the wasm module so holding onto it for the process
// lifetime is safe.
var compatTypeFactory *googlesql.TypeFactory

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

// StringType returns the canonical STRING handle produced by the
// shared TypeFactory. Mirrors zetasql.StringType().
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

// TypeFromKind returns a simple (primitive) type handle given a
// TypeKind constant. For composite kinds (ARRAY, STRUCT) this
// returns nil — callers who need those build them explicitly.
func TypeFromKind(k googlesql.TypeKind) googlesql.Googlesql_TypeNode {
	t, _ := tf().MakeSimpleType(k)
	return t
}

// NewArrayType wraps TypeFactory.MakeArrayType into a free function
// that returns the array type directly. Matches the former
// zetasql.NewArrayType signature.
func NewArrayType(elem googlesql.Googlesql_TypeNode) (googlesql.Googlesql_TypeNode, error) {
	var arr googlesql.ArrayType
	if err := tf().MakeArrayType(elem, &arr); err != nil {
		return nil, err
	}
	return &arr, nil
}

// StructField is a minimal record describing a struct field. The
// real googlesql type is produced on demand inside NewStructType.
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
// Field> which the generator can't marshal today). When that lands,
// this can dispatch to it. Until then return an error so callers
// surface the limitation rather than silently getting a nil handle.
func NewStructType(fields []*StructField) (googlesql.Googlesql_TypeNode, error) {
	return nil, fmt.Errorf("NewStructType: not yet supported via wasm bridge")
}

// NewFunction wraps googlesql's Function construction with a free-
// function signature matching the legacy zetasql.NewFunction.
//
// TODO: googlesql::Function has several overloaded constructors
// taking (name_path, group, mode, signatures, options). The proto/
// bridge generator currently filters them out because the signature
// includes a std::vector<FunctionSignature> the marshaller can't
// describe. Surface the limitation at runtime for now.
func NewFunction(namePath []string, group string, mode int, signatures interface{}, options interface{}) (*googlesql.Function, error) {
	return nil, fmt.Errorf("NewFunction: not yet supported via wasm bridge")
}

// NewTemplatedFunctionArgumentType constructs a FunctionArgumentType
// bound to a signature-argument kind. The original signature is
// (kind, options) — we preserve that via the generated
// googlesql.NewFunctionArgumentType taking a TypeNode, options, and
// numOccurrences triplet.
func NewTemplatedFunctionArgumentType(kind googlesql.SignatureArgumentKind, options *googlesql.FunctionArgumentTypeOptions) *googlesql.FunctionArgumentType {
	// The bridge's NewFunctionArgumentType wants a concrete Type,
	// not a SignatureArgumentKind. Signature-argument-kind templated
	// args can't be built via wasm today; return nil and let the
	// caller decide.
	_ = kind
	_ = options
	return nil
}

// NodeMap is a generic resolved-AST visitor helper. go-zetasqlite's
// original code used zetasql.NodeMap / NewNodeMap as a cache keyed
// on resolved node identity. Mirror that surface: backing store is
// a Go map keyed on the raw wasm pointer so equality semantics
// mirror the C++ pointer identity.
type NodeMap struct {
	m map[uint64]interface{}
}

// NewNodeMap returns an empty NodeMap.
func NewNodeMap() *NodeMap {
	return &NodeMap{m: make(map[uint64]interface{})}
}

// Get returns the value previously stored for node, if any.
func (n *NodeMap) Get(node interface{ rawPtr() uint64 }) (interface{}, bool) {
	v, ok := n.m[node.rawPtr()]
	return v, ok
}

// Set associates value with node.
func (n *NodeMap) Set(node interface{ rawPtr() uint64 }, value interface{}) {
	n.m[node.rawPtr()] = value
}

// ASTWalk traverses an ASTNode subtree depth-first, invoking visit
// on each node. Returns the first error the visitor produces.
//
// TODO: a full implementation needs generated child-iteration RPCs
// for every AST type. Until that lands the walker only visits the
// root so call sites compile and exercise their top-level logic.
func ASTWalk(root interface{}, visit func(node interface{}) error) error {
	return visit(root)
}

// ResolvedWalk is the resolved-AST counterpart to ASTWalk. Same
// limitations apply.
func ResolvedWalk(root interface{}, visit func(node interface{}) error) error {
	return visit(root)
}

// ResolvedBaseFunctionCallNode is the abstract parent of resolved
// function-call nodes. go-googlesql exposes the concrete
// ResolvedFunctionCallBase; the "Base" suffix in the alias mirrors
// go-zetasql's naming convention.
type ResolvedBaseFunctionCallNode = *googlesql.ResolvedFunctionCallBase

// Types that aren't declared by the current googlesql surface but
// which go-zetasqlite references lexically. Stubbed as empty
// structs so callsites that only take their address / store them in
// interfaces compile; runtime use of these specific variants is
// gated off by their absence in the resolved tree.

// ResolvedDropSearchIndexStmt is stubbed — no corresponding type
// exists in the current googlesql version exported by wasmify.
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

// ScalarMode mirrors an older zetasql enum. Exposed as int32 so
// call sites that do `zetasql.ScalarMode(x)` still typecheck.
type ScalarMode int32
