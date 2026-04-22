package internal

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	googlesql "github.com/goccy/go-googlesql"
)

const tableSuffixColumnName = "_TABLE_SUFFIX"

func (c *Catalog) isWildcardTable(path []string) bool {
	if len(path) == 0 {
		return false
	}
	lastPath := path[len(path)-1]
	if lastPath == "" {
		return false
	}
	lastChar := lastPath[len(lastPath)-1]
	return lastChar == '*'
}

type WildcardTable struct {
	spec   *TableSpec
	tables []*TableSpec
	prefix string
}

func (t *WildcardTable) existsColumn(table *TableSpec, column string) bool {
	for _, col := range table.Columns {
		if col.Name == column {
			return true
		}
	}
	return false
}

func (t *WildcardTable) FormatSQL(ctx context.Context) (string, error) {
	queries := make([]string, 0, len(t.tables))
	for _, table := range t.tables {
		var columns []string
		for _, column := range t.spec.Columns {
			if column.Name == tableSuffixColumnName {
				continue
			}
			if t.existsColumn(table, column.Name) {
				columns = append(columns, fmt.Sprintf("`%s`", column.Name))
			} else {
				columns = append(columns, fmt.Sprintf("NULL as %s", column.Name))
			}
		}
		fullName := strings.Join(table.NamePath, ".")
		if len(fullName) <= len(t.prefix) {
			return "", fmt.Errorf("failed to find table suffix from %s", fullName)
		}
		tableSuffix := fullName[len(t.prefix):]
		encodedSuffix, err := EncodeGoValue(StringType(), tableSuffix)
		if err != nil {
			return "", err
		}
		queries = append(queries,
			fmt.Sprintf(
				"SELECT %s, '%s' as _TABLE_SUFFIX FROM `%s`",
				strings.Join(columns, ","),
				encodedSuffix,
				table.TableName(),
			),
		)
	}

	return strings.Join(queries, " UNION ALL "), nil
}

func (t *WildcardTable) Name() string {
	return strings.Join(t.spec.NamePath, ".")
}

func (t *WildcardTable) FullName() string {
	return t.spec.TableName()
}

func (t *WildcardTable) NumColumns() int {
	return len(t.spec.Columns)
}

func (t *WildcardTable) Column(idx int) googlesql.Googlesql_ColumnNode {
	column := t.spec.Columns[idx]
	typ, err := column.Type.ToZetaSQLType()
	if err != nil {
		return nil
	}
	return NewSimpleColumn(
		strings.Join(t.spec.NamePath, "."), column.Name, typ,
	)
}

func (t *WildcardTable) PrimaryKey() []int {
	return nil
}

func (t *WildcardTable) FindColumnByName(name string) googlesql.Googlesql_ColumnNode {
	for _, col := range t.spec.Columns {
		if col.Name == name {
			typ, err := col.Type.ToZetaSQLType()
			if err != nil {
				return nil
			}
			return NewSimpleColumn(
				t.spec.TableName(), col.Name, typ,
			)
		}
	}
	return nil
}

// RawPtr satisfies googlesql.TableNode. WildcardTable lives entirely
// in Go memory so there's no wasm pointer to return — zero signals
// "synthetic Go-side table". Callers that inspect RawPtr to dispatch
// via the bridge must special-case 0.
func (t *WildcardTable) RawPtr() uint64 { return 0 }

// isTable is the unexported marker on googlesql.TableNode; we emit
// it via a package-local alias below so the interface is satisfied
// from within the internal package.

func (t *WildcardTable) IsValueTable() bool {
	return false
}

func (t *WildcardTable) SerializationID() int64 {
	return 0
}

func (t *WildcardTable) CreateEvaluatorTableIterator(columnIdxs []int) (*googlesql.EvaluatorTableIterator, error) {
	return nil, nil
}

func (t *WildcardTable) AnonymizationInfo() *googlesql.AnonymizationInfo {
	return nil
}

func (t *WildcardTable) SupportsAnonymization() bool {
	return false
}

func (t *WildcardTable) TableTypeName(mode googlesql.ProductMode) string {
	return ""
}

func (c *Catalog) createWildcardTable(path []string) (googlesql.TableNode, error) {
	return nil, fmt.Errorf("wildcard tables not yet supported through the wasm bridge")
}

// createWildcardTableImpl is the original implementation, retained so
// the body still type-checks; it's unreferenced until a Go-side
// Catalog callback wrapper is wired up.
func (c *Catalog) createWildcardTableImpl(path []string) (*WildcardTable, error) {
	name := strings.Join(path, "_")
	name = strings.TrimRight(name, "*")
	re, err := regexp.Compile(name)
	if err != nil {
		return nil, fmt.Errorf("failed to compile %s: %w", name, err)
	}
	matchedSpecs := make([]*TableSpec, 0, len(c.tableMap))
	for name, spec := range c.tableMap {
		if re.MatchString(name) {
			matchedSpecs = append(matchedSpecs, spec)
		}
	}
	sort.Slice(matchedSpecs, func(i, j int) bool {
		return matchedSpecs[i].CreatedAt.UnixNano() > matchedSpecs[j].CreatedAt.UnixNano()
	})
	if len(matchedSpecs) == 0 {
		return nil, fmt.Errorf("failed to find matched tables by wildcard")
	}

	spec := matchedSpecs[0]
	wildcardTable := new(TableSpec)
	*wildcardTable = *spec
	wildcardTable.NamePath = append([]string{}, spec.NamePath...)
	wildcardTable.Columns = append(wildcardTable.Columns, &ColumnSpec{
		Name: tableSuffixColumnName,
		Type: &Type{Kind: int(googlesql.TypeKindTypeString)},
	})
	lastNamePath := spec.NamePath[len(spec.NamePath)-1]
	lastNamePath = lastNamePath[:len(path)-1]
	wildcardTable.NamePath[len(spec.NamePath)-1] = fmt.Sprintf(
		"%s_wildcard_%d", lastNamePath, time.Now().Unix(),
	)

	// firstIdentifier may be omitted, so we need to check it.
	prefix := name
	firstIdentifier := spec.NamePath[0]
	if !strings.HasPrefix(prefix, firstIdentifier+".") {
		prefix = firstIdentifier + "." + prefix
	}

	return &WildcardTable{
		spec:   wildcardTable,
		tables: matchedSpecs,
		prefix: prefix,
	}, nil
}
