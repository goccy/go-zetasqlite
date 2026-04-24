package internal

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
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

// WildcardTable captures the set of real tables that a wildcard pattern
// expanded to, plus the merged column list the analyzer sees. The
// analyzer-visible handle is the embedded *SimpleTable (already a real
// wasm-side googlesql.TableNode) — WildcardTable itself never crosses the
// bridge. At SQL-emission time the formatter looks up the metadata by the
// SimpleTable's handle ptr and rewrites the scan into a UNION ALL.
type WildcardTable struct {
	spec   *TableSpec
	tables []*TableSpec
	prefix string
}

// wildcardTableRegistry maps a SimpleTable's wasm handle ptr to the
// WildcardTable metadata. googlesql.SimpleTable is the object the
// analyzer receives from the catalog; the formatter consults this
// registry when emitting SQL for a ResolvedTableScan to decide whether
// to expand the scan into a UNION across the real tables.
var (
	wildcardTableRegistryMu sync.RWMutex
	wildcardTableRegistry   = map[uint64]*WildcardTable{}
)

func registerWildcardTable(handle *googlesql.SimpleTable, wt *WildcardTable) {
	ptr := handleRawPtr(handle)
	if ptr == 0 {
		return
	}
	wildcardTableRegistryMu.Lock()
	defer wildcardTableRegistryMu.Unlock()
	wildcardTableRegistry[ptr] = wt
}

func lookupWildcardTable(table googlesql.TableNode) *WildcardTable {
	ptr := handleRawPtr(table)
	if ptr == 0 {
		return nil
	}
	wildcardTableRegistryMu.RLock()
	defer wildcardTableRegistryMu.RUnlock()
	return wildcardTableRegistry[ptr]
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

// createWildcardTable builds a *googlesql.SimpleTable covering the merged
// wildcard column list and registers the corresponding WildcardTable
// metadata so the formatter can rewrite scans into UNION queries.
func (c *Catalog) createWildcardTable(path []string) (googlesql.TableNode, error) {
	wt, err := c.createWildcardTableImpl(path)
	if err != nil {
		return nil, err
	}
	tableName := strings.Join(wt.spec.NamePath, ".")
	simpleTable, err := c.createSimpleTable(tableName, wt.spec)
	if err != nil {
		return nil, err
	}
	registerWildcardTable(simpleTable, wt)
	return simpleTable, nil
}

// createWildcardTableImpl builds the WildcardTable metadata (matched
// tables, merged column list, prefix) without touching the wasm-side
// catalog.
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
