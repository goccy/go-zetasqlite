package internal

import (
	"fmt"
	"strings"

	"github.com/goccy/go-json"
)

type NamePath struct {
	path   []string
	maxNum int
}

func (p *NamePath) Clone() *NamePath {
	return &NamePath{path: append([]string{}, p.path...)}
}

func (p *NamePath) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.path)
}

func (p *NamePath) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &p.path)
}

func (p *NamePath) isInformationSchema(path []string) bool {
	if len(path) == 0 {
		return false
	}
	// If INFORMATION_SCHEMA is at the end of path, ignore it.
	for _, subPath := range path[:len(path)-1] {
		if strings.EqualFold(subPath, "information_schema") {
			return true
		}
	}
	return false
}

func (p *NamePath) setMaxNum(num int) {
	if num > 0 {
		p.maxNum = num
	}
}

func (p *NamePath) getMaxNum(path []string) int {
	if p.maxNum == 0 {
		return 0
	}
	// INFORMATION_SCHEMA is a special View.
	// This means that one path is added to the rules for specifying a normal Table/Function.
	if p.isInformationSchema(path) {
		return p.maxNum + 1
	}
	return p.maxNum
}

func (p *NamePath) normalizePath(path []string) []string {
	ret := []string{}
	for _, p := range path {
		splitted := strings.Split(p, ".")
		ret = append(ret, splitted...)
	}
	return ret
}

func (p *NamePath) mergePath(path []string) *NamePath {
	path = p.normalizePath(path)
	maxNum := p.getMaxNum(path)
	if maxNum > 0 && p.hasMaxComponents(path) {
		return &NamePath{path: path}
	}
	if len(path) == 0 {
		return &NamePath{path: p.path}
	}
	merged := []string{}
	for _, basePath := range p.path {
		if path[0] == basePath {
			break
		}
		if maxNum > 0 && len(merged)+len(path) >= maxNum {
			break
		}
		merged = append(merged, basePath)
	}
	return &NamePath{path: append(merged, path...)}
}

func (p *NamePath) format(path []string) string {
	mergedPath := p.mergePath(path)
	return mergedPath.CatalogPath()
}

func (p *NamePath) dropFirst() *NamePath {
	if p.Empty() {
		return p
	}
	return &NamePath{path: p.path[1:]}
}

func (p *NamePath) dropLast() *NamePath {
	if p.Empty() {
		return p
	}
	return &NamePath{path: p.path[:len(p.path)-1]}
}

func (p *NamePath) CatalogPath() string {
	return strings.Join(p.path, "_")
}

func (p *NamePath) FormatNamePath() string {
	if p.HasFullyQualifiedName() {
		return fmt.Sprintf("%s.%s", p.GetProjectId(), strings.Join(p.path[1:], "_"))
	}
	return strings.Join(p.path, "_")
}

func (p *NamePath) Path() []string {
	return p.path
}

func (p *NamePath) setPath(path []string) error {
	normalizedPath := p.normalizePath(path)
	maxNum := p.getMaxNum(path)
	if maxNum > 0 && len(normalizedPath) > maxNum {
		return fmt.Errorf("specified too many name paths %v(%d). max name path is %d", path, len(normalizedPath), maxNum)
	}
	p.path = normalizedPath
	return nil
}

func (p *NamePath) addPath(path string) error {
	normalizedPath := p.normalizePath([]string{path})
	totalPath := len(p.path) + len(normalizedPath)
	maxNum := p.getMaxNum(normalizedPath)
	if maxNum > 0 && totalPath > maxNum {
		return fmt.Errorf(
			"specified too many name paths %v(%d). max name path is %d",
			append(p.path, normalizedPath...),
			totalPath,
			maxNum,
		)
	}
	p.path = append(p.path, normalizedPath...)
	return nil
}

func (p *NamePath) replace(index int, value string) {
	p.path[index] = value
}

func (p *NamePath) Length() int {
	return len(p.path)
}

func (p *NamePath) Empty() bool {
	return p.Length() == 0
}

func (p *NamePath) GetCatalogId() string {
	if p.Length() < 2 {
		return ""
	}
	return p.path[0]
}

func (p *NamePath) GetProjectId() string {
	if p.Length() < 3 {
		return ""
	}
	return p.path[0]
}

func (p *NamePath) GetDatasetId() string {
	if p.Length() < 2 {
		return ""
	} else if p.Length() == 2 {
		return p.path[0]
	} else {
		return p.path[1]
	}
}

func (p *NamePath) GetObjectId() string {
	if p.Empty() {
		return ""
	}
	return p.path[p.Length()-1]
}

func (p *NamePath) HasSimpleName() bool {
	return p.Length() == 1
}

func (p *NamePath) HasQualifiers() bool {
	return p.Length() > 1
}

func (p *NamePath) HasFullyQualifiedName() bool {
	return p.Length() > 2
}

func (p *NamePath) hasMaxComponents(path []string) bool {
	maxNum := p.getMaxNum(path)
	return maxNum > 0 && len(path) == maxNum
}

func NewNamePath(path []string) *NamePath {
	return &NamePath{path: path}
}
