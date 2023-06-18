package internal

import (
	"fmt"
	"strings"
)

type NamePath struct {
	path   []string
	maxNum int
}

func (p *NamePath) normalizePath(path []string) []string {
	ret := []string{}
	for _, p := range path {
		splitted := strings.Split(p, ".")
		ret = append(ret, splitted...)
	}
	return ret
}

func (p *NamePath) mergePath(path []string) []string {
	path = p.normalizePath(path)
	if p.maxNum > 0 && len(path) == p.maxNum {
		return path
	}
	if len(path) == 0 {
		return p.path
	}
	merged := []string{}
	for _, basePath := range p.path {
		if path[0] == basePath {
			break
		}
		merged = append(merged, basePath)
	}
	return append(merged, path...)
}

func (p *NamePath) format(path []string) string {
	return formatPath(p.mergePath(path))
}

func formatPath(path []string) string {
	return strings.Join(path, "_")
}

func (p *NamePath) setPath(path []string) error {
	normalizedPath := p.normalizePath(path)
	if p.maxNum > 0 && len(normalizedPath) > p.maxNum {
		return fmt.Errorf("specified too many name paths %v(%d). max name path is %d", path, len(normalizedPath), p.maxNum)
	}
	p.path = normalizedPath
	return nil
}

func (p *NamePath) addPath(path string) error {
	normalizedPath := p.normalizePath([]string{path})
	totalPath := len(p.path) + len(normalizedPath)
	if p.maxNum > 0 && totalPath > p.maxNum {
		return fmt.Errorf(
			"specified too many name paths %v(%d). max name path is %d",
			append(p.path, normalizedPath...),
			totalPath,
			p.maxNum,
		)
	}
	p.path = append(p.path, normalizedPath...)
	return nil
}

func (p *NamePath) empty() bool {
	return len(p.path) == 0
}
