package internal

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestNamePath(t *testing.T) {
	namePath := new(NamePath)
	if err := namePath.setPath([]string{"project1", "dataset1"}); err != nil {
		t.Fatal(err)
	}
	namePath.setMaxNum(3)
	if diff := cmp.Diff(namePath.mergePath([]string{"project1", "dataset1", "table1"}).Path(), []string{"project1", "dataset1", "table1"}); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(namePath.mergePath([]string{"dataset1", "table1"}).Path(), []string{"project1", "dataset1", "table1"}); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(namePath.mergePath([]string{"project2", "dataset2", "table1"}).Path(), []string{"project2", "dataset2", "table1"}); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(namePath.mergePath([]string{"dataset2", "table1"}).Path(), []string{"project1", "dataset2", "table1"}); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(namePath.mergePath([]string{"table1"}).Path(), []string{"project1", "dataset1", "table1"}); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(namePath.mergePath([]string{"project2", "dataset2", "INFORMATION_SCHEMA", "TABLES"}).Path(), []string{"project2", "dataset2", "INFORMATION_SCHEMA", "TABLES"}); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(namePath.mergePath([]string{"dataset2", "INFORMATION_SCHEMA", "TABLES"}).Path(), []string{"project1", "dataset2", "INFORMATION_SCHEMA", "TABLES"}); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(namePath.mergePath([]string{"INFORMATION_SCHEMA", "TABLES"}).Path(), []string{"project1", "dataset1", "INFORMATION_SCHEMA", "TABLES"}); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}
