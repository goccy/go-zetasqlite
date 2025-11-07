package internal

import (
	"testing"
)

func TestMergeWithClauses(t *testing.T) {
	t.Run("both empty", func(t *testing.T) {
		result := mergeWithClauses(nil, nil)
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})

	t.Run("outer empty", func(t *testing.T) {
		inner := []*WithClause{
			{Name: "inner_cte", Recursive: false},
		}
		result := mergeWithClauses(nil, inner)
		if len(result) != 1 {
			t.Fatalf("expected 1 clause, got %d", len(result))
		}
		if result[0].Name != "inner_cte" {
			t.Errorf("expected 'inner_cte', got '%s'", result[0].Name)
		}
	})

	t.Run("inner empty", func(t *testing.T) {
		outer := []*WithClause{
			{Name: "outer_cte", Recursive: false},
		}
		result := mergeWithClauses(outer, nil)
		if len(result) != 1 {
			t.Fatalf("expected 1 clause, got %d", len(result))
		}
		if result[0].Name != "outer_cte" {
			t.Errorf("expected 'outer_cte', got '%s'", result[0].Name)
		}
	})

	t.Run("no conflicts - different names", func(t *testing.T) {
		outer := []*WithClause{
			{Name: "outer_cte1", Recursive: false},
			{Name: "outer_cte2", Recursive: false},
		}
		inner := []*WithClause{
			{Name: "inner_cte1", Recursive: false},
			{Name: "inner_cte2", Recursive: false},
		}
		result := mergeWithClauses(outer, inner)
		if len(result) != 4 {
			t.Fatalf("expected 4 clauses, got %d", len(result))
		}

		// Verify inner CTEs come first
		if result[0].Name != "inner_cte1" {
			t.Errorf("expected 'inner_cte1' at position 0, got '%s'", result[0].Name)
		}
		if result[1].Name != "inner_cte2" {
			t.Errorf("expected 'inner_cte2' at position 1, got '%s'", result[1].Name)
		}

		// Verify outer CTEs come after
		names := make(map[string]bool)
		for _, clause := range result {
			names[clause.Name] = true
		}
		if !names["outer_cte1"] || !names["outer_cte2"] {
			t.Errorf("missing outer CTEs in result")
		}
	})

	t.Run("name conflict - inner wins", func(t *testing.T) {
		outer := []*WithClause{
			{Name: "shared_cte", Recursive: false, Materialized: false},
		}
		inner := []*WithClause{
			{Name: "shared_cte", Recursive: true, Materialized: true},
		}
		result := mergeWithClauses(outer, inner)

		if len(result) != 1 {
			t.Fatalf("expected 1 clause (inner should win), got %d", len(result))
		}

		// Verify inner CTE was kept (it's recursive and materialized)
		if result[0].Name != "shared_cte" {
			t.Errorf("expected 'shared_cte', got '%s'", result[0].Name)
		}
		if !result[0].Recursive {
			t.Errorf("expected inner CTE (Recursive=true) to win, got Recursive=%v", result[0].Recursive)
		}
		if !result[0].Materialized {
			t.Errorf("expected inner CTE (Materialized=true) to win, got Materialized=%v", result[0].Materialized)
		}
	})

	t.Run("partial conflict", func(t *testing.T) {
		outer := []*WithClause{
			{Name: "outer_only", Recursive: false},
			{Name: "shared", Recursive: false},
		}
		inner := []*WithClause{
			{Name: "shared", Recursive: true},
			{Name: "inner_only", Recursive: false},
		}
		result := mergeWithClauses(outer, inner)

		if len(result) != 3 {
			t.Fatalf("expected 3 clauses, got %d", len(result))
		}

		names := make(map[string]*WithClause)
		for _, clause := range result {
			names[clause.Name] = clause
		}

		// Verify all expected CTEs are present
		if _, ok := names["outer_only"]; !ok {
			t.Errorf("missing 'outer_only' in result")
		}
		if _, ok := names["inner_only"]; !ok {
			t.Errorf("missing 'inner_only' in result")
		}
		if _, ok := names["shared"]; !ok {
			t.Errorf("missing 'shared' in result")
		}

		// Verify inner version of 'shared' was kept
		if !names["shared"].Recursive {
			t.Errorf("expected inner version of 'shared' (Recursive=true), got Recursive=%v", names["shared"].Recursive)
		}
	})

	t.Run("multiple conflicts", func(t *testing.T) {
		outer := []*WithClause{
			{Name: "cte1", Recursive: false},
			{Name: "cte2", Recursive: false},
			{Name: "cte3", Recursive: false},
		}
		inner := []*WithClause{
			{Name: "cte1", Recursive: true},
			{Name: "cte2", Recursive: true},
			{Name: "cte3", Recursive: true},
		}
		result := mergeWithClauses(outer, inner)

		if len(result) != 3 {
			t.Fatalf("expected 3 clauses (all conflicts resolved to inner), got %d", len(result))
		}

		// Verify all inner CTEs were kept
		for _, clause := range result {
			if !clause.Recursive {
				t.Errorf("expected all inner CTEs (Recursive=true), got clause '%s' with Recursive=%v", clause.Name, clause.Recursive)
			}
		}
	})

	t.Run("preserves order - inner first, then non-conflicting outer", func(t *testing.T) {
		outer := []*WithClause{
			{Name: "a_outer", Recursive: false},
			{Name: "b_outer", Recursive: false},
		}
		inner := []*WithClause{
			{Name: "c_inner", Recursive: false},
			{Name: "d_inner", Recursive: false},
		}
		result := mergeWithClauses(outer, inner)

		if len(result) != 4 {
			t.Fatalf("expected 4 clauses, got %d", len(result))
		}

		// Verify inner CTEs come first in their original order
		if result[0].Name != "c_inner" {
			t.Errorf("expected 'c_inner' at position 0, got '%s'", result[0].Name)
		}
		if result[1].Name != "d_inner" {
			t.Errorf("expected 'd_inner' at position 1, got '%s'", result[1].Name)
		}

		// Outer CTEs come after inner CTEs
		if result[2].Name != "a_outer" {
			t.Errorf("expected 'a_outer' at position 2, got '%s'", result[2].Name)
		}
		if result[3].Name != "b_outer" {
			t.Errorf("expected 'b_outer' at position 3, got '%s'", result[3].Name)
		}
	})
}