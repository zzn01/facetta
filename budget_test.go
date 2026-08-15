package facetta

import (
	"errors"
	"math/rand"
	"testing"
)

// budgetStore seeds a store with the given records at a fixed MaxScanRows.
func budgetStore(t *testing.T, budget int, recs []Record) *Store {
	t.Helper()
	s, err := New(testSchema(), Config{MaxScanRows: budget})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.seed(recs); err != nil {
		t.Fatal(err)
	}
	return s
}

// TestBudgetRefusal: a query whose candidate span exceeds the budget is refused
// with ErrScanBudget, the stats counter increments, and dst comes back nil.
func TestBudgetRefusal(t *testing.T) {
	recs := baseRecords() // 4 base rows, all source=s1 except one
	s := budgetStore(t, 2, recs)
	before := s.Stats().ScanBudgetRefusals
	// source=s1 matches a 3-row candidate interval (>2 budget).
	got, err := s.Query(nil, []Cond{{Dim: "source", Value: "s1"}})
	if !errors.Is(err, ErrScanBudget) {
		t.Fatalf("err = %v, want ErrScanBudget", err)
	}
	if got != nil {
		t.Fatalf("dst = %v, want nil on refusal", got)
	}
	if s.Stats().ScanBudgetRefusals != before+1 {
		t.Fatalf("refusal not counted: %d, want %d", s.Stats().ScanBudgetRefusals, before+1)
	}
}

// TestBudgetUnderBudgetIdentical: a query under budget returns exactly what an
// unbudgeted store returns.
func TestBudgetUnderBudgetIdentical(t *testing.T) {
	recs := baseRecords()
	budgeted := budgetStore(t, 1_000_000, recs)
	unbounded := seededStore(t, recs) // Config{} => MaxScanRows 0

	cases := [][][]Cond{
		{{{Dim: "source", Value: "s1"}, {Dim: "account", Value: "a1"}}},
		{{{Dim: "source", Value: "s1"}}},
		{{{Dim: "os", Value: "ios"}}}, // full scan, but budget is huge
	}
	for i, groups := range cases {
		want, err := unbounded.Query(nil, groups...)
		if err != nil {
			t.Fatalf("case %d unbounded: %v", i, err)
		}
		got, err := budgeted.Query(nil, groups...)
		if err != nil {
			t.Fatalf("case %d budgeted: %v", i, err)
		}
		assertSame(t, got, want)
	}
}

// TestBudgetFullScanRefused: a full scan is refused under a tight budget but the
// same query succeeds with MaxScanRows == 0.
func TestBudgetFullScanRefused(t *testing.T) {
	recs := baseRecords() // os=ios is a non-prefix cond => full scan of 4 rows
	tight := budgetStore(t, 3, recs)
	if _, err := tight.Query(nil, []Cond{{Dim: "os", Value: "ios"}}); !errors.Is(err, ErrScanBudget) {
		t.Fatalf("full scan under budget: err = %v, want ErrScanBudget", err)
	}
	unbounded := seededStore(t, recs)
	if _, err := unbounded.Query(nil, []Cond{{Dim: "os", Value: "ios"}}); err != nil {
		t.Fatalf("full scan with MaxScanRows=0: %v", err)
	}
}

// TestBudgetOverlapDedup pins the dedup arithmetic: overlapping/nested candidate
// intervals are counted ONCE, matching the sweep's actual visited-row count.
// baseRecords sorts to rows: 0=s1a1p1, 1=s1a1p2, 2=s1a2p1, 3=s2a2p1.
//
//	group {source=s1}            -> rows [0,3)  (3 rows)
//	group {source=s1,account=a1} -> rows [0,2)  (nested)
//
// Union visits exactly 3 rows. Budget=3 must pass; budget=2 must refuse.
func TestBudgetOverlapDedup(t *testing.T) {
	recs := baseRecords()
	groups := [][]Cond{
		{{Dim: "source", Value: "s1"}},
		{{Dim: "source", Value: "s1"}, {Dim: "account", Value: "a1"}},
	}
	if _, err := budgetStore(t, 3, recs).Query(nil, groups...); err != nil {
		t.Fatalf("budget == visited (3): err = %v, want success", err)
	}
	if _, err := budgetStore(t, 2, recs).Query(nil, groups...); !errors.Is(err, ErrScanBudget) {
		t.Fatalf("budget == visited-1 (2): err = %v, want ErrScanBudget", err)
	}
}

// TestBudgetDeltaCounts: delta rows count toward the budget. Base contributes 0
// candidate rows for a delta-only value, so the budget must still trip on delta.
func TestBudgetDeltaCounts(t *testing.T) {
	s := budgetStore(t, 3, baseRecords())
	// Apply 4 delta rows; delta scan is always linear over all delta rows.
	extra := []Record{
		rec(ts(500), []float64{1, 1}, "s9", "a9", "p9", "JP", "web"),
		rec(ts(500), []float64{1, 1}, "s9", "a9", "p8", "JP", "web"),
		rec(ts(500), []float64{1, 1}, "s9", "a9", "p7", "JP", "web"),
		rec(ts(500), []float64{1, 1}, "s9", "a9", "p6", "JP", "web"),
	}
	if err := s.Apply(extra); err != nil {
		t.Fatal(err)
	}
	// A query with no base candidates (unknown value) still scans 4 delta rows.
	if _, err := s.Query(nil, []Cond{{Dim: "source", Value: "s9"}}); !errors.Is(err, ErrScanBudget) {
		t.Fatalf("delta rows not counted toward budget: err = %v, want ErrScanBudget", err)
	}
	// With budget covering the 4 delta rows it succeeds.
	s2 := budgetStore(t, 100, baseRecords())
	if err := s2.Apply(extra); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Query(nil, []Cond{{Dim: "source", Value: "s9"}}); err != nil {
		t.Fatalf("delta query under ample budget: %v", err)
	}
}

// TestBudgetZeroAlloc: both the allowed and the refused paths are alloc-free.
func TestBudgetZeroAlloc(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	recs := make([]Record, 0, 10000)
	for range 10000 {
		recs = append(recs, randomRecord(rng, 5))
	}
	groups := [][]Cond{{{Dim: "source", Value: "s1"}}}

	// Allowed path: ample budget.
	allow := budgetStore(t, 1_000_000, recs)
	buf := make([]float64, 0, 2)
	if a := testing.AllocsPerRun(100, func() {
		buf, _ = allow.QueryGroups(buf, groups)
	}); a != 0 {
		t.Fatalf("allowed path allocates: %v allocs/op", a)
	}

	// Refused path: budget of 1 trips on the source=s1 candidate span.
	refuse := budgetStore(t, 1, recs)
	if a := testing.AllocsPerRun(100, func() {
		if _, err := refuse.QueryGroups(buf, groups); !errors.Is(err, ErrScanBudget) {
			panic("expected ErrScanBudget")
		}
	}); a != 0 {
		t.Fatalf("refused path allocates: %v allocs/op", a)
	}
}
