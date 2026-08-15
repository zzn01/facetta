package facetta

import (
	"errors"
	"testing"
	"time"
)

func TestCompactMergesDelta(t *testing.T) {
	recs := baseRecords()
	s := seededStore(t, recs)
	rt := newRefTable(testSchema())
	rt.apply(recs)
	ups := []Record{
		rec(ts(500), []float64{100, 10}, "s1", "a1", "p1", "US", "ios"),
		rec(ts(500), []float64{7, 0.7}, "s9", "a9", "p9", "JP", "web"),
	}
	if err := s.Apply(ups); err != nil {
		t.Fatal(err)
	}
	rt.apply(ups)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if s.DeltaRows() != 0 {
		t.Fatalf("delta not drained: %d", s.DeltaRows())
	}
	if s.Rows() != 5 {
		t.Fatalf("rows = %d, want 5", s.Rows())
	}
	for _, groups := range [][][]Cond{
		{{{Dim: "source", Value: "s1"}}},
		{{{Dim: "source", Value: "s9"}}},
		{{}},
	} {
		got, err := s.QueryGroups(nil, groups)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, got, rt.query(groups))
	}
	if s.Stats().Compactions != 1 {
		t.Fatal("compaction not counted")
	}
	if !s.SyncPosition().Equal(ts(500)) {
		t.Fatalf("position lost after compaction: %v", s.SyncPosition())
	}
}

func TestCompactTTLEviction(t *testing.T) {
	s, err := New(testSchema(), Config{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(10000, 0).UTC()
	s.now = func() time.Time { return now }
	// seed applies TTL too, so make both rows fresh first
	old := rec(now.Add(-2*time.Hour), []float64{1, 1}, "s1", "a1", "p1", "US", "ios")
	fresh := rec(now.Add(-time.Minute), []float64{2, 2}, "s2", "a2", "p2", "DE", "web")
	old2 := old
	old2.UpdatedAt = now.Add(-time.Minute)
	if err := s.seed([]Record{old2, fresh}); err != nil {
		t.Fatal(err)
	}
	if s.Rows() != 2 {
		t.Fatal("seed failed")
	}
	// age the first row by moving the clock, then compact
	now = now.Add(2 * time.Hour)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if s.Rows() != 0 {
		// both rows are now older than 1h TTL
		t.Fatalf("rows = %d, want 0 after TTL compaction", s.Rows())
	}
}

func TestRowCapRefusesCompaction(t *testing.T) {
	s, err := New(testSchema(), Config{MaxRows: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.seed(baseRecords()); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply([]Record{rec(ts(500), []float64{1, 1}, "s9", "a9", "p9", "JP", "web")}); err != nil {
		t.Fatal(err)
	}
	err = s.Compact()
	if !errors.Is(err, ErrRowLimit) {
		t.Fatalf("err = %v, want ErrRowLimit", err)
	}
	// old view keeps serving: base rows unchanged, delta still visible
	if s.Rows() != 4 || s.DeltaRows() != 1 {
		t.Fatalf("rows=%d delta=%d, old view lost", s.Rows(), s.DeltaRows())
	}
	if s.Stats().CompactionFailures != 1 {
		t.Fatal("failure not counted")
	}
	// ReplaceAll over the cap also refuses
	six := append(baseRecords(),
		rec(ts(600), []float64{1, 1}, "s8", "a8", "p8", "JP", "web"),
		rec(ts(600), []float64{1, 1}, "s9", "a9", "p9", "JP", "web"))
	if err := s.ReplaceAll(six); !errors.Is(err, ErrRowLimit) {
		t.Fatalf("ReplaceAll err = %v, want ErrRowLimit", err)
	}
}

// TestCapBlockedGatesIngestion covers the backpressure path: once a compaction is
// refused over MaxRows, Apply drops brand-new tuples (counted in DroppedOverCap)
// but still lands updates to existing tuples; a successful ReplaceAll under the
// cap clears the flag and new tuples flow again.
func TestCapBlockedGatesIngestion(t *testing.T) {
	s, err := New(testSchema(), Config{MaxRows: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.seed(baseRecords()); err != nil {
		t.Fatal(err)
	}
	// push delta over the cap, then a refused compaction sets capBlocked.
	if err := s.Apply([]Record{rec(ts(500), []float64{1, 1}, "s9", "a9", "p9", "JP", "web")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); !errors.Is(err, ErrRowLimit) {
		t.Fatalf("Compact err = %v, want ErrRowLimit", err)
	}
	if !s.isCapBlocked() {
		t.Fatal("cap not blocked after refused compaction")
	}

	// (a) a brand-new tuple is dropped: delta unchanged, stat incremented.
	deltaBefore := s.DeltaRows()
	if err := s.Apply([]Record{rec(ts(600), []float64{5, 5}, "s7", "a7", "p7", "JP", "web")}); err != nil {
		t.Fatal(err)
	}
	if s.DeltaRows() != deltaBefore {
		t.Fatalf("new tuple leaked into delta: %d, want %d", s.DeltaRows(), deltaBefore)
	}
	if s.Stats().DroppedOverCap != 1 {
		t.Fatalf("DroppedOverCap = %d, want 1", s.Stats().DroppedOverCap)
	}
	got, _ := s.Query(nil, []Cond{{Dim: "source", Value: "s7"}})
	assertSame(t, got, []float64{0, 0})

	// (b) an update to an EXISTING base tuple still lands.
	if err := s.Apply([]Record{rec(ts(700), []float64{99, 9}, "s1", "a1", "p1", "US", "ios")}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Query(nil, []Cond{
		{Dim: "source", Value: "s1"}, {Dim: "account", Value: "a1"},
		{Dim: "publisher", Value: "p1"}, {Dim: "country", Value: "US"}, {Dim: "os", Value: "ios"},
	})
	assertSame(t, got, []float64{99, 9})
	if s.Stats().DroppedOverCap != 1 {
		t.Fatalf("update wrongly counted as dropped: %d", s.Stats().DroppedOverCap)
	}

	// (c) upstream shrinks under the cap: ReplaceAll succeeds, flag clears,
	// new tuples flow again.
	if err := s.ReplaceAll(baseRecords()[:2]); err != nil {
		t.Fatal(err)
	}
	if s.isCapBlocked() {
		t.Fatal("cap still blocked after successful ReplaceAll under cap")
	}
	if err := s.Apply([]Record{rec(ts(800), []float64{3, 3}, "s7", "a7", "p7", "JP", "web")}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Query(nil, []Cond{{Dim: "source", Value: "s7"}})
	assertSame(t, got, []float64{3, 3})
}

func TestReplaceAllConvergesDeletes(t *testing.T) {
	s := seededStore(t, baseRecords())
	// source lost one row (manual delete upstream)
	remaining := baseRecords()[:3]
	if err := s.ReplaceAll(remaining); err != nil {
		t.Fatal(err)
	}
	if s.Rows() != 3 {
		t.Fatalf("rows = %d, want 3", s.Rows())
	}
	got, _ := s.Query(nil, []Cond{{Dim: "source", Value: "s2"}})
	assertSame(t, got, []float64{0, 0})
	if s.Stats().Reconciles != 1 {
		t.Fatal("reconcile not counted")
	}
}
