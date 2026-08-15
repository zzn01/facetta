package facetta

import (
	"testing"
	"time"
)

func TestApplyDeltaVisibleImmediately(t *testing.T) {
	recs := baseRecords()
	s := seededStore(t, recs)
	rt := newRefTable(testSchema())
	rt.apply(recs)

	upserts := []Record{
		// update an existing base row (same tuple, newer ts, new metrics)
		rec(ts(500), []float64{100, 10}, "s1", "a1", "p1", "US", "ios"),
		// brand-new row with a brand-new dictionary value "s9"
		rec(ts(500), []float64{7, 0.7}, "s9", "a9", "p9", "JP", "web"),
	}
	if err := s.Apply(upserts); err != nil {
		t.Fatal(err)
	}
	rt.apply(upserts)

	cases := [][][]Cond{
		{{{Dim: "source", Value: "s1"}}}, // must see updated metrics, not doubled
		{{{Dim: "source", Value: "s9"}}}, // delta-only dictionary value
		{{{Dim: "os", Value: "ios"}}},    // full scan sees base+delta correctly
		{{}},                             // match-all
		{{{Dim: "source", Value: "s1"}}, {{Dim: "source", Value: "s9"}}},
	}
	for i, groups := range cases {
		got, err := s.QueryGroups(nil, groups)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		assertSame(t, got, rt.query(groups))
	}
	if s.DeltaRows() != 2 {
		t.Fatalf("delta rows = %d, want 2", s.DeltaRows())
	}
}

func TestApplyDedupWithinDelta(t *testing.T) {
	s := seededStore(t, baseRecords())
	if err := s.Apply([]Record{
		rec(ts(500), []float64{1, 1}, "s9", "a9", "p9", "JP", "web"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply([]Record{
		rec(ts(600), []float64{2, 2}, "s9", "a9", "p9", "JP", "web"),
	}); err != nil {
		t.Fatal(err)
	}
	if s.DeltaRows() != 1 {
		t.Fatalf("delta rows = %d, want 1 (upsert replaced)", s.DeltaRows())
	}
	got, _ := s.Query(nil, []Cond{{Dim: "source", Value: "s9"}})
	assertSame(t, got, []float64{2, 2})
}

// TestApplyStaleUpsertAgainstNewerBase reproduces the ordering bug where a
// record older than the base row it targets was baked in anyway. The oracle
// ignores such stale upserts, so the store must too.
func TestApplyStaleUpsertAgainstNewerBase(t *testing.T) {
	sc := testSchema()
	s, err := New(sc, Config{})
	if err != nil {
		t.Fatal(err)
	}
	rt := newRefTable(sc)

	newer := []Record{rec(ts(200), []float64{50, 5}, "s1", "a1", "p1", "US", "ios")}
	if err := s.Apply(newer); err != nil {
		t.Fatal(err)
	}
	rt.apply(newer)
	if err := s.Compact(); err != nil { // bake the newer row into base
		t.Fatal(err)
	}

	stale := []Record{rec(ts(100), []float64{999, 999}, "s1", "a1", "p1", "US", "ios")}
	if err := s.Apply(stale); err != nil {
		t.Fatal(err)
	}
	rt.apply(stale) // oracle drops it (older than existing)

	if s.DeltaRows() != 0 {
		t.Fatalf("stale upsert created a delta row: delta=%d", s.DeltaRows())
	}
	groups := [][]Cond{{{Dim: "source", Value: "s1"}}}
	got, err := s.QueryGroups(nil, groups)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, got, rt.query(groups)) // must be {50,5}, not {999,999}

	// a stale-vs-base upsert with equal ts wins (shadow), matching After()->skip.
	tie := []Record{rec(ts(200), []float64{7, 7}, "s1", "a1", "p1", "US", "ios")}
	if err := s.Apply(tie); err != nil {
		t.Fatal(err)
	}
	rt.apply(tie)
	got, _ = s.QueryGroups(nil, groups)
	assertSame(t, got, rt.query(groups)) // {7,7}
}

// TestCompactDropsTTLExpiredDelta exercises mergeView's delta TTL-skip loop: a
// delta row older than the cutoff at compaction time disappears while fresh rows
// survive.
func TestCompactDropsTTLExpiredDelta(t *testing.T) {
	s, err := New(testSchema(), Config{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100000, 0).UTC()
	s.now = func() time.Time { return now }

	// two fresh delta rows applied now; no base rows.
	stale := rec(now.Add(-time.Minute), []float64{1, 1}, "s1", "a1", "p1", "US", "ios")
	fresh := rec(now.Add(-time.Minute), []float64{2, 2}, "s2", "a2", "p2", "DE", "web")
	if err := s.Apply([]Record{stale, fresh}); err != nil {
		t.Fatal(err)
	}
	if s.DeltaRows() != 2 {
		t.Fatalf("delta rows = %d, want 2", s.DeltaRows())
	}

	// advance the clock so the stale row (but not a fresh re-apply) ages out.
	now = now.Add(2 * time.Hour)
	fresh2 := fresh
	fresh2.UpdatedAt = now.Add(-time.Minute)
	if err := s.Apply([]Record{fresh2}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if s.Rows() != 1 {
		t.Fatalf("rows = %d, want 1 (stale delta row dropped)", s.Rows())
	}
	got, _ := s.Query(nil, []Cond{{Dim: "source", Value: "s2"}})
	assertSame(t, got, []float64{2, 2})
	got, _ = s.Query(nil, []Cond{{Dim: "source", Value: "s1"}})
	assertSame(t, got, []float64{0, 0})
}

func TestApplyPositionAdvances(t *testing.T) {
	s := seededStore(t, baseRecords())
	if !s.SyncPosition().Equal(ts(100)) {
		t.Fatalf("position = %v", s.SyncPosition())
	}
	_ = s.Apply([]Record{rec(ts(700), []float64{1, 1}, "s9", "a9", "p9", "JP", "web")})
	if !s.SyncPosition().Equal(ts(700)) {
		t.Fatalf("position = %v, want ts(700)", s.SyncPosition())
	}
}
