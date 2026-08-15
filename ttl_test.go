package facetta

import (
	"testing"
	"time"
)

// recExp builds a record with an explicit ExpireAt (per-record TTL).
func recExp(updated, expire time.Time, metrics []float64, dims ...string) Record {
	r := rec(updated, metrics, dims...)
	r.ExpireAt = expire
	return r
}

// fixedClock returns a store whose clock is controllable via the returned
// pointer; both the store and oracle share it in these tests.
func newClockStore(t *testing.T, cfg Config) (*Store, *time.Time) {
	t.Helper()
	s, err := New(testSchema(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(10000, 0).UTC()
	clk := &now
	s.now = func() time.Time { return *clk }
	return s, clk
}

// TestPerRecordExpiryVisibility covers acceptance §2: expired-at-write dropped,
// future rows visible, clock advance hides them WITHOUT compaction, Compact
// physically reclaims. Oracle agreement throughout.
func TestPerRecordExpiryVisibility(t *testing.T) {
	s, clk := newClockStore(t, Config{})
	rt := newRefTable(testSchema())
	rt.now = func() time.Time { return *clk }
	now := *clk

	future := now.Add(time.Hour)
	past := now.Add(-time.Minute)
	recs := []Record{
		rec(now, []float64{10, 1}, "s1", "a1", "p1", "US", "ios"),              // never expires
		recExp(now, future, []float64{20, 2}, "s2", "a2", "p2", "DE", "web"),   // future
		recExp(now, past, []float64{99, 9}, "s3", "a3", "p3", "FR", "android"), // already expired -> dropped at build
	}
	if err := s.seed(recs); err != nil {
		t.Fatal(err)
	}
	rt.replaceAll(recs)

	// already-expired row dropped physically at build; two rows kept.
	if s.Rows() != 2 {
		t.Fatalf("seed rows = %d, want 2 (expired dropped)", s.Rows())
	}
	assertQueryAgree(t, s, rt, "seed")

	// future-expiring row visible now.
	got, _ := s.Query(nil, []Cond{{Dim: "source", Value: "s2"}})
	if got[0] != 20 {
		t.Fatalf("future row visits = %v, want 20", got[0])
	}

	// advance clock past the future expiry: invisible WITHOUT any compaction.
	*clk = future.Add(time.Millisecond)
	got, _ = s.Query(nil, []Cond{{Dim: "source", Value: "s2"}})
	if got[0] != 0 {
		t.Fatalf("expired row still visible: %v", got[0])
	}
	// physical rows unchanged (no compaction yet).
	if s.Rows() != 2 {
		t.Fatalf("rows changed without compaction: %d", s.Rows())
	}
	assertQueryAgree(t, s, rt, "after clock advance")

	// Compact physically reclaims the expired row.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if s.Rows() != 1 {
		t.Fatalf("rows after compaction = %d, want 1 (reclaimed)", s.Rows())
	}
	assertQueryAgree(t, s, rt, "after compaction")
}

// TestTombstoneViaExpiry covers acceptance §2/spec §5: applying a record with
// ExpireAt<=now for an existing base tuple makes it immediately invisible, then
// physically gone after Compact.
func TestTombstoneViaExpiry(t *testing.T) {
	s, clk := newClockStore(t, Config{})
	rt := newRefTable(testSchema())
	rt.now = func() time.Time { return *clk }
	now := *clk

	base := []Record{rec(now, []float64{10, 1}, "s1", "a1", "p1", "US", "ios")}
	if err := s.seed(base); err != nil {
		t.Fatal(err)
	}
	rt.replaceAll(base)
	got, _ := s.Query(nil, []Cond{{Dim: "source", Value: "s1"}})
	if got[0] != 10 {
		t.Fatalf("base row not visible: %v", got[0])
	}

	// tombstone: same tuple, newer UpdatedAt, ExpireAt = now (invisible now).
	tomb := recExp(now.Add(time.Minute), now, []float64{10, 1}, "s1", "a1", "p1", "US", "ios")
	if err := s.Apply([]Record{tomb}); err != nil {
		t.Fatal(err)
	}
	rt.apply([]Record{tomb})
	got, _ = s.Query(nil, []Cond{{Dim: "source", Value: "s1"}})
	if got[0] != 0 {
		t.Fatalf("tombstoned row still visible: %v", got[0])
	}
	assertQueryAgree(t, s, rt, "after tombstone")

	// Compact drops it physically.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if s.Rows() != 0 {
		t.Fatalf("rows after compaction = %d, want 0 (tombstone reclaimed)", s.Rows())
	}
}

// TestNeedsCompactionPerRecordExpiry covers the idle-upstream reclaim gap: a
// store with future-expiring rows and empty delta reports no compaction need
// until the clock passes expiry, then Compact reclaims and it clears.
func TestNeedsCompactionPerRecordExpiry(t *testing.T) {
	s, clk := newClockStore(t, Config{})
	now := *clk
	recs := []Record{
		rec(now, []float64{10, 1}, "s1", "a1", "p1", "US", "ios"), // never expires
		recExp(now, now.Add(time.Hour), []float64{20, 2}, "s2", "a2", "p2", "DE", "web"),
	}
	if err := s.seed(recs); err != nil {
		t.Fatal(err)
	}
	if s.DeltaRows() != 0 {
		t.Fatal("expected empty delta after seed")
	}
	if s.NeedsCompaction() {
		t.Fatal("NeedsCompaction true before any expiry")
	}
	*clk = now.Add(time.Hour + time.Millisecond)
	if !s.NeedsCompaction() {
		t.Fatal("NeedsCompaction false after per-record expiry hit")
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if s.Rows() != 1 {
		t.Fatalf("rows after compaction = %d, want 1", s.Rows())
	}
	if s.NeedsCompaction() {
		t.Fatal("NeedsCompaction still true after reclaim")
	}
}

// TestNeedsCompactionGlobalTTL covers the same idle-reclaim path for global
// Config.TTL with an empty delta.
func TestNeedsCompactionGlobalTTL(t *testing.T) {
	s, clk := newClockStore(t, Config{TTL: time.Hour})
	now := *clk
	recs := []Record{rec(now, []float64{10, 1}, "s1", "a1", "p1", "US", "ios")}
	if err := s.seed(recs); err != nil {
		t.Fatal(err)
	}
	if s.NeedsCompaction() {
		t.Fatal("NeedsCompaction true before TTL expiry")
	}
	*clk = now.Add(2 * time.Hour) // row now older than TTL
	if !s.NeedsCompaction() {
		t.Fatal("NeedsCompaction false after global TTL expiry")
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if s.Rows() != 0 {
		t.Fatalf("rows after TTL compaction = %d, want 0", s.Rows())
	}
	if s.NeedsCompaction() {
		t.Fatal("NeedsCompaction still true after TTL reclaim")
	}
}

// TestNeedsCompactionFreshStore: a non-expiring store with empty delta never
// needs compaction.
func TestNeedsCompactionFreshStore(t *testing.T) {
	s, _ := newClockStore(t, Config{})
	now := s.now()
	recs := []Record{rec(now, []float64{10, 1}, "s1", "a1", "p1", "US", "ios")}
	if err := s.seed(recs); err != nil {
		t.Fatal(err)
	}
	if s.NeedsCompaction() {
		t.Fatal("fresh non-expiring store needs compaction")
	}
	// empty store likewise.
	s2, _ := newClockStore(t, Config{})
	if s2.NeedsCompaction() {
		t.Fatal("empty store needs compaction")
	}
}

// TestQueryZeroAllocWithExpiry confirms the read-time expiry skip keeps the
// query path zero-alloc even when rows carry ExpireAt (some already expired).
func TestQueryZeroAllocWithExpiry(t *testing.T) {
	now := time.Unix(10000, 0).UTC()
	s, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return now }
	recs := make([]Record, 0, 10000)
	for i := range 10000 {
		r := rec(now, []float64{float64(i % 100), float64(i % 7)},
			"s"+string(rune('0'+i%5)), "a"+string(rune('0'+i%5)),
			"p"+string(rune('0'+i%5)), "c"+string(rune('0'+i%3)), "o"+string(rune('0'+i%4)))
		switch i % 3 {
		case 0:
			r.ExpireAt = now.Add(time.Hour) // future
		case 1:
			r.ExpireAt = now.Add(-time.Minute) // expired (present via Apply below)
		}
		recs = append(recs, r)
	}
	if err := s.seed(recs); err != nil {
		t.Fatal(err)
	}
	// Also push some expiring rows into the delta so both scan paths exercise
	// the skip while staying zero-alloc.
	if err := s.Apply([]Record{
		recExp(now, now.Add(time.Hour), []float64{1, 1}, "s9", "a9", "p9", "c9", "o9"),
		recExp(now, now.Add(-time.Minute), []float64{1, 1}, "s8", "a8", "p8", "c8", "o8"),
	}); err != nil {
		t.Fatal(err)
	}
	buf := make([]float64, 0, 2)
	groups := [][]Cond{
		{{Dim: "source", Value: "s1"}, {Dim: "account", Value: "a1"}},
		{{Dim: "source", Value: "s2"}},
	}
	allocs := testing.AllocsPerRun(100, func() {
		var err error
		buf, err = s.QueryGroups(buf, groups)
		if err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("query with expiry allocates: %v allocs/op", allocs)
	}
}

// assertQueryAgree checks store and oracle agree over a fixed set of probes.
func assertQueryAgree(t *testing.T, s *Store, rt *refTable, step string) {
	t.Helper()
	probes := [][][]Cond{
		{{}},
		{{{Dim: "source", Value: "s1"}}},
		{{{Dim: "source", Value: "s2"}}},
		{{{Dim: "source", Value: "s3"}}},
		{{{Dim: "os", Value: "ios"}}},
	}
	for _, g := range probes {
		got, err := s.QueryGroups(nil, g)
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		assertSame(t, got, rt.query(g))
	}
}

// TestTombstoneReclaimThenStaleUpsert pins the resurrection semantics: once a
// tombstone is physically reclaimed, the tuple's updated watermark is gone,
// so an out-of-order OLDER record recreates the row. This is defined behavior
// of the delete model (invariant: drift beyond tombstones is repaired by
// host-driven ReplaceAll); the oracle models physical reclaim so the
// equivalence contract stays exact on both sides.
func TestTombstoneReclaimThenStaleUpsert(t *testing.T) {
	sc := testSchema()
	s, err := New(sc, Config{})
	if err != nil {
		t.Fatal(err)
	}
	clk := ts(1000)
	s.now = func() time.Time { return clk }
	rt := newRefTable(sc)
	rt.now = s.now

	tomb := rec(ts(900), []float64{2, 2}, "s0", "a0", "p0", "c0", "o0")
	tomb.ExpireAt = ts(950) // already past: tombstone
	if err := s.Apply([]Record{tomb}); err != nil {
		t.Fatal(err)
	}
	rt.apply([]Record{tomb})

	if err := s.Compact(); err != nil { // physical reclaim on both sides
		t.Fatal(err)
	}
	rt.reclaimExpired(s.now())
	if got := s.Rows() + s.DeltaRows(); got != 0 {
		t.Fatalf("rows after reclaim = %d, want 0", got)
	}

	stale := rec(ts(800), []float64{1, 1}, "s0", "a0", "p0", "c0", "o0")
	if err := s.Apply([]Record{stale}); err != nil {
		t.Fatal(err)
	}
	rt.apply([]Record{stale})

	groups := [][]Cond{{}}
	got, err := s.QueryGroups(nil, groups)
	if err != nil {
		t.Fatal(err)
	}
	want := rt.query(groups)
	for m := range want {
		if got[m] != want[m] {
			t.Fatalf("engine %v != oracle %v", got, want)
		}
	}
	if got[0] != 1 { // the resurrected stale row is visible by definition
		t.Fatalf("resurrected row: got %v, want [1 1]", got)
	}
}
