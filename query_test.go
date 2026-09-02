package facetta

import (
	"fmt"
	"math/rand"
	"runtime/debug"
	"testing"
)

func seededStore(t *testing.T, recs []Record) *Store {
	t.Helper()
	s, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.seed(recs); err != nil {
		t.Fatal(err)
	}
	return s
}

func baseRecords() []Record {
	return []Record{
		rec(ts(100), []float64{10, 1.5}, "s1", "a1", "p1", "US", "ios"),
		rec(ts(100), []float64{20, 2.5}, "s1", "a1", "p2", "US", "android"),
		rec(ts(100), []float64{30, 3.5}, "s1", "a2", "p1", "DE", "ios"),
		rec(ts(100), []float64{40, 4.0}, "s2", "a2", "p1", "DE", "ios"),
	}
}

func assertSame(t *testing.T, got, want []float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestQueryAgainstOracle(t *testing.T) {
	recs := baseRecords()
	s := seededStore(t, recs)
	rt := newRefTable(testSchema())
	rt.apply(recs)

	cases := [][][]Cond{
		{{{Dim: "source", Value: "s1"}}},
		{{{Dim: "source", Value: "s1"}, {Dim: "account", Value: "a1"}}},
		{{{Dim: "source", Value: "s1"}, {Dim: "account", Value: "a1"}, {Dim: "publisher", Value: "p1"}}},
		{{{Dim: "os", Value: "ios"}}},                                 // non-prefix: full scan
		{{{Dim: "source", Value: "s1"}, {Dim: "os", Value: "ios"}}},   // prefix + extra cond
		{{{Dim: "source", Value: "s1"}}, {{Dim: "os", Value: "ios"}}}, // overlapping union
		{{{Dim: "source", Value: "sX"}}},                              // unknown value: empty result
		{{}},                                                          // empty group: match all
		{{{Dim: "account", Value: "a2"}}},                             // index dim but no dim0: full scan
	}
	var buf []float64
	for i, groups := range cases {
		got, err := s.QueryGroups(buf, groups)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		buf = got
		assertSame(t, got, rt.query(groups))
		_ = i
	}
}

func TestQueryErrors(t *testing.T) {
	s := seededStore(t, baseRecords())
	if _, err := s.QueryGroups(nil, nil); err == nil {
		t.Fatal("zero groups accepted")
	}
	if _, err := s.Query(nil, []Cond{{Dim: "nope", Value: "x"}}); err == nil {
		t.Fatal("unknown dimension accepted")
	}
}

func TestFullScanCounter(t *testing.T) {
	s := seededStore(t, baseRecords())
	before := s.Stats().FullScans
	if _, err := s.Query(nil, []Cond{{Dim: "os", Value: "ios"}}); err != nil {
		t.Fatal(err)
	}
	if s.Stats().FullScans != before+1 {
		t.Fatal("full scan not counted")
	}
	if _, err := s.Query(nil, []Cond{{Dim: "source", Value: "s1"}}); err != nil {
		t.Fatal(err)
	}
	if s.Stats().FullScans != before+1 {
		t.Fatal("indexed query wrongly counted as full scan")
	}
}

func TestQueryZeroAlloc(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	recs := make([]Record, 0, 10000)
	for range 10000 {
		recs = append(recs, randomRecord(rng, 5))
	}
	s := seededStore(t, recs)
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
		t.Fatalf("query allocates: %v allocs/op", allocs)
	}
}

// randomRecord generates low-cardinality dims so filters actually match rows.
func randomRecord(rng *rand.Rand, card int) Record {
	pick := func(prefix string) string {
		return prefix + string(rune('0'+rng.Intn(card)))
	}
	return rec(
		ts(int64(100+rng.Intn(1000))),
		[]float64{float64(rng.Intn(100)), float64(rng.Intn(100))},
		pick("s"), pick("a"), pick("p"), pick("c"), pick("o"),
	)
}

// padConds repeats base cyclically until the result has n entries. Duplicate
// equality conditions on the same dim/value are a harmless redundant AND, so
// this is a simple way to build a group with more conditions than the
// schema has distinct dims for — exactly what's needed to push a query's
// total condition count over fastConds without a wider schema.
func padConds(base []Cond, n int) []Cond {
	out := make([]Cond, n)
	for i := range out {
		out[i] = base[i%len(base)]
	}
	return out
}

// TestQueryPooledScratchPath exercises the pooled (heap) scratch path end to
// end. A legal query shape can exceed the stack fast path today (e.g. 3
// groups of 11 equality conditions each: 33 total, over fastConds=32), and
// every append inside plan()/resolveAggs on that path is a manual
// reslice-then-set that panics on a sizing mistake in getPooledScratch —
// this test is what stands between a silent bug there and an
// out-of-bounds panic on real traffic. It runs every query twice: the
// second call very likely gets back (via sync.Pool) the exact scratch the
// first call released, so it also catches stale mets/ddims/acc/bms/
// condDims/inWins/rWins leaking across queries.
func TestQueryPooledScratchPath(t *testing.T) {
	recs := numericRecords() // s1,a1,p1,10,ios / s1,a1,p2,25,android / s1,a2,p1,90,ios / s2,a2,p1,150,ios
	s := numericStore(t, recs)

	groupA := padConds([]Cond{ // row0 only: s1,a1,p1,ios
		{Dim: "source", Value: "s1"}, {Dim: "account", Value: "a1"},
		{Dim: "publisher", Value: "p1"}, {Dim: "os", Value: "ios"},
	}, 11)
	groupB := padConds([]Cond{ // row1 only: s1,a1,p2,android
		{Dim: "source", Value: "s1"}, {Dim: "account", Value: "a1"},
		{Dim: "publisher", Value: "p2"}, {Dim: "os", Value: "android"},
	}, 11)
	groupC := padConds([]Cond{ // row3 only: s2,a2,p1,ios
		{Dim: "source", Value: "s2"}, {Dim: "account", Value: "a2"},
		{Dim: "publisher", Value: "p1"}, {Dim: "os", Value: "ios"},
	}, 11)
	equalityOnly := [][]Cond{groupA, groupB, groupC}

	aggs := []Agg{
		{Op: AggCount},
		{Metric: "visits", Op: AggSum},
		{Metric: "revenue", Op: AggAvg},
		{Op: AggDistinct, Dim: "os"},
	}

	if measureShape(equalityOnly, 0).fits() {
		t.Fatal("33 conds across 3 groups must not fit the stack fast path")
	}
	if measureShape(equalityOnly, len(aggs)).fits() {
		t.Fatal("shape with aggs must still route pooled")
	}

	for i := 0; i < 2; i++ {
		got, err := s.QueryGroups(nil, equalityOnly)
		if err != nil {
			t.Fatalf("run %d: QueryGroups: %v", i, err)
		}
		assertSame(t, got, []float64{70, 3}) // row0+row1+row3: visits, revenue

		gotAggs, err := s.QueryAggs(nil, aggs, equalityOnly)
		if err != nil {
			t.Fatalf("run %d: QueryAggs: %v", i, err)
		}
		assertSameNaN(t, gotAggs, []float64{3, 70, 1, 2}) // count, sum(visits), avg(revenue), distinct(os)
	}

	// Add a 4th group carrying an IN condition and a Range condition (well
	// within the query-wide 16 IN conds / 128 IN values / 16 ranges
	// limits), to also exercise inWins/inPool/rWins pooled sizing:
	// publisher in {p1,p2} AND country (DimInt) in [10,100] pulls in row2
	// (p1, country=90), which none of the equality-only groups match.
	groupD := []Cond{
		{Dim: "publisher", In: []string{"p1", "p2"}},
		{Dim: "country", Range: &Range{Min: 10, Max: 100}},
	}
	withInAndRange := [][]Cond{groupA, groupB, groupC, groupD}
	if measureShape(withInAndRange, 0).fits() {
		t.Fatal("4-group shape with IN/range must still route pooled")
	}

	for i := 0; i < 2; i++ {
		got, err := s.QueryGroups(nil, withInAndRange)
		if err != nil {
			t.Fatalf("run %d: QueryGroups: %v", i, err)
		}
		assertSame(t, got, []float64{100, 4}) // all four rows, each counted once

		gotAggs, err := s.QueryAggs(nil, aggs, withInAndRange)
		if err != nil {
			t.Fatalf("run %d: QueryAggs: %v", i, err)
		}
		assertSameNaN(t, gotAggs, []float64{4, 100, 1, 2})
	}
}

// TestQueryUnboundedShapes exercises shapes beyond the old 16/16/16/128
// limits through the pooled scratch and checks exact sums against a
// hand-computed expectation.
func TestQueryUnboundedShapes(t *testing.T) {
	sc := testSchema()
	s, err := New(sc, Config{})
	if err != nil {
		t.Fatal(err)
	}
	// 64 distinct source values, 1 row each, metric = row index.
	recs := make([]Record, 64)
	for i := range recs {
		recs[i] = rec(ts(100), []float64{float64(i), 0},
			fmt.Sprintf("s%d", i), "a0", "p0", "c0", "o0")
	}
	if err := s.ReplaceAll(recs); err != nil {
		t.Fatal(err)
	}
	// 40 OR groups (> old 16): sources 0..39, expected sum 0+1+...+39.
	groups := make([][]Cond, 40)
	for i := range groups {
		groups[i] = []Cond{{Dim: "source", Value: fmt.Sprintf("s%d", i)}}
	}
	got, err := s.QueryGroups(nil, groups)
	if err != nil {
		t.Fatal(err)
	}
	if want := float64(39 * 40 / 2); got[0] != want {
		t.Fatalf("40 groups: got %v want %v", got[0], want)
	}
	// one IN with 200 values (> old 16 per cond and > 128 total).
	in := make([]string, 200)
	for i := range in {
		in[i] = fmt.Sprintf("s%d", i) // 64 exist, 136 unknown (dropped)
	}
	got, err = s.QueryGroups(nil, [][]Cond{{{Dim: "source", In: in}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := float64(63 * 64 / 2); got[0] != want {
		t.Fatalf("200-value IN: got %v want %v", got[0], want)
	}
	// 20 aggregate columns (> old 16).
	aggs := make([]Agg, 20)
	for i := range aggs {
		aggs[i] = Agg{Op: AggCount}
	}
	adst, err := s.QueryAggs(nil, aggs, [][]Cond{{}})
	if err != nil {
		t.Fatal(err)
	}
	if adst[19] != 64 {
		t.Fatalf("20 agg cols: got %v want 64", adst[19])
	}
}

// TestLargeInMatchesEqualityUnion pins IN-condition semantics: a >16-value IN
// condition (large enough to hit the binary-search path once matchIns grows
// one) must match exactly the same rows, with the same total, as the
// equivalent union of single-value equality groups — duplicate values in the
// IN list included. This passes both before and after the sort+binary-search
// rewrite; it's a regression tripwire for that rewrite, not evidence on its
// own that the rewrite is correct.
func TestLargeInMatchesEqualityUnion(t *testing.T) {
	sc := testSchema()
	s, err := New(sc, Config{})
	if err != nil {
		t.Fatal(err)
	}
	recs := make([]Record, 64)
	for i := range recs {
		recs[i] = rec(ts(100), []float64{float64(i), 0},
			fmt.Sprintf("s%d", i), "a0", "p0", "c0", "o0")
	}
	if err := s.ReplaceAll(recs); err != nil {
		t.Fatal(err)
	}
	in := make([]string, 33) // > 16: binary path once implemented
	groups := make([][]Cond, 33)
	for i := range in {
		in[i] = fmt.Sprintf("s%d", 2*i%64) // duplicates included: s0 appears twice
		groups[i] = []Cond{{Dim: "source", Value: in[i]}}
	}
	a, err := s.QueryGroups(nil, [][]Cond{{{Dim: "source", In: in}}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.QueryGroups(nil, groups)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, a, b)
}

// TestQueryZeroAllocLarge: after a warm-up call populates the pool, an
// over-capacity query must not allocate on the steady state (pool hit,
// pre-sized slices, no growth). GC is disabled for the measurement window
// so a background collection can't steal the pooled scratch mid-run.
//
// Under -race the bound is NOT loosened arbitrarily: it is derived from a
// specific, documented Go runtime behavior, confirmed by direct
// measurement rather than assumed. $GOROOT/src/sync/pool.go's Put has:
//
//	if race.Enabled {
//		if runtime_randn(4) == 0 {
//			// Randomly drop x on floor.
//			return
//		}
//		...
//	}
//
// i.e. under every -race build, sync.Pool.Put unconditionally discards
// ~1/4 of items, by design, to widen race-detector coverage of the
// fresh-vs-reused code paths — independent of GC, GOMAXPROCS or goroutine
// scheduling. This was verified directly: with GC fully disabled
// (debug.SetGCPercent(-1), confirmed via runtime.MemStats.NumGC delta ==
// 0 across the run) and GOMAXPROCS(1) (ruling out cross-P pool-affinity
// misses), a bare sync.Pool of a trivial struct in this same package still
// showed ~52/200 fresh allocations under -race and exactly 1/200 without
// it — matching the ~25% Put-drop rate, not a GC or scheduling artifact.
// Rebuilding this test's evicted scratch costs 5 mallocs (1 struct + 4
// grown slices: plans/ivs/condDims/condIDs, the only pools this shape's
// 40 groups/40 conds/0 aggs actually needs), so steady state under -race
// is expected to average ~0.25*5 = 1.25 allocs/op (measured 1.0-1.3
// across repeated runs); a real pooling regression (e.g. release() never
// actually returning the scratch) would show ~5/op even under -race,
// still well past the threshold below.
func TestQueryZeroAllocLarge(t *testing.T) {
	sc := testSchema()
	s, err := New(sc, Config{})
	if err != nil {
		t.Fatal(err)
	}
	recs := []Record{rec(ts(100), []float64{1, 0}, "s0", "a0", "p0", "c0", "o0")}
	if err := s.ReplaceAll(recs); err != nil {
		t.Fatal(err)
	}
	groups := make([][]Cond, 40)
	for i := range groups {
		groups[i] = []Cond{{Dim: "source", Value: "s0"}}
	}
	var dst []float64
	if _, err := s.QueryGroups(dst, groups); err != nil { // warm the pool
		t.Fatal(err)
	}
	if _, err := s.QueryGroups(dst, groups); err != nil { // second warm-up: settle the pool item
		t.Fatal(err)
	}
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)
	allocs := testing.AllocsPerRun(200, func() {
		dst, _ = s.QueryGroups(dst, groups)
	})
	threshold := 0.5
	if raceEnabled {
		threshold = 3 // see the doc comment above: ~1.25 expected, real regressions read ~5
	}
	if allocs > threshold {
		t.Fatalf("large query steady state allocates %.1f/op (threshold %.1f)", allocs, threshold)
	}
}
