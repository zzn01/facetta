package facetta

import (
	"math/rand"
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
	big := make([][]Cond, maxGroups+1)
	if _, err := s.QueryGroups(nil, big); err == nil {
		t.Fatal("too many groups accepted")
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
