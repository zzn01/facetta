package facetta

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func manyRandomRecords(n, card int) []Record {
	rng := rand.New(rand.NewSource(1))
	recs := make([]Record, 0, n)
	for range n {
		recs = append(recs, randomRecord(rng, card))
	}
	return recs
}

// assertSameNaN compares aggregate outputs treating NaN == NaN.
func assertSameNaN(t *testing.T, got, want []float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] && !(math.IsNaN(got[i]) && math.IsNaN(want[i])) {
			t.Fatalf("agg %d: got %v, want %v", i, got, want)
		}
	}
}

func TestQueryAggsBasics(t *testing.T) {
	s := seededStore(t, baseRecords())
	aggs := []Agg{
		{Metric: "visits", Op: AggSum},
		{Op: AggCount},
		{Metric: "visits", Op: AggMin},
		{Metric: "visits", Op: AggMax},
		{Metric: "revenue", Op: AggAvg},
	}
	// source=s1 matches visits 10,20,30 / revenue 1.5,2.5,3.5
	got, err := s.QueryAggs(nil, aggs, [][]Cond{{{Dim: "source", Value: "s1"}}})
	if err != nil {
		t.Fatal(err)
	}
	assertSameNaN(t, got, []float64{60, 3, 10, 30, 2.5})

	// no matching rows: Sum=0, Count=0, Min/Max/Avg=NaN
	got, err = s.QueryAggs(got, aggs, [][]Cond{{{Dim: "source", Value: "absent"}}})
	if err != nil {
		t.Fatal(err)
	}
	assertSameNaN(t, got, []float64{0, 0, math.NaN(), math.NaN(), math.NaN()})
}

func TestQueryAggsUnionNoDoubleCount(t *testing.T) {
	s := seededStore(t, baseRecords())
	// source=s1 (3 rows) union os=ios (3 rows), overlap 2 rows -> 4 distinct
	got, err := s.QueryAggs(nil, []Agg{{Op: AggCount}, {Metric: "visits", Op: AggSum}}, [][]Cond{
		{{Dim: "source", Value: "s1"}},
		{{Dim: "os", Value: "ios"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertSameNaN(t, got, []float64{4, 100})
}

func TestQueryAggsAgainstOracle(t *testing.T) {
	recs := baseRecords()
	s := seededStore(t, recs)
	rt := newRefTable(testSchema())
	rt.apply(recs)
	aggs := []Agg{
		{Op: AggCount},
		{Metric: "visits", Op: AggSum},
		{Metric: "visits", Op: AggAvg},
		{Metric: "revenue", Op: AggMin},
		{Metric: "revenue", Op: AggMax},
	}
	cases := [][][]Cond{
		{{{Dim: "source", Value: "s1"}}},
		{{{Dim: "os", Value: "ios"}}},
		{{}},
		{{{Dim: "source", Value: "s1"}}, {{Dim: "os", Value: "ios"}}},
		{{{Dim: "source", Value: "sX"}}},
		{{{Dim: "os", In: []string{"ios", "android"}}}},
		{{{Dim: "source", Value: "s2"}, {Dim: "os", In: []string{"ios", "nope"}}}},
	}
	var buf []float64
	for i, groups := range cases {
		got, err := s.QueryAggs(buf, aggs, groups)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		buf = got
		assertSameNaN(t, got, rt.queryAggs(aggs, groups))
	}
}

func TestQueryAggsSkipsExpired(t *testing.T) {
	s := seededStore(t, baseRecords())
	// upsert one visible row with a future expiry, then advance the clock past it
	if err := s.Apply([]Record{recExp(ts(200), ts(300), []float64{99, 9}, "s1", "a1", "p1", "US", "ios")}); err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return ts(400) }
	got, err := s.QueryAggs(nil, []Agg{{Op: AggCount}, {Metric: "visits", Op: AggSum}}, [][]Cond{{}})
	if err != nil {
		t.Fatal(err)
	}
	// the upsert shadowed the base 10-visits row and then expired: 3 rows remain
	assertSameNaN(t, got, []float64{3, 90})
}

func TestQueryAggsErrors(t *testing.T) {
	s := seededStore(t, baseRecords())
	all := [][]Cond{{}}
	if _, err := s.QueryAggs(nil, nil, all); err == nil {
		t.Fatal("zero aggs accepted")
	}
	big := make([]Agg, maxAggs+1)
	if _, err := s.QueryAggs(nil, big, all); err == nil {
		t.Fatal("too many aggs accepted")
	}
	if _, err := s.QueryAggs(nil, []Agg{{Metric: "nope", Op: AggSum}}, all); err == nil {
		t.Fatal("unknown metric accepted")
	}
	if _, err := s.QueryAggs(nil, []Agg{{Metric: "visits", Op: AggCount}}, all); err == nil {
		t.Fatal("AggCount with a metric accepted")
	}
	if _, err := s.QueryAggs(nil, []Agg{{Op: AggSum}}, all); err == nil {
		t.Fatal("AggSum without a metric accepted")
	}
	if _, err := s.QueryAggs(nil, []Agg{{Op: AggCount}}, nil); err == nil {
		t.Fatal("zero groups accepted")
	}
}

func TestCondIn(t *testing.T) {
	recs := baseRecords()
	s := seededStore(t, recs)
	rt := newRefTable(testSchema())
	rt.apply(recs)

	cases := [][][]Cond{
		// IN on a non-index dim: filter during scan
		{{{Dim: "os", In: []string{"ios", "android"}}}},
		// IN with values partially absent from the dictionaries
		{{{Dim: "os", In: []string{"ios", "nope"}}}},
		// IN where no value exists anywhere: matches nothing
		{{{Dim: "os", In: []string{"nope1", "nope2"}}}},
		// IN combined with an indexed equality prefix
		{{{Dim: "source", Value: "s1"}, {Dim: "publisher", In: []string{"p1", "p2"}}}},
		// IN on the leading index dim (degrades to scan, still correct)
		{{{Dim: "source", In: []string{"s1", "s2"}}}},
		// single-value IN behaves like equality
		{{{Dim: "account", In: []string{"a2"}}}},
	}
	var buf []float64
	for i, groups := range cases {
		got, err := s.QueryGroups(buf, groups)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		buf = got
		assertSame(t, got, rt.query(groups))
	}
}

func TestCondInDeltaOnlyValue(t *testing.T) {
	s := seededStore(t, baseRecords())
	rt := newRefTable(testSchema())
	rt.apply(baseRecords())
	// a value that only exists in the delta overlay (extras dictionary)
	fresh := []Record{rec(ts(200), []float64{7, 0.5}, "s9", "a9", "p9", "JP", "linux")}
	if err := s.Apply(fresh); err != nil {
		t.Fatal(err)
	}
	rt.apply(fresh)
	groups := [][]Cond{{{Dim: "source", In: []string{"s9", "s1"}}}}
	got, err := s.QueryGroups(nil, groups)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, got, rt.query(groups))
	// all IN values only in extras: base rows can never match
	groups = [][]Cond{{{Dim: "source", In: []string{"s9"}}}}
	got, err = s.QueryGroups(got, groups)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, got, rt.query(groups))
}

func TestCondInErrors(t *testing.T) {
	s := seededStore(t, baseRecords())
	if _, err := s.Query(nil, []Cond{{Dim: "os", Value: "ios", In: []string{"android"}}}); err == nil {
		t.Fatal("Value and In together accepted")
	}
	big := make([]string, maxInVals+1)
	for i := range big {
		big[i] = "v"
	}
	if _, err := s.Query(nil, []Cond{{Dim: "os", In: big}}); err == nil {
		t.Fatal("too many In values accepted")
	}
}

func TestQueryAggsZeroAlloc(t *testing.T) {
	s := seededStore(t, manyRandomRecords(10000, 5))
	buf := make([]float64, 0, 4)
	aggs := []Agg{
		{Op: AggCount},
		{Metric: "visits", Op: AggSum},
		{Metric: "visits", Op: AggMin},
		{Metric: "revenue", Op: AggAvg},
	}
	groups := [][]Cond{
		{{Dim: "source", Value: "s1"}, {Dim: "account", Value: "a1"}},
		{{Dim: "source", Value: "s2"}, {Dim: "os", In: []string{"o1", "o2"}}},
	}
	allocs := testing.AllocsPerRun(100, func() {
		var err error
		buf, err = s.QueryAggs(buf, aggs, groups)
		if err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("QueryAggs allocates: %v allocs/op", allocs)
	}
}
