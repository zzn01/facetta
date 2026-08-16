package facetta

import (
	"testing"
	"time"
)

func TestQueryAggsDistinctBasics(t *testing.T) {
	s := seededStore(t, baseRecords())
	aggs := []Agg{
		{Dim: "publisher", Op: AggDistinct},
		{Dim: "country", Op: AggDistinct},
		{Op: AggCount},
	}
	// all rows: publishers {p1,p2}, countries {US,DE}
	got, err := s.QueryAggs(nil, aggs, [][]Cond{{}})
	if err != nil {
		t.Fatal(err)
	}
	assertSameNaN(t, got, []float64{2, 2, 4})

	// source=s1: rows p1,p2,p1 -> 2 publishers; US,US,DE -> 2 countries
	got, err = s.QueryAggs(got, aggs, [][]Cond{{{Dim: "source", Value: "s1"}}})
	if err != nil {
		t.Fatal(err)
	}
	assertSameNaN(t, got, []float64{2, 2, 3})

	// no matching rows: distinct is 0 (count semantics, not NaN)
	got, err = s.QueryAggs(got, aggs, [][]Cond{{{Dim: "source", Value: "absent"}}})
	if err != nil {
		t.Fatal(err)
	}
	assertSameNaN(t, got, []float64{0, 0, 0})
}

func TestQueryAggsDistinctDeltaAndShadow(t *testing.T) {
	s := seededStore(t, baseRecords())
	rt := newRefTable(testSchema())
	rt.apply(baseRecords())
	fresh := []Record{
		// shadows the base (s1,a1,p1,US,ios) row: p1 must still count once
		rec(ts(200), []float64{11, 1.6}, "s1", "a1", "p1", "US", "ios"),
		// brand-new publisher only in the extras dictionary
		rec(ts(200), []float64{7, 0.5}, "s1", "a9", "p9", "JP", "linux"),
	}
	if err := s.Apply(fresh); err != nil {
		t.Fatal(err)
	}
	rt.apply(fresh)
	aggs := []Agg{{Dim: "publisher", Op: AggDistinct}, {Dim: "country", Op: AggDistinct}}
	got, err := s.QueryAggs(nil, aggs, [][]Cond{{}})
	if err != nil {
		t.Fatal(err)
	}
	assertSameNaN(t, got, rt.queryAggs(aggs, [][]Cond{{}})) // p1,p2,p9 / US,DE,JP
	assertSameNaN(t, got, []float64{3, 3})
}

func TestQueryAggsDistinctSkipsExpired(t *testing.T) {
	s := seededStore(t, baseRecords())
	// a soon-to-expire row with an otherwise-unseen publisher
	if err := s.Apply([]Record{recExp(ts(200), ts(300), []float64{1, 1}, "s1", "a1", "p9", "US", "ios")}); err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return ts(400) }
	got, err := s.QueryAggs(nil, []Agg{{Dim: "publisher", Op: AggDistinct}}, [][]Cond{{}})
	if err != nil {
		t.Fatal(err)
	}
	// p9's row is expired and the base row it shadowed stays masked: {p1,p2}...
	// minus the shadowed row's p1 still visible via other rows -> 2
	assertSameNaN(t, got, []float64{2})
}

func TestQueryAggsDistinctErrors(t *testing.T) {
	s := seededStore(t, baseRecords())
	all := [][]Cond{{}}
	if _, err := s.QueryAggs(nil, []Agg{{Op: AggDistinct}}, all); err == nil {
		t.Fatal("AggDistinct without Dim accepted")
	}
	if _, err := s.QueryAggs(nil, []Agg{{Dim: "nope", Op: AggDistinct}}, all); err == nil {
		t.Fatal("AggDistinct with unknown Dim accepted")
	}
	if _, err := s.QueryAggs(nil, []Agg{{Dim: "publisher", Metric: "visits", Op: AggDistinct}}, all); err == nil {
		t.Fatal("AggDistinct with a Metric accepted")
	}
	if _, err := s.QueryAggs(nil, []Agg{{Dim: "publisher", Metric: "visits", Op: AggSum}}, all); err == nil {
		t.Fatal("AggSum with a Dim accepted")
	}
	if _, err := s.QueryAggs(nil, []Agg{{Dim: "publisher", Op: AggCount}}, all); err == nil {
		t.Fatal("AggCount with a Dim accepted")
	}
}

// TestQueryAggsDistinctAllocs pins the documented contract: a query allocates
// exactly one id bitmap per AggDistinct column and nothing else.
func TestQueryAggsDistinctAllocs(t *testing.T) {
	s := seededStore(t, manyRandomRecords(10000, 5))
	buf := make([]float64, 0, 3)
	aggs := []Agg{
		{Dim: "publisher", Op: AggDistinct},
		{Dim: "os", Op: AggDistinct},
		{Metric: "visits", Op: AggSum},
	}
	groups := [][]Cond{{{Dim: "source", Value: "s1"}}}
	allocs := testing.AllocsPerRun(100, func() {
		var err error
		buf, err = s.QueryAggs(buf, aggs, groups)
		if err != nil {
			panic(err)
		}
	})
	if allocs != 2 {
		t.Fatalf("QueryAggs with 2 distinct columns allocates %v/op, want 2 (one bitmap each)", allocs)
	}
}

func TestQueryGroupByDistinct(t *testing.T) {
	recs := baseRecords()
	s := seededStore(t, recs)
	rt := newRefTable(testSchema())
	rt.apply(recs)
	aggs := []Agg{{Dim: "publisher", Op: AggDistinct}, {Op: AggCount}}
	var res GroupedResult
	if err := s.QueryGroupBy(&res, []string{"country"}, aggs, [][]Cond{{}}); err != nil {
		t.Fatal(err)
	}
	// DE: rows p1,p1 -> 1 distinct; US: p1,p2 -> 2 distinct
	assertGroupsEqual(t, groupedToMap(&res, 1, 2), map[string][]float64{
		"DE": {1, 2},
		"US": {2, 2},
	})
	assertGroupsEqual(t, groupedToMap(&res, 1, 2), rt.queryGroupBy([]string{"country"}, aggs, [][]Cond{{}}))

	// reuse the result across calls: the distinct sets must reset too
	if err := s.QueryGroupBy(&res, []string{"os"}, aggs, [][]Cond{{}}); err != nil {
		t.Fatal(err)
	}
	assertGroupsEqual(t, groupedToMap(&res, 1, 2), rt.queryGroupBy([]string{"os"}, aggs, [][]Cond{{}}))
}
