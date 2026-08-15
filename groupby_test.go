package facetta

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func groupedToMap(res *GroupedResult, nb, na int) map[string][]float64 {
	out := map[string][]float64{}
	for i := 0; i < res.N; i++ {
		k := strings.Join(res.Keys[i*nb:(i+1)*nb], "\x1f")
		out[k] = append([]float64(nil), res.Aggs[i*na:(i+1)*na]...)
	}
	return out
}

func assertGroupsEqual(t *testing.T, got, want map[string][]float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("group count: got %v, want %v", got, want)
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("missing group %q: got %v, want %v", k, got, want)
		}
		assertSameNaN(t, g, w)
	}
}

func TestQueryGroupByBasics(t *testing.T) {
	s := seededStore(t, baseRecords())
	var res GroupedResult
	aggs := []Agg{{Op: AggCount}, {Metric: "visits", Op: AggSum}}
	if err := s.QueryGroupBy(&res, []string{"source"}, aggs, [][]Cond{{}}); err != nil {
		t.Fatal(err)
	}
	if res.N != 2 {
		t.Fatalf("N = %d, want 2", res.N)
	}
	// deterministic output: sorted by key strings
	if res.Keys[0] != "s1" || res.Keys[1] != "s2" {
		t.Fatalf("keys = %v, want [s1 s2]", res.Keys)
	}
	assertSameNaN(t, res.Aggs, []float64{3, 60, 1, 40})
}

func TestQueryGroupByMultiDim(t *testing.T) {
	s := seededStore(t, baseRecords())
	var res GroupedResult
	aggs := []Agg{{Metric: "visits", Op: AggSum}}
	if err := s.QueryGroupBy(&res, []string{"source", "os"}, aggs, [][]Cond{{}}); err != nil {
		t.Fatal(err)
	}
	// groups sorted by (source, os): s1/android, s1/ios, s2/ios
	if res.N != 3 {
		t.Fatalf("N = %d, want 3", res.N)
	}
	wantKeys := []string{"s1", "android", "s1", "ios", "s2", "ios"}
	for i, k := range wantKeys {
		if res.Keys[i] != k {
			t.Fatalf("keys = %v, want %v", res.Keys, wantKeys)
		}
	}
	assertSameNaN(t, res.Aggs, []float64{20, 40, 40})
}

func TestQueryGroupByAgainstOracle(t *testing.T) {
	recs := baseRecords()
	s := seededStore(t, recs)
	rt := newRefTable(testSchema())
	rt.apply(recs)
	aggs := []Agg{
		{Op: AggCount},
		{Metric: "visits", Op: AggSum},
		{Metric: "revenue", Op: AggMin},
		{Metric: "revenue", Op: AggMax},
		{Metric: "visits", Op: AggAvg},
	}
	cases := []struct {
		by     []string
		groups [][]Cond
	}{
		{[]string{"source"}, [][]Cond{{}}},
		{[]string{"os"}, [][]Cond{{{Dim: "source", Value: "s1"}}}},
		{[]string{"country", "os"}, [][]Cond{{}}},
		{[]string{"publisher"}, [][]Cond{{{Dim: "os", In: []string{"ios", "android"}}}}},
		{[]string{"source"}, [][]Cond{{{Dim: "source", Value: "sX"}}}}, // empty result
		{[]string{"account"}, [][]Cond{{{Dim: "source", Value: "s1"}}, {{Dim: "os", Value: "ios"}}}},
	}
	var res GroupedResult
	for i, c := range cases {
		if err := s.QueryGroupBy(&res, c.by, aggs, c.groups); err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		assertGroupsEqual(t, groupedToMap(&res, len(c.by), len(aggs)), rt.queryGroupBy(c.by, aggs, c.groups))
	}
}

func TestQueryGroupByDeltaAndExtras(t *testing.T) {
	s := seededStore(t, baseRecords())
	rt := newRefTable(testSchema())
	rt.apply(baseRecords())
	// delta rows: one shadows a base row, one introduces brand-new strings
	fresh := []Record{
		rec(ts(200), []float64{11, 1.6}, "s1", "a1", "p1", "US", "ios"),
		rec(ts(200), []float64{7, 0.5}, "s9", "a9", "p9", "JP", "linux"),
	}
	if err := s.Apply(fresh); err != nil {
		t.Fatal(err)
	}
	rt.apply(fresh)
	aggs := []Agg{{Op: AggCount}, {Metric: "visits", Op: AggSum}}
	var res GroupedResult
	if err := s.QueryGroupBy(&res, []string{"source", "os"}, aggs, [][]Cond{{}}); err != nil {
		t.Fatal(err)
	}
	assertGroupsEqual(t, groupedToMap(&res, 2, 2), rt.queryGroupBy([]string{"source", "os"}, aggs, [][]Cond{{}}))
}

func TestQueryGroupBySkipsExpired(t *testing.T) {
	s := seededStore(t, baseRecords())
	if err := s.Apply([]Record{recExp(ts(200), ts(300), []float64{99, 9}, "s1", "a1", "p1", "US", "ios")}); err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return ts(400) }
	var res GroupedResult
	if err := s.QueryGroupBy(&res, []string{"source"}, []Agg{{Op: AggCount}}, [][]Cond{{}}); err != nil {
		t.Fatal(err)
	}
	// the shadowing upsert expired: s1 keeps 2 visible rows, s2 keeps 1
	assertGroupsEqual(t, groupedToMap(&res, 1, 1), map[string][]float64{"s1": {2}, "s2": {1}})
}

func TestQueryGroupByReuse(t *testing.T) {
	s := seededStore(t, baseRecords())
	var res GroupedResult
	aggs := []Agg{{Metric: "visits", Op: AggSum}}
	if err := s.QueryGroupBy(&res, []string{"source", "os"}, aggs, [][]Cond{{}}); err != nil {
		t.Fatal(err)
	}
	// a second query with fewer groups must fully reset the buffer
	if err := s.QueryGroupBy(&res, []string{"source"}, aggs, [][]Cond{{{Dim: "source", Value: "s2"}}}); err != nil {
		t.Fatal(err)
	}
	if res.N != 1 || res.Keys[0] != "s2" {
		t.Fatalf("reused result not reset: N=%d keys=%v", res.N, res.Keys)
	}
	assertSameNaN(t, res.Aggs[:1], []float64{40})
}

// TestQueryGroupByAllocsAmortized pins the documented allocation contract:
// on a reused GroupedResult the per-call allocations are O(result groups),
// not O(scanned rows), and slice/map storage amortizes across calls.
func TestQueryGroupByAllocsAmortized(t *testing.T) {
	s := seededStore(t, manyRandomRecords(10000, 5))
	var res GroupedResult
	aggs := []Agg{{Op: AggCount}, {Metric: "visits", Op: AggSum}}
	by := []string{"source", "os"} // 25 result groups
	warm := func() {
		if err := s.QueryGroupBy(&res, by, aggs, [][]Cond{{}}); err != nil {
			t.Fatal(err)
		}
	}
	warm()
	groupsOut := res.N
	allocs := testing.AllocsPerRun(100, warm)
	// loose ceiling: a handful per result group (map key interning, sort
	// scratch) — catches any regression to per-row allocation
	if limit := float64(4*groupsOut + 16); allocs > limit {
		t.Fatalf("QueryGroupBy allocates %v/op for %d groups (limit %v)", allocs, groupsOut, limit)
	}
}

func TestQueryGroupByErrors(t *testing.T) {
	s := seededStore(t, baseRecords())
	aggs := []Agg{{Op: AggCount}}
	all := [][]Cond{{}}
	var res GroupedResult
	if err := s.QueryGroupBy(nil, []string{"source"}, aggs, all); err == nil {
		t.Fatal("nil result accepted")
	}
	if err := s.QueryGroupBy(&res, nil, aggs, all); err == nil {
		t.Fatal("empty by accepted")
	}
	if err := s.QueryGroupBy(&res, []string{"source", "source"}, aggs, all); err == nil {
		t.Fatal("duplicate by dim accepted")
	}
	if err := s.QueryGroupBy(&res, []string{"nope"}, aggs, all); err == nil {
		t.Fatal("unknown by dim accepted")
	}
	if err := s.QueryGroupBy(&res, []string{"source"}, nil, all); err == nil {
		t.Fatal("zero aggs accepted")
	}
	if err := s.QueryGroupBy(&res, []string{"source"}, aggs, nil); err == nil {
		t.Fatal("zero groups accepted")
	}
}

func TestQueryGroupByScanBudget(t *testing.T) {
	s, err := New(testSchema(), Config{MaxScanRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.seed(manyRandomRecords(1000, 5)); err != nil {
		t.Fatal(err)
	}
	var res GroupedResult
	err = s.QueryGroupBy(&res, []string{"source"}, []Agg{{Op: AggCount}}, [][]Cond{{}})
	if !errors.Is(err, ErrScanBudget) {
		t.Fatalf("err = %v, want ErrScanBudget", err)
	}
}
