package facetta

import (
	"errors"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// numericSchema is testSchema with country declared numeric.
func numericSchema() Schema {
	sc := testSchema()
	sc.Dims[3].Type = DimInt // country
	return sc
}

func numericStore(t *testing.T, recs []Record) *Store {
	t.Helper()
	s, err := New(numericSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.seed(recs); err != nil {
		t.Fatal(err)
	}
	return s
}

// numericRecords: country carries numeric strings (identity = the number).
func numericRecords() []Record {
	return []Record{
		rec(ts(100), []float64{10, 1}, "s1", "a1", "p1", "10", "ios"),
		rec(ts(100), []float64{20, 1}, "s1", "a1", "p2", "25", "android"),
		rec(ts(100), []float64{30, 1}, "s1", "a2", "p1", "90", "ios"),
		rec(ts(100), []float64{40, 1}, "s2", "a2", "p1", "150", "ios"),
	}
}

func TestCondRangeBasics(t *testing.T) {
	recs := numericRecords()
	s := numericStore(t, recs)
	rt := newRefTable(numericSchema())
	rt.apply(recs)

	cases := [][][]Cond{
		// closed range on a non-index dim
		{{{Dim: "country", Range: &Range{Min: 10, Max: 90}}}},
		// inclusive bounds: exactly the endpoints
		{{{Dim: "country", Range: &Range{Min: 25, Max: 25}}}},
		// open-ended via the int64 extremes
		{{{Dim: "country", Range: &Range{Min: 100, Max: math.MaxInt64}}}},
		{{{Dim: "country", Range: &Range{Min: math.MinInt64, Max: 24}}}},
		// combined with an indexed equality prefix
		{{{Dim: "source", Value: "s1"}, {Dim: "country", Range: &Range{Min: 20, Max: 100}}}},
		// combined with IN
		{{{Dim: "os", In: []string{"ios"}}, {Dim: "country", Range: &Range{Min: 0, Max: 200}}}},
		// range matching nothing
		{{{Dim: "country", Range: &Range{Min: 1000, Max: 2000}}}},
		// inverted bounds match nothing
		{{{Dim: "country", Range: &Range{Min: 20, Max: 10}}}},
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

func TestCondRangeDeltaAndExtras(t *testing.T) {
	s := numericStore(t, numericRecords())
	rt := newRefTable(numericSchema())
	rt.apply(numericRecords())
	fresh := []Record{
		// new numeric value only in the extras dictionary
		rec(ts(200), []float64{7, 1}, "s3", "a1", "p1", "55", "ios"),
		// shadow an existing row with a new country value
		rec(ts(200), []float64{11, 1}, "s1", "a1", "p1", "60", "ios"),
	}
	if err := s.Apply(fresh); err != nil {
		t.Fatal(err)
	}
	rt.apply(fresh)
	groups := [][]Cond{{{Dim: "country", Range: &Range{Min: 50, Max: 100}}}}
	got, err := s.QueryGroups(nil, groups)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, got, rt.query(groups)) // 90 + 55 + 60 rows
}

func TestCondRangeSurvivesCompactAndSnapshot(t *testing.T) {
	s := numericStore(t, numericRecords())
	if err := s.Apply([]Record{rec(ts(200), []float64{7, 1}, "s3", "a1", "p1", "55", "ios")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	groups := [][]Cond{{{Dim: "country", Range: &Range{Min: 50, Max: 100}}}}
	want, err := s.QueryGroups(nil, groups)
	if err != nil {
		t.Fatal(err)
	}
	// snapshot roundtrip: vals are not persisted and must be re-derived on load
	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := s.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	s2, err := New(numericSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.LoadSnapshot(path); err != nil {
		t.Fatal(err)
	}
	got, err := s2.QueryGroups(nil, groups)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, got, want)
	_ = os.Remove(path)
}

func TestCondRangeLeadingDimFullScan(t *testing.T) {
	// numeric leading index dim: range on it cannot use the prefix
	sc := Schema{
		Dims:      []Dim{{Name: "hour", Type: DimInt}, {Name: "account"}},
		IndexDims: 1,
		Metrics:   []string{"visits"},
	}
	s, err := New(sc, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.seed([]Record{
		{Dims: []string{"1", "a1"}, Metrics: []float64{1}, UpdatedAt: ts(100)},
		{Dims: []string{"2", "a1"}, Metrics: []float64{2}, UpdatedAt: ts(100)},
		{Dims: []string{"3", "a2"}, Metrics: []float64{4}, UpdatedAt: ts(100)},
	}); err != nil {
		t.Fatal(err)
	}
	before := s.Stats().FullScans
	got, err := s.Query(nil, []Cond{{Dim: "hour", Range: &Range{Min: 2, Max: 3}}})
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, got, []float64{6})
	if s.Stats().FullScans != before+1 {
		t.Fatal("leading-dim range did not count as a full scan")
	}
}

func TestCondRangeErrors(t *testing.T) {
	s := numericStore(t, numericRecords())
	r := &Range{Min: 0, Max: 1}
	if _, err := s.Query(nil, []Cond{{Dim: "os", Range: r}}); err == nil {
		t.Fatal("range on a non-numeric dim accepted")
	}
	if _, err := s.Query(nil, []Cond{{Dim: "country", Value: "10", Range: r}}); err == nil {
		t.Fatal("Value and Range together accepted")
	}
	if _, err := s.Query(nil, []Cond{{Dim: "country", In: []string{"10"}, Range: r}}); err == nil {
		t.Fatal("In and Range together accepted")
	}
	if _, err := s.Query(nil, []Cond{{Dim: "nope", Range: r}}); err == nil {
		t.Fatal("range on unknown dim accepted")
	}
	group := make([]Cond, 0, maxRanges+1)
	for i := 0; i <= maxRanges; i++ {
		group = append(group, Cond{Dim: "country", Range: r})
	}
	if _, err := s.Query(nil, group); err == nil {
		t.Fatal("too many ranges accepted")
	}

	// maxRanges is a per-QUERY total, not per-group: 9 ranges in one group
	// plus 8 in another (17 total, neither group alone over the limit) must
	// still be rejected.
	g1 := make([]Cond, 0, 9)
	for i := 0; i < 9; i++ {
		g1 = append(g1, Cond{Dim: "country", Range: r})
	}
	g2 := make([]Cond, 0, 8)
	for i := 0; i < 8; i++ {
		g2 = append(g2, Cond{Dim: "country", Range: r})
	}
	if _, err := s.Query(nil, g1, g2); !errors.Is(err, errTooManyRanges) {
		t.Fatalf("17 ranges split 9+8 across two groups: err = %v, want errTooManyRanges", err)
	}
}

func TestCondRangeAggsAndGroupBy(t *testing.T) {
	recs := numericRecords()
	s := numericStore(t, recs)
	rt := newRefTable(numericSchema())
	rt.apply(recs)
	groups := [][]Cond{{{Dim: "country", Range: &Range{Min: 10, Max: 100}}}}
	aggs := []Agg{{Op: AggCount}, {Metric: "visits", Op: AggSum}}

	got, err := s.QueryAggs(nil, aggs, groups)
	if err != nil {
		t.Fatal(err)
	}
	assertSameNaN(t, got, rt.queryAggs(aggs, groups))

	var res GroupedResult
	if err := s.QueryGroupBy(&res, []string{"source"}, aggs, groups); err != nil {
		t.Fatal(err)
	}
	assertGroupsEqual(t, groupedToMap(&res, 1, 2), rt.queryGroupBy([]string{"source"}, aggs, groups))
}

func TestQueryRangeZeroAlloc(t *testing.T) {
	// numeric country values so the range actually filters
	recs := numericRecords()
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		r := randomRecord(rng, 5)
		r.Dims[3] = strconv.Itoa(rng.Intn(200)) // numeric country
		recs = append(recs, r)
	}
	s := numericStore(t, recs)
	buf := make([]float64, 0, 2)
	groups := [][]Cond{
		{{Dim: "source", Value: "s1"}, {Dim: "country", Range: &Range{Min: 0, Max: 40}}},
		{{Dim: "country", Value: "+40"}},              // non-canonical spelling
		{{Dim: "country", In: []string{"10", "025"}}}, // mixed spellings
	}
	allocs := testing.AllocsPerRun(100, func() {
		var err error
		buf, err = s.QueryGroups(buf, groups)
		if err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("range query allocates: %v allocs/op", allocs)
	}
}

func TestCondRangeScanBudget(t *testing.T) {
	s, err := New(numericSchema(), Config{MaxScanRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.seed(numericRecords()); err != nil {
		t.Fatal(err)
	}
	_, err = s.Query(nil, []Cond{{Dim: "country", Range: &Range{Min: 0, Max: 100}}})
	if !errors.Is(err, ErrScanBudget) {
		t.Fatalf("err = %v, want ErrScanBudget", err)
	}
}

func TestNumericDimCanonicalIdentity(t *testing.T) {
	s := numericStore(t, numericRecords())
	rt := newRefTable(numericSchema())
	rt.apply(numericRecords())
	// same tuple, different spelling: "010" must upsert the "10" row
	up := []Record{rec(ts(200), []float64{11, 2}, "s1", "a1", "p1", "010", "ios")}
	if err := s.Apply(up); err != nil {
		t.Fatal(err)
	}
	rt.apply(up)
	if got := s.Rows() + s.DeltaRows(); got != len(numericRecords())+1 {
		// base row is shadowed, delta holds the upsert: 4 base + 1 delta
		t.Fatalf("rows+delta = %d, want %d (upsert must not create a new identity)", got, len(numericRecords())+1)
	}
	// every spelling of the same number finds the same row
	for _, spell := range []string{"10", "+10", "010", "0010"} {
		got, err := s.Query(nil, []Cond{{Dim: "country", Value: spell}})
		if err != nil {
			t.Fatalf("%q: %v", spell, err)
		}
		assertSame(t, got, []float64{11, 2})
	}
	// group-by key comes out canonical
	var res GroupedResult
	if err := s.QueryGroupBy(&res, []string{"country"}, []Agg{{Op: AggCount}}, [][]Cond{{}}); err != nil {
		t.Fatal(err)
	}
	assertGroupsEqual(t, groupedToMap(&res, 1, 1), rt.queryGroupBy([]string{"country"}, []Agg{{Op: AggCount}}, [][]Cond{{}}))
	for i := 0; i < res.N; i++ {
		if k := res.Keys[i]; k == "+10" || k == "010" {
			t.Fatalf("non-canonical group key %q", k)
		}
	}
}

func TestNumericDimNegativeZero(t *testing.T) {
	// -0 and 0 compare equal as numbers, so they must be ONE identity: the
	// canonical-string/float64 bijection would otherwise leak exactly the
	// spelling/value split this design exists to prevent.
	s := numericStore(t, numericRecords())
	if err := s.Apply([]Record{rec(ts(200), []float64{7, 1}, "s1", "a1", "p1", "-0", "ios")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply([]Record{rec(ts(300), []float64{9, 2}, "s1", "a1", "p1", "+0", "ios")}); err != nil {
		t.Fatal(err)
	}
	for _, spell := range []string{"0", "-0", "+0", "00"} {
		got, err := s.Query(nil, []Cond{{Dim: "country", Value: spell}})
		if err != nil {
			t.Fatalf("%q: %v", spell, err)
		}
		assertSame(t, got, []float64{9, 2}) // one row, latest upsert wins
	}
	got, err := s.Query(nil, []Cond{{Dim: "country", Range: &Range{Min: 0, Max: 0}}})
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, got, []float64{9, 2})
}

func TestNumericDimRejectsBadValues(t *testing.T) {
	bad := []Record{rec(ts(100), []float64{1, 1}, "s1", "a1", "p1", "oops", "ios")}
	s, err := New(numericSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.seed(bad); err == nil {
		t.Fatal("full build accepted a non-numeric value on a numeric dim")
	}
	if err := s.Apply(bad); err == nil {
		t.Fatal("Apply accepted a non-numeric value on a numeric dim")
	}
	if err := s.ReplaceAll(bad); err == nil {
		t.Fatal("ReplaceAll accepted a non-numeric value on a numeric dim")
	}
	for _, v := range []string{"NaN", "Inf", "-Inf", "1.5", "1.0", "1e3"} {
		r := []Record{rec(ts(100), []float64{1, 1}, "s1", "a1", "p1", v, "ios")}
		if err := s.Apply(r); err == nil {
			t.Fatalf("Apply accepted non-integer value %q", v)
		}
	}
}

func TestNumericCondValueErrors(t *testing.T) {
	s := numericStore(t, numericRecords())
	if _, err := s.Query(nil, []Cond{{Dim: "country", Value: "abc"}}); err == nil {
		t.Fatal("non-numeric equality value on a numeric dim accepted")
	}
	if _, err := s.Query(nil, []Cond{{Dim: "country", In: []string{"10", "abc"}}}); err == nil {
		t.Fatal("non-numeric IN value on a numeric dim accepted")
	}
}

func TestSnapshotRejectsNonCanonicalNumeric(t *testing.T) {
	// write a snapshot under a schema where country is NOT numeric, holding a
	// non-canonical spelling; loading under the numeric schema must refuse it
	// (DimType is outside the fingerprint, so only this check guards it)
	plain, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := plain.seed([]Record{rec(ts(100), []float64{1, 1}, "s1", "a1", "p1", "10.0", "ios")}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := plain.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	numeric, err := New(numericSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := numeric.LoadSnapshot(path); err == nil {
		t.Fatal("snapshot with non-canonical numeric dictionary entry accepted")
	}
	// canonical content loads fine
	if err := plain.seed([]Record{rec(ts(100), []float64{1, 1}, "s1", "a1", "p1", "10", "ios")}); err != nil {
		t.Fatal(err)
	}
	if err := plain.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	if err := numeric.LoadSnapshot(path); err != nil {
		t.Fatal(err)
	}
}
