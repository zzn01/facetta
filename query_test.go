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
