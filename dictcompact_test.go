package facetta

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

// TestCompactReclaimsDict: every Compact also compacts the dictionaries --
// strings referenced by no surviving row are dropped and ids renumbered,
// without changing any query result (spec §4.1, §4.3).
func TestCompactReclaimsDict(t *testing.T) {
	s, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	clk := ts(1000)
	s.now = func() time.Time { return clk }

	var recs []Record
	for i := range 10 {
		recs = append(recs, rec(ts(int64(100+i)), []float64{float64(i), 1},
			fmt.Sprintf("s%d", i%3), fmt.Sprintf("a%d", i%5), fmt.Sprintf("p%d", i),
			"c0", fmt.Sprintf("o%d", i%2)))
	}
	if err := s.ReplaceAll(recs); err != nil {
		t.Fatal(err)
	}
	// tombstone rows 5..9: invisible immediately, reclaimed at next merge
	var tombs []Record
	for i := 5; i < 10; i++ {
		r := recs[i]
		r.UpdatedAt = ts(500)
		r.ExpireAt = ts(999)
		tombs = append(tombs, r)
	}
	if err := s.Apply(tombs); err != nil {
		t.Fatal(err)
	}

	groups := [][]Cond{
		{},                                // match all
		{{Dim: "source", Value: "s1"}},    // survives
		{{Dim: "publisher", Value: "p7"}}, // dies with its tombstoned row
	}
	var before [][]float64
	for _, g := range groups {
		got, err := s.Query(nil, g)
		if err != nil {
			t.Fatal(err)
		}
		before = append(before, slices.Clone(got))
	}

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := s.Rows(); got != 5 {
		t.Fatalf("rows after Compact = %d, want 5", got)
	}
	// live cardinality: sources 3, accounts 5, publishers 5, country 1, os 2;
	// the tombstoned rows' strings are gone with them
	if want := []int{3, 5, 5, 1, 2}; !slices.Equal(s.Stats().DictEntries, want) {
		t.Fatalf("DictEntries after Compact = %v, want %v", s.Stats().DictEntries, want)
	}
	if !slices.IsSorted(s.view.Load().base.keys) {
		t.Fatal("Compact output keys must stay sorted")
	}
	for i, g := range groups {
		got, err := s.Query(nil, g)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, before[i]) {
			t.Fatalf("group %d: query changed after Compact: got %v, want %v", i, got, before[i])
		}
	}
}

// TestCompactRecoversKeyOverflow: a view whose LIVE index-dim widths exceed
// 64 bits still refuses to merge, but once tombstones shrink the live
// cardinality, the next ordinary Compact renumbers and succeeds -- no
// dedicated recovery operation needed (spec §4.2).
func TestCompactRecoversKeyOverflow(t *testing.T) {
	sc := Schema{IndexDims: 16, Metrics: []string{"m"}}
	for i := range 16 {
		sc.Dims = append(sc.Dims, Dim{Name: fmt.Sprintf("d%d", i)})
	}
	s, err := New(sc, Config{})
	if err != nil {
		t.Fatal(err)
	}
	clk := ts(1000)
	s.now = func() time.Time { return clk }

	dims := func(i int) []string {
		v := fmt.Sprintf("v%d", i)
		return slices.Repeat([]string{v}, 16)
	}
	var recs []Record
	for i := range 32 {
		recs = append(recs, Record{Dims: dims(i), Metrics: []float64{float64(i)}, UpdatedAt: ts(int64(100 + i))})
	}
	if err := s.Apply(recs); err != nil {
		t.Fatal(err)
	}
	// 16 index dims x dictBits(32)=5 -> 80 bits of LIVE cardinality: refuse
	if err := s.Compact(); !errors.Is(err, ErrKeyOverflow) {
		t.Fatalf("Compact = %v, want ErrKeyOverflow", err)
	}
	// tombstone all but v0, v1: live cardinality 2 per dim -> 16 bits
	var tombs []Record
	for i := 2; i < 32; i++ {
		tombs = append(tombs, Record{Dims: dims(i), Metrics: []float64{0}, UpdatedAt: ts(500), ExpireAt: ts(999)})
	}
	if err := s.Apply(tombs); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact after tombstones = %v, want success", err)
	}
	if got := s.Rows(); got != 2 {
		t.Fatalf("rows = %d, want 2", got)
	}
	st := s.Stats()
	if st.IndexKeyBits != 16 {
		t.Fatalf("IndexKeyBits = %d, want 16", st.IndexKeyBits)
	}
	for d, n := range st.DictEntries {
		if n != 2 {
			t.Fatalf("DictEntries[%d] = %d, want 2", d, n)
		}
	}
	got, err := s.Query(nil, []Cond{{Dim: "d3", Value: "v1"}})
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 1 {
		t.Fatalf("query v1 = %v, want [1]", got)
	}
	// the store is fully unblocked: normal ingestion works again
	if err := s.Apply([]Record{{Dims: dims(40), Metrics: []float64{40}, UpdatedAt: ts(600)}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact after re-ingest = %v, want success", err)
	}
	if got := s.Rows(); got != 3 {
		t.Fatalf("rows after re-ingest = %d, want 3", got)
	}
}

// TestCompactDictGatingByInterval: with DictCompactInterval set, merges
// inside the window keep ids stable (garbage retained, cheap path); once the
// interval elapses the next Compact renumbers and reclaims.
func TestCompactDictGatingByInterval(t *testing.T) {
	s, err := New(testSchema(), Config{DictCompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	clk := ts(1000)
	s.now = func() time.Time { return clk }

	var recs []Record
	for i := range 10 {
		recs = append(recs, rec(ts(int64(100+i)), []float64{float64(i), 1},
			fmt.Sprintf("s%d", i%3), fmt.Sprintf("a%d", i%5), fmt.Sprintf("p%d", i),
			"c0", fmt.Sprintf("o%d", i%2)))
	}
	if err := s.ReplaceAll(recs); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil { // first compact: gate never satisfied before -> renumbers
		t.Fatal(err)
	}
	var tombs []Record
	for i := 5; i < 10; i++ {
		r := recs[i]
		r.UpdatedAt = ts(500)
		r.ExpireAt = ts(999)
		tombs = append(tombs, r)
	}
	if err := s.Apply(tombs); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil { // inside the window: id-stable, garbage retained
		t.Fatal(err)
	}
	if got := s.Rows(); got != 5 {
		t.Fatalf("rows after gated Compact = %d, want 5", got)
	}
	if want := []int{3, 5, 10, 1, 2}; !slices.Equal(s.Stats().DictEntries, want) {
		t.Fatalf("DictEntries inside gate = %v, want %v (garbage retained)", s.Stats().DictEntries, want)
	}

	clk = clk.Add(time.Hour + time.Second)
	if err := s.Compact(); err != nil { // window elapsed: renumbering merge
		t.Fatal(err)
	}
	if want := []int{3, 5, 5, 1, 2}; !slices.Equal(s.Stats().DictEntries, want) {
		t.Fatalf("DictEntries after gate elapsed = %v, want %v", s.Stats().DictEntries, want)
	}
	got, err := s.Query(nil, []Cond{{Dim: "source", Value: "s1"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []float64{1 + 4, 2}
	if !slices.Equal(got, want) {
		t.Fatalf("query after renumber = %v, want %v", got, want)
	}
}

// TestCompactOverflowRetriesRenumberInsideGate: an id-stable merge refused
// with ErrKeyOverflow retries once with dictionary compaction regardless of
// the interval gate, so overflow recovery is never delayed by gating.
func TestCompactOverflowRetriesRenumberInsideGate(t *testing.T) {
	sc := Schema{IndexDims: 16, Metrics: []string{"m"}}
	for i := range 16 {
		sc.Dims = append(sc.Dims, Dim{Name: fmt.Sprintf("d%d", i)})
	}
	s, err := New(sc, Config{DictCompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	clk := ts(1000)
	s.now = func() time.Time { return clk }

	dims := func(i int) []string {
		v := fmt.Sprintf("v%d", i)
		return slices.Repeat([]string{v}, 16)
	}
	if err := s.Apply([]Record{
		{Dims: dims(0), Metrics: []float64{0}, UpdatedAt: ts(100)},
		{Dims: dims(1), Metrics: []float64{1}, UpdatedAt: ts(101)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil { // renumbering merge: gate timestamp set
		t.Fatal(err)
	}
	// grow the combined dict space to 32 per dim (80 bits), then tombstone the
	// newcomers so LIVE cardinality stays 2 per dim -- all inside the gate
	var batch []Record
	for i := 2; i < 32; i++ {
		batch = append(batch, Record{Dims: dims(i), Metrics: []float64{float64(i)}, UpdatedAt: ts(int64(100 + i))})
	}
	for i := 2; i < 32; i++ {
		batch = append(batch, Record{Dims: dims(i), Metrics: []float64{0}, UpdatedAt: ts(500), ExpireAt: ts(999)})
	}
	if err := s.Apply(batch); err != nil {
		t.Fatal(err)
	}
	// id-stable widths (32/dim -> 80 bits) overflow; the retry renumbers to
	// the live 2/dim (16 bits) and succeeds within the gate window
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact = %v, want in-gate renumber retry to succeed", err)
	}
	if got := s.Rows(); got != 2 {
		t.Fatalf("rows = %d, want 2", got)
	}
	st := s.Stats()
	if st.IndexKeyBits != 16 {
		t.Fatalf("IndexKeyBits = %d, want 16", st.IndexKeyBits)
	}
	for d, n := range st.DictEntries {
		if n != 2 {
			t.Fatalf("DictEntries[%d] = %d, want 2", d, n)
		}
	}
}
