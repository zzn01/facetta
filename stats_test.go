package facetta

import (
	"fmt"
	"slices"
	"testing"
)

// TestStatsDictObservability checks the dictionary/key-width water levels the
// host uses to decide when a FullCompact or ReplaceAll is due (spec §2.4).
func TestStatsDictObservability(t *testing.T) {
	s, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	var recs []Record
	for i := range 10 {
		recs = append(recs, rec(ts(int64(100+i)), []float64{1, 1},
			fmt.Sprintf("s%d", i%3), fmt.Sprintf("a%d", i%5), fmt.Sprintf("p%d", i),
			"c0", fmt.Sprintf("o%d", i%2)))
	}
	if err := s.ReplaceAll(recs); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	// distinct per dim: source 3, account 5, publisher 10, country 1, os 2
	if want := []int{3, 5, 10, 1, 2}; !slices.Equal(st.DictEntries, want) {
		t.Fatalf("DictEntries = %v, want %v", st.DictEntries, want)
	}
	// index dims (3): dictBits(3)+dictBits(5)+dictBits(10) = 2+3+4
	if st.IndexKeyBits != 9 {
		t.Fatalf("IndexKeyBits = %d, want 9", st.IndexKeyBits)
	}

	// new strings land in extras and must be counted immediately
	if err := s.Apply([]Record{
		rec(ts(200), []float64{1, 1}, "s3", "a0", "p0", "c0", "o0"),
		rec(ts(201), []float64{1, 1}, "s4", "a0", "p1", "c0", "o0"),
	}); err != nil {
		t.Fatal(err)
	}
	st = s.Stats()
	if want := []int{5, 5, 10, 1, 2}; !slices.Equal(st.DictEntries, want) {
		t.Fatalf("DictEntries after Apply = %v, want %v", st.DictEntries, want)
	}
	// source card 3 -> 5: dictBits(5)+dictBits(5)+dictBits(10) = 3+3+4
	if st.IndexKeyBits != 10 {
		t.Fatalf("IndexKeyBits after Apply = %d, want 10", st.IndexKeyBits)
	}
}
