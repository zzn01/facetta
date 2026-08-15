package facetta

import "testing"

func TestComputeShifts(t *testing.T) {
	// cards 4,3,70000 -> widths 2,2,17 -> shifts 19,17,0
	shifts, err := computeShifts([]int{4, 3, 70000, 9, 9}, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []uint{19, 17, 0}
	for i := range want {
		if shifts[i] != want[i] {
			t.Fatalf("shifts = %v, want %v", shifts, want)
		}
	}
	if dictBits(0) != 1 || dictBits(1) != 1 || dictBits(2) != 1 || dictBits(3) != 2 || dictBits(65536) != 16 {
		t.Fatal("dictBits wrong")
	}
	// 5 dims x 20 bits > 64 -> overflow
	if _, err := computeShifts([]int{1 << 20, 1 << 20, 1 << 20, 1 << 20}, 4); err != ErrKeyOverflow {
		t.Fatalf("want ErrKeyOverflow, got %v", err)
	}
}

func TestBuildFromRecords(t *testing.T) {
	sc := testSchema()
	recs := []Record{
		rec(ts(300), []float64{40, 4}, "s2", "a2", "p1", "DE", "ios"),
		rec(ts(100), []float64{10, 1}, "s1", "a1", "p1", "US", "ios"),
		rec(ts(200), []float64{11, 2}, "s1", "a1", "p1", "US", "ios"), // dup, newer wins
		rec(ts(40), []float64{99, 9}, "s0", "a0", "p0", "FR", "web"),  // below TTL cutoff
	}
	snap, err := buildFromRecords(&sc, recs, ts(50).UnixMilli(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if snap.rows() != 2 {
		t.Fatalf("rows = %d, want 2 (dedup + TTL)", snap.rows())
	}
	// keys sorted ascending
	for i := 1; i < len(snap.keys); i++ {
		if snap.keys[i-1] > snap.keys[i] {
			t.Fatal("keys not sorted")
		}
	}
	// the deduped row kept the newer metrics
	found := false
	for r := 0; r < snap.rows(); r++ {
		if snap.dicts[0].strs[snap.dims[0][r]] == "s1" {
			found = true
			if snap.mets[0][r] != 11 || snap.updated[r] != ts(200).UnixMilli() {
				t.Fatalf("dedup kept wrong version: %v @ %d", snap.mets[0][r], snap.updated[r])
			}
		}
	}
	if !found {
		t.Fatal("s1 row missing")
	}
	if snap.maxUpdated != ts(300).UnixMilli() {
		t.Fatalf("maxUpdated = %d", snap.maxUpdated)
	}
	// record arity validation
	if _, err := buildFromRecords(&sc, []Record{{Dims: []string{"x"}, Metrics: []float64{1, 2}}}, 0, 0); err == nil {
		t.Fatal("bad arity accepted")
	}
}
