package facetta

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// buildRowsRange generates the deterministic rows [start, start+n) of the same
// distribution buildRows produces, without materializing the whole [0, N) slice.
// It reproduces buildRows exactly for the row at absolute index i: the rng is
// seeded per-index so the sequence is position-stable regardless of chunking,
// and publisher spread widens with total to keep tuples ~unique at scale.
func buildRowsRange(start, n, total int) []Record {
	spread := max(30000, total/3000)
	recs := make([]Record, n)
	for j := range recs {
		i := start + j
		// Per-index seed keeps a given absolute row identical across any
		// chunking, so re-generation is reproducible for upsert-invariance.
		rng := rand.New(rand.NewSource(int64(i) + 1))
		recs[j] = Record{
			Dims: []string{
				fmt.Sprintf("src%d", rng.Intn(50)),
				fmt.Sprintf("acc%d", rng.Intn(2000)),
				fmt.Sprintf("pub%d", i%spread),
				fmt.Sprintf("c%d", rng.Intn(200)),
				fmt.Sprintf("os%d", rng.Intn(8)),
			},
			Metrics:   []float64{float64(rng.Intn(1000)), float64(rng.Intn(1000)) / 100},
			UpdatedAt: time.Unix(int64(1000000+i), 0),
		}
	}
	return recs
}

// TestQueryLatencyDuringCompact proves the read path never blocks on Compact:
// readers hammer the store while a writer applies delta and forces repeated
// compactions. The applied records exactly duplicate existing base rows (same
// Dims/Metrics, newer UpdatedAt), so upsert keeps every query result invariant
// across every Apply and every view swap. Any reader observing a different
// result means it saw a torn/inconsistent view.
func TestQueryLatencyDuringCompact(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}
	const n = 3_000_000

	s, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceAll(buildRows(n)); err != nil {
		t.Fatal(err)
	}

	// Two fixed queries whose results must stay constant for the whole run.
	indexedGroups := [][]Cond{
		{{Dim: "source", Value: "src7"}, {Dim: "account", Value: "acc42"}},
		{{Dim: "source", Value: "src9"}, {Dim: "account", Value: "acc7"}},
	}
	fullScanGroups := [][]Cond{{{Dim: "os", Value: "os3"}}} // "os" is unindexed -> full base scan

	wantIndexed, err := s.QueryGroups(make([]float64, 0, 2), indexedGroups)
	if err != nil {
		t.Fatal(err)
	}
	wantFull, err := s.QueryGroups(make([]float64, 0, 2), fullScanGroups)
	if err != nil {
		t.Fatal(err)
	}
	wantIndexed = append([]float64(nil), wantIndexed...)
	wantFull = append([]float64(nil), wantFull...)
	t.Logf("expected indexed=%v full-scan=%v", wantIndexed, wantFull)

	const compactions = 10
	var (
		wg       sync.WaitGroup
		done     = make(chan struct{})
		stop     = make(chan struct{})
		readErrs = make([]error, 4)
		maxLat   = make([]time.Duration, 4)
		isFull   = []bool{false, false, true, true} // 2 indexed readers, 2 full-scan
	)

	// Precompute the duplicate batches OUTSIDE the writer loop: each is a slice
	// of existing base rows with only UpdatedAt bumped, so the upsert replaces
	// them with identical Dims/Metrics and every query result is invariant.
	// (Regenerating buildRows(n) inside the loop would dominate wall time.)
	all := buildRows(n)
	batches := make([][]Record, compactions)
	for c := range batches {
		src := all[c*20_000 : c*20_000+20_000]
		dup := make([]Record, len(src))
		for i, r := range src {
			dup[i] = r
			dup[i].UpdatedAt = r.UpdatedAt.Add(time.Hour) // newer -> upsert wins, values identical
		}
		batches[c] = dup
	}
	all = nil

	// Writer: force >= 10 compactions, each preceded by an Apply of the
	// duplicate rows, so the view swaps repeatedly with invariant results.
	var compactDurs []time.Duration
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for c := 0; c < compactions; c++ {
			if err := s.Apply(batches[c]); err != nil {
				t.Errorf("apply: %v", err)
				return
			}
			start := time.Now()
			if err := s.Compact(); err != nil {
				t.Errorf("compact: %v", err)
				return
			}
			compactDurs = append(compactDurs, time.Since(start))
		}
	}()

	// Readers: tight query loop until the writer signals done.
	for ri := 0; ri < 4; ri++ {
		wg.Add(1)
		go func(ri int) {
			defer wg.Done()
			buf := make([]float64, 0, 2)
			groups := indexedGroups
			want := wantIndexed
			if isFull[ri] {
				groups = fullScanGroups
				want = wantFull
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				t0 := time.Now()
				res, qerr := s.QueryGroups(buf, groups)
				lat := time.Since(t0)
				if qerr != nil {
					readErrs[ri] = qerr
					return
				}
				buf = res
				if lat > maxLat[ri] {
					maxLat[ri] = lat
				}
				// Compaction rewrites the base and reorders rows, so the
				// float64 sum accumulates in a different order across swaps:
				// a tiny relative difference is expected and semantically
				// identical. A torn/inconsistent view would differ grossly
				// (missing or double-counted rows), which this tolerance
				// still catches.
				for m := range want {
					if !nearlyEqual(buf[m], want[m]) {
						readErrs[ri] = fmt.Errorf("reader %d: torn view: got %v want %v", ri, buf, want)
						return
					}
				}
			}
		}(ri)
	}

	<-done
	close(stop)
	wg.Wait()

	for ri, e := range readErrs {
		if e != nil {
			t.Errorf("reader %d error: %v", ri, e)
		}
	}

	st := s.Stats()
	var indexedMax, fullMax time.Duration
	for ri := 0; ri < 4; ri++ {
		t.Logf("reader %d (%s) max latency: %v", ri, readerKind(isFull[ri]), maxLat[ri])
		if isFull[ri] {
			fullMax = max(fullMax, maxLat[ri])
		} else {
			indexedMax = max(indexedMax, maxLat[ri])
		}
	}
	var totalCompact time.Duration
	for _, d := range compactDurs {
		totalCompact += d
	}
	median := time.Duration(0)
	if len(compactDurs) > 0 {
		median = totalCompact / time.Duration(len(compactDurs))
	}
	t.Logf("compactions=%d mean-compact=%v last-compact=%v indexed-max=%v full-scan-max=%v",
		st.Compactions, median, st.LastCompaction, indexedMax, fullMax)

	if st.Compactions < compactions {
		t.Errorf("only %d compactions, want >= %d", st.Compactions, compactions)
	}

	// Primary lock-free proof (machine- and race-independent): if an indexed
	// read blocked on the compaction lock its latency would be ~= a full
	// compaction. It is instead a small fraction of it, so reads never wait on
	// Compact. Guard against a degenerate near-zero mean.
	if median > 20*time.Millisecond && indexedMax >= median/2 {
		t.Errorf("indexed query max latency %v >= half the mean compaction %v: read may be blocking on compact",
			indexedMax, median)
	}

	// Absolute ceiling: a coarse sanity backstop, secondary to the relative
	// bound above. Even a lock-free reader is paused by the GC stop-the-world
	// triggered by the compactor allocating a fresh ~150MB snapshot every
	// iteration, so at 3M live rows single-query spikes of ~60ms are ordinary
	// GC noise (measured), not lock blocking. The ceiling is set well below the
	// ~300ms mean-compact synchronous-blocking signature so a real block still
	// trips it. Under -race, allocation and STW pauses are amplified ~10x, so
	// the ceiling is relaxed further.
	ceiling := 150 * time.Millisecond
	if raceEnabled {
		ceiling = 600 * time.Millisecond
	}
	if indexedMax >= ceiling {
		t.Errorf("indexed query max latency %v >= %v ceiling (race=%v)", indexedMax, ceiling, raceEnabled)
	}
}

// nearlyEqual reports whether a and b are equal within a small relative
// tolerance, absorbing float64 summation-order differences from base-row
// reordering across compactions without masking a torn view (which differs by
// whole rows).
func nearlyEqual(a, b float64) bool {
	const rel = 1e-9
	d := a - b
	if d < 0 {
		d = -d
	}
	scale := a
	if scale < 0 {
		scale = -scale
	}
	if bb := b; bb < 0 {
		bb = -bb
		if bb > scale {
			scale = bb
		}
	} else if bb > scale {
		scale = bb
	}
	return d <= rel*scale || d < 1e-6
}

func readerKind(full bool) string {
	if full {
		return "full-scan"
	}
	return "indexed"
}

// TestStressScale probes scales beyond the 5M design point. It runs only when
// FACETTA_STRESS_ROWS is set, and ingests in 1M-row chunks to bound peak memory, so it
// runs 30M on a 16GB box and 100M unchanged on a bigger one. Assertions are
// soft (this documents scaling) except correctness invariants.
func TestStressScale(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}
	// On-demand scale probe: the 5M design point is already covered by
	// TestCapacity5M's hard assertions, so without an explicit row count
	// there is nothing new to measure here.
	n, _ := strconv.Atoi(os.Getenv("FACETTA_STRESS_ROWS"))
	if n <= 0 {
		t.Skip("set FACETTA_STRESS_ROWS=<n> to run the scale probe")
	}
	t.Logf("rows=%d (from FACETTA_STRESS_ROWS) estimated resident footprint ~%.1f GB (N*~60B)",
		n, float64(n)*60/(1<<30))

	s, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Chunked ingestion: generate + Apply 1M at a time, compacting whenever the
	// delta grows past 2M rows (and once at the end) to exercise the real
	// incremental path without materializing all N records at once.
	const chunk = 1_000_000
	start := time.Now()
	for off := 0; off < n; off += chunk {
		c := min(chunk, n-off)
		if err := s.Apply(buildRowsRange(off, c, n)); err != nil {
			t.Fatal(err)
		}
		if s.DeltaRows() >= 2_000_000 {
			if err := s.Compact(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	ingestDur := time.Since(start)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	resident := after.HeapAlloc - before.HeapAlloc

	rows := s.Rows()
	t.Logf("ingest+compact=%v rows=%d resident=%.1f MB", ingestDur, rows, float64(resident)/(1<<20))

	// Correctness: dedupe shrinkage bound (tuples ~unique by construction).
	if float64(rows) <= 0.9*float64(n) {
		t.Errorf("rows %d <= 0.9*N (%d): unexpected dedupe shrinkage", rows, int(0.9*float64(n)))
	}

	// Time one full-size Compact after a small fresh delta.
	if err := s.Apply(buildRowsRange(n, 100_000, n)); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	t.Logf("final full-size compact=%v", time.Since(start))

	// Indexed query: correctness across a compact + latency + zero-alloc.
	indexedGroups := [][]Cond{{{Dim: "source", Value: "src7"}, {Dim: "account", Value: "acc42"}}}
	buf := make([]float64, 0, 2)
	wantIdx, err := s.QueryGroups(buf, indexedGroups)
	if err != nil {
		t.Fatal(err)
	}
	wantIdx = append([]float64(nil), wantIdx...)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	gotIdx, err := s.QueryGroups(buf, indexedGroups)
	if err != nil {
		t.Fatal(err)
	}
	for m := range wantIdx {
		if gotIdx[m] != wantIdx[m] {
			t.Errorf("indexed result changed across compact: got %v want %v", gotIdx, wantIdx)
		}
	}

	const iters = 10_000
	start = time.Now()
	for range iters {
		if buf, err = s.QueryGroups(buf, indexedGroups); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("indexed query: %v/op", time.Since(start)/iters)

	if allocs := testing.AllocsPerRun(100, func() {
		buf, _ = s.QueryGroups(buf, indexedGroups)
	}); allocs != 0 {
		t.Errorf("indexed query allocated %v/op, want 0", allocs)
	}

	// Full-scan query on an unindexed dim, timed.
	fullScanGroups := [][]Cond{{{Dim: "os", Value: "os3"}}}
	start = time.Now()
	if _, err := s.QueryGroups(buf, fullScanGroups); err != nil {
		t.Fatal(err)
	}
	t.Logf("full-scan query: %v", time.Since(start))

	// Snapshot round-trip timing.
	path := t.TempDir() + "/snap.bin"
	start = time.Now()
	if err := s.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	saveDur := time.Since(start)
	s2, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	if err := s2.LoadSnapshot(path); err != nil {
		t.Fatal(err)
	}
	t.Logf("snapshot save=%v load=%v rows=%d", saveDur, time.Since(start), s2.Rows())
}
