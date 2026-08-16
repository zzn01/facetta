package facetta

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

// buildRows generates n records with realistic cardinalities:
// source ~50, account ~2k, publisher ~30k, country ~200, os ~8.
func buildRows(n int) []Record {
	rng := rand.New(rand.NewSource(42))
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{
			Dims: []string{
				fmt.Sprintf("src%d", rng.Intn(50)),
				fmt.Sprintf("acc%d", rng.Intn(2000)),
				fmt.Sprintf("pub%d", i%30000), // spread publishers so tuples are mostly unique
				fmt.Sprintf("c%d", rng.Intn(200)),
				fmt.Sprintf("os%d", rng.Intn(8)),
			},
			Metrics:   []float64{float64(rng.Intn(1000)), float64(rng.Intn(1000)) / 100},
			UpdatedAt: time.Unix(int64(1000000+i), 0),
		}
	}
	return recs
}

func benchStore(b *testing.B, n int) *Store {
	b.Helper()
	s, err := New(testSchema(), Config{})
	if err != nil {
		b.Fatal(err)
	}
	if err := s.ReplaceAll(buildRows(n)); err != nil {
		b.Fatal(err)
	}
	return s
}

func BenchmarkQueryIndexed1M(b *testing.B) {
	s := benchStore(b, 1_000_000)
	buf := make([]float64, 0, 2)
	groups := [][]Cond{
		{{Dim: "source", Value: "src7"}, {Dim: "account", Value: "acc42"}},
		{{Dim: "source", Value: "src9"}, {Dim: "account", Value: "acc7"}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		buf, err = s.QueryGroups(buf, groups)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryFullScan1M measures the degraded path: "os" is not one of
// the leading IndexDims, so plan() falls back to a full base scan.
func BenchmarkQueryFullScan1M(b *testing.B) {
	s := benchStore(b, 1_000_000)
	buf := make([]float64, 0, 2)
	groups := [][]Cond{{{Dim: "os", Value: "os3"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		buf, err = s.QueryGroups(buf, groups)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryMultiGroup1M measures the interval-union sweep with 8
// indexed groups, some spanning overlapping index ranges.
func BenchmarkQueryMultiGroup1M(b *testing.B) {
	s := benchStore(b, 1_000_000)
	buf := make([]float64, 0, 2)
	groups := [][]Cond{
		{{Dim: "source", Value: "src0"}, {Dim: "account", Value: "acc10"}},
		{{Dim: "source", Value: "src0"}, {Dim: "account", Value: "acc20"}},
		{{Dim: "source", Value: "src1"}, {Dim: "account", Value: "acc30"}},
		{{Dim: "source", Value: "src7"}, {Dim: "account", Value: "acc42"}},
		{{Dim: "source", Value: "src9"}, {Dim: "account", Value: "acc7"}},
		{{Dim: "source", Value: "src12"}, {Dim: "account", Value: "acc99"}},
		{{Dim: "source", Value: "src12"}, {Dim: "account", Value: "acc100"}},
		{{Dim: "source", Value: "src33"}, {Dim: "account", Value: "acc1500"}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		buf, err = s.QueryGroups(buf, groups)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryAggsIndexed1M runs the same indexed 2-group query as
// BenchmarkQueryIndexed1M through QueryAggs with four aggregate columns.
func BenchmarkQueryAggsIndexed1M(b *testing.B) {
	s := benchStore(b, 1_000_000)
	buf := make([]float64, 0, 4)
	aggs := []Agg{
		{Op: AggCount},
		{Metric: "visits", Op: AggSum},
		{Metric: "visits", Op: AggMin},
		{Metric: "revenue", Op: AggAvg},
	}
	groups := [][]Cond{
		{{Dim: "source", Value: "src7"}, {Dim: "account", Value: "acc42"}},
		{{Dim: "source", Value: "src9"}, {Dim: "account", Value: "acc7"}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		buf, err = s.QueryAggs(buf, aggs, groups)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryAggsDistinct1M adds a COUNT(DISTINCT publisher) column to the
// indexed query; the per-query cost is one ~30k-bit bitmap allocation plus a
// test-and-set per matched row.
func BenchmarkQueryAggsDistinct1M(b *testing.B) {
	s := benchStore(b, 1_000_000)
	buf := make([]float64, 0, 2)
	aggs := []Agg{
		{Dim: "publisher", Op: AggDistinct},
		{Metric: "visits", Op: AggSum},
	}
	groups := [][]Cond{{{Dim: "source", Value: "src7"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		buf, err = s.QueryAggs(buf, aggs, groups)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGroupByIndexed1M groups an indexed ~20k-row candidate range
// (source=src7) by the 8-value os dim, on a reused GroupedResult.
func BenchmarkGroupByIndexed1M(b *testing.B) {
	s := benchStore(b, 1_000_000)
	var res GroupedResult
	aggs := []Agg{{Op: AggCount}, {Metric: "visits", Op: AggSum}}
	groups := [][]Cond{{{Dim: "source", Value: "src7"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.QueryGroupBy(&res, []string{"os"}, aggs, groups); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryWithDelta1M runs the same indexed 2-group query as
// BenchmarkQueryIndexed1M, but against a store with a ~10k-row delta overlay
// applied (no Compact), quantifying the delta linear-scan overhead.
func BenchmarkQueryWithDelta1M(b *testing.B) {
	s := benchStore(b, 1_000_000)
	if err := s.Apply(buildRows(10_000)); err != nil {
		b.Fatal(err)
	}
	buf := make([]float64, 0, 2)
	groups := [][]Cond{
		{{Dim: "source", Value: "src7"}, {Dim: "account", Value: "acc42"}},
		{{Dim: "source", Value: "src9"}, {Dim: "account", Value: "acc7"}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		buf, err = s.QueryGroups(buf, groups)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkApply1K measures the cost of Apply-ing a 1000-record batch onto a
// 1M-row store. Batches are pre-generated (varied per iteration so dedupe
// doesn't collapse them) outside the timer. Apply deep-copies the delta on
// every call, so the delta is periodically folded back in with a Compact
// (timer stopped) to keep it from growing unboundedly across iterations.
func BenchmarkApply1K(b *testing.B) {
	s := benchStore(b, 1_000_000)
	const batchRows = 1000
	batches := make([][]Record, b.N)
	for i := range batches {
		batches[i] = buildRows(batchRows * (i + 2))[batchRows*(i+1) : batchRows*(i+2)]
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Apply(batches[i]); err != nil {
			b.Fatal(err)
		}
		if s.DeltaRows() > 50_000 {
			b.StopTimer()
			if err := s.Compact(); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	}
}

// BenchmarkApplySmallOnLargeDelta measures a 10-record Apply against a store
// whose delta already holds ~100k rows: the copy-on-write cost of one Apply
// as a function of the standing delta size (spec: apply write-path cost).
func BenchmarkApplySmallOnLargeDelta(b *testing.B) {
	s := benchStore(b, 200_000)
	if err := s.Apply(buildRows(300_000)[200_000:]); err != nil {
		b.Fatal(err)
	}
	const batchRows = 10
	all := buildRows(300_000 + batchRows*b.N)
	batches := make([][]Record, b.N)
	for i := range batches {
		batches[i] = all[300_000+batchRows*i : 300_000+batchRows*(i+1)]
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Apply(batches[i]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReplaceAll1M measures buildFromRecords throughput: a full
// reconcile build of 1M records per iteration (the hourly-reconcile cost).
func BenchmarkReplaceAll1M(b *testing.B) {
	recs := buildRows(1_000_000)
	s, err := New(testSchema(), Config{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.ReplaceAll(recs); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSaveSnapshot1M measures persistence write throughput at 1M rows.
func BenchmarkSaveSnapshot1M(b *testing.B) {
	s := benchStore(b, 1_000_000)
	path := b.TempDir() + "/snap.bin"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.SaveSnapshot(path); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLoadSnapshot1M measures persistence read throughput at 1M rows.
// A fresh Store is created inside the loop (cheap: it holds no rows yet) so
// each iteration loads into a clean store, matching a real cold-start path.
func BenchmarkLoadSnapshot1M(b *testing.B) {
	src := benchStore(b, 1_000_000)
	path := b.TempDir() + "/snap.bin"
	if err := src.SaveSnapshot(path); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := New(testSchema(), Config{})
		if err != nil {
			b.Fatal(err)
		}
		if err := s.LoadSnapshot(path); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompact1M re-applies a fresh 1k-row delta every iteration (timer
// stopped) so every measured Compact merges a real delta, not a zero delta.
func BenchmarkCompact1M(b *testing.B) {
	s := benchStore(b, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if err := s.Apply(buildRows(1000)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := s.Compact(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompactIDStable1M measures the gated fast path: with
// DictCompactInterval open-ended, merges keep ids stable and skip the
// mark/renumber passes.
func BenchmarkCompactIDStable1M(b *testing.B) {
	s, err := New(testSchema(), Config{DictCompactInterval: 24 * time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	if err := s.ReplaceAll(buildRows(1_000_000)); err != nil {
		b.Fatal(err)
	}
	// consume the store's one initial renumbering merge so every timed
	// iteration takes the id-stable path
	if err := s.Apply(buildRows(1000)); err != nil {
		b.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if err := s.Apply(buildRows(1000)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := s.Compact(); err != nil {
			b.Fatal(err)
		}
	}
}

// TestCapacity5M asserts the spec §3 quantitative targets at 5M rows.
// Slow (~1 min); skipped with -short.
func TestCapacity5M(t *testing.T) {
	if testing.Short() {
		t.Skip("capacity test skipped in -short mode")
	}
	const n = 5_000_000

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	recs := buildRows(n)
	s, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := s.ReplaceAll(recs); err != nil {
		t.Fatal(err)
	}
	buildDur := time.Since(start)

	recs = nil
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	resident := after.HeapAlloc - before.HeapAlloc
	t.Logf("rows=%d resident=%.1f MB build=%v", s.Rows(), float64(resident)/(1<<20), buildDur)
	if resident > 400<<20 {
		t.Errorf("resident memory %.1f MB > 400 MB target", float64(resident)/(1<<20))
	}
	// GC-friendliness: object count scales with dictionary cardinality
	// (~32k publishers dominate), not with 5M rows.
	if objs := after.HeapObjects - before.HeapObjects; objs > 1_000_000 {
		t.Errorf("heap objects grew by %d; data area must be pointer-free", objs)
	}

	// compaction < 2s with a 1% delta
	if err := s.Apply(buildRows(50_000)); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	compactDur := time.Since(start)
	t.Logf("compact 5M+50k: %v", compactDur)
	if compactDur > 2*time.Second {
		t.Errorf("compaction took %v > 2s target", compactDur)
	}

	// typical indexed query <= 5µs
	buf := make([]float64, 0, 2)
	groups := [][]Cond{{{Dim: "source", Value: "src7"}, {Dim: "account", Value: "acc42"}}}
	start = time.Now()
	const iters = 10_000
	for range iters {
		if buf, err = s.QueryGroups(buf, groups); err != nil {
			t.Fatal(err)
		}
	}
	perOp := time.Since(start) / iters
	t.Logf("indexed query: %v/op", perOp)
	if perOp > 5*time.Microsecond {
		t.Errorf("query %v/op > 5µs target", perOp)
	}
}
