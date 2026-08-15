# facetta

An embedded, read-optimized in-memory OLAP aggregation table using dictionary-encoded columnar snapshots with a delta overlay and atomic view swap. It provides Store primitives plus an optional background compactor; the host owns ingestion from its source of truth. See [docs/design-rationale.md](docs/design-rationale.md) for why the design is the way it is, and [docs/architecture.md](docs/architecture.md) for an implementation walkthrough.

The library is a **pure store plus a compaction policy**. It owns the
read-optimized aggregation table (`Store`) and an optional background
`Compactor`. Ingestion, incremental-pull position management, reconcile
cadence, and startup load-or-fallback orchestration are the **host's**
integration contract.

## Usage Example

```go
// Create a store with the schema
store, err := New(Schema{
    Dims:      []string{"source", "account", "publisher", "country", "os"},
    IndexDims: 3,
    Metrics:   []string{"impressions", "clicks"},
}, Config{
    TTL:     15 * time.Minute,
    MaxRows: 5_000_000,
})
if err != nil {
    log.Fatal(err)
}

// Startup: warm-start from the snapshot, else full load from the source.
const snapPath = "/var/cache/facetta-snapshot.bin"
if err := store.LoadSnapshot(snapPath); err != nil {
    all := fetchAllFromSource() // host-owned
    if err := store.ReplaceAll(all); err != nil {
        log.Fatal(err)
    }
}

// Run the background compactor. It only drives Store.Compact; the host feeds data.
ctx := context.Background()
compactor := NewCompactor(store, CompactorConfig{SnapshotPath: snapPath})
go compactor.Run(ctx)

// Incremental ingestion (host pull loop). SyncPosition is the resume cursor.
for {
    recs := fetchSince(store.SyncPosition()) // host-owned
    if err := store.Apply(recs); err != nil {
        log.Print(err)
    }
    // ... sleep until the next pull ...
}

// Query aggregates by groups
buf := make([]float64, 0, 2)
groups := [][]Cond{
    {{Dim: "source", Value: "src7"}, {Dim: "account", Value: "acc42"}},
}
results, err := store.QueryGroups(buf, groups)
if err != nil {
    log.Fatal(err)
}
// results contains aggregated metric values for the matching groups
```

Deletes converge either via `ExpireAt` tombstones (write a record with
`ExpireAt <= now` for the tuple) or a host-driven `ReplaceAll` with a full
dump. A periodic `ReplaceAll` is the on-demand drift-repair path.

One defined edge of the tombstone model: physical reclaim (any merge or
rebuild) forgets the tuple's `UpdatedAt` watermark, so an out-of-order
**older** record arriving afterwards recreates the row. This resurrection is
deliberate — remembering every reclaimed tombstone forever would unbound
memory — and falls in the same drift category `ReplaceAll` repairs.

## Aggregates, IN conditions and GROUP BY

Beyond the sum-everything `Query`/`QueryGroups`, three richer query forms
share the same planner, union semantics (each row counted once across
groups) and expiry visibility:

```go
// Selected aggregates, still zero-alloc: one output per Agg column.
sums, err := store.QueryAggs(buf, []facetta.Agg{
    {Op: facetta.AggCount},                       // matched row count
    {Metric: "impressions", Op: facetta.AggSum},
    {Metric: "clicks", Op: facetta.AggAvg},       // NaN when nothing matches
}, groups)

// IN condition: dimension equals ANY listed value.
groups := [][]facetta.Cond{{
    {Dim: "source", Value: "src7"},
    {Dim: "os", In: []string{"ios", "android"}},
}}

// GROUP BY: aggregates per distinct combination of the by dims.
var res facetta.GroupedResult // reuse across calls
err = store.QueryGroupBy(&res, []string{"country", "os"}, aggs, groups)
for i := 0; i < res.N; i++ {
    key := res.Keys[i*2 : (i+1)*2]   // ["US", "ios"], sorted, deterministic
    row := res.Aggs[i*len(aggs) : (i+1)*len(aggs)]
    _ = key; _ = row
}
```

Semantics and limits:

- **`QueryAggs`** supports `AggSum`, `AggCount`, `AggMin`, `AggMax`, `AggAvg`
  (up to 16 columns). Over zero matched rows Sum/Count are 0 and Min/Max/Avg
  are **NaN** (the float64 stand-in for SQL NULL). Zero heap allocations,
  same as `QueryGroups`.
- **`Cond.In`** filters rows during the scan but does **not** contribute to
  index-prefix planning: a group whose leading index dims carry only IN
  conditions degrades to a full scan (fail-fast guarded by `MaxScanRows`).
  For indexed multi-value queries use one group per value instead. Caps: 16
  values per condition, 16 IN conditions / 128 resolved values per query.
- **`QueryGroupBy`** writes into a reusable `GroupedResult` (flat row-major
  `Keys`/`Aggs`, groups sorted lexicographically — deterministic output).
  It is the one query entry point with a relaxed allocation contract:
  per call it allocates **O(result groups)** (map-key interning and sort
  scratch, never O(scanned rows)); slices and map buckets amortize across
  calls on a reused result. Key strings alias the store's immutable
  dictionaries — no copies. The scan budget applies unchanged.

## Configuration

### Store Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| `TTL` | none | Time-to-live for rows; expired rows are evicted during compaction |
| `MaxRows` | none | Maximum row capacity; `Compact`/`ReplaceAll` are refused (old snapshot keeps serving) if exceeded. While refused, `Apply` drops records that would create a new row (updates to existing rows still land) and counts them in `Stats().DroppedOverCap`; ingestion recovers automatically once TTL shrinkage or upstream deletes bring a compaction back under the cap |
| `MaxScanRows` | none (unlimited) | Per-query scan budget: refuses a query with `ErrScanBudget` if its known scan work (deduped base candidate rows + delta rows, computed after planning and before touching any row) exceeds this count. See [Bounded query latency](#bounded-query-latency) |
| `DictCompactInterval` | 0 (every merge) | Rate-limits dictionary compaction (garbage reclaim + id renumbering + key-width shrink) to at most one merge per interval; merges inside the window keep ids stable and skip the mark/remap passes (~40% cheaper). An `ErrKeyOverflow`-refused merge retries once with dictionary compaction regardless of the gate. See [Dictionary hygiene](#dictionary-hygiene) |

### Compactor Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SnapshotPath` | empty (disabled) | Filesystem path saved after each successful compaction; enables crash recovery |
| `CheckInterval` | 10s | Ratio/expiry poll cadence: compacts when the delta grows past `DeltaRatio` (skipped while cap-blocked) |
| `CompactInterval` | 5m | Periodic tidy cadence: compacts when `NeedsCompaction()` holds (attempts even while cap-blocked, for recovery) |
| `DeltaRatio` | 0.1 | Threshold: compact when delta rows exceed this fraction of snapshot rows (bounds the delta's *relative* size) |
| `MaxDeltaRows` | 0 (disabled) | Absolute delta cap: compact when `DeltaRows() >= MaxDeltaRows`, in addition to the ratio trigger (skipped while cap-blocked, like the ratio trigger). Bounds the delta's *absolute* per-query scan cost, so pair it with `Config.MaxScanRows` |

## Bounded query latency

For deadline-sensitive callers, a slow answer is worse than an instant refusal. Two knobs turn the unbounded worst case into a structural guarantee plus a fail-fast refusal:

- **`Config.MaxScanRows`** (store-level) caps per-query scan *work*. After planning, the total work is known before any row is touched: the merged base candidate intervals plus the delta row count. If that sum exceeds the budget, the query returns `ErrScanBudget` immediately — no rows scanned, zero heap allocation on the refusal path. A full scan's `[0,N)` span naturally trips any sane budget on a large table, so there is no separate "no full scans" flag.
- **`CompactorConfig.MaxDeltaRows`** (compactor-level) keeps the delta small so its unconditional linear scan never dominates.

Both bound **work** (rows visited), not wall-clock. Measured per-row cost is **~5–6 ns/row** (from `BenchmarkQueryFullScan1M`); index range location is a binary search:

```
worst-case scan work ≈ log₂(N) steps  +  (MaxScanRows + MaxDeltaRows) × ~6 ns/row
example: N = 30M, MaxScanRows = 10_000, MaxDeltaRows = 50_000
       ≈ 25 steps  +  60_000 × 6 ns  ≈ ~360 µs upper bound
```

The formula bounds *work*. Go's GC and scheduler still add tail jitter (sub-millisecond typical) that sits on top and is **not** bounded by the library — the guarantee is "this query will touch at most K rows", not "this query finishes within T wall-clock".

`ErrScanBudget` means **"refused fast, fall back"**: on a hard-deadline path, treat it as "too expensive in this layer" and fall back to the primary store or a degraded answer rather than retrying. The counts behind a refusal (candidate rows, delta rows, budget) are not wrapped into the error — so the refusal path stays allocation-free — but the refusal count is exposed as `Stats().ScanBudgetRefusals`.

Example config bounding worst-case scan work to ~360 µs (10k query budget
+ 50k delta cap, at the measured ~6 ns/row; tighten `MaxDeltaRows` if your
deadline needs a lower ceiling — the delta term usually dominates):

```go
store, _ := facetta.New(schema, facetta.Config{
    MaxRows:     5_000_000,
    MaxScanRows: 10_000, // ≈ 60 µs of base-scan work, refuse anything larger
})
compactor := facetta.NewCompactor(store, facetta.CompactorConfig{
    DeltaRatio:   0.1,
    MaxDeltaRows: 50_000, // keep the delta scan bounded too
    SnapshotPath: "/var/lib/app/facetta.snap",
})
```

Budget refusal is an **availability** feature, not a query-semantics change: the reference oracle has no budget concept, and equivalence tests run with `MaxScanRows = 0` (the default, no behavior change).

## Per-record TTL

`Record.ExpireAt` sets an absolute per-row expiry (independent of the global `Config.TTL`, which is `UpdatedAt`-based). Semantics:

- **Zero value = never expires** (fully backward compatible).
- **Read-time visibility:** a row with a non-zero `ExpireAt` is invisible to queries once `now >= ExpireAt`, effective immediately without a compaction. A single `now` is sampled per query, so results are internally consistent.
- **Physical reclaim:** expired rows are dropped physically at the next `Compact`/`ReplaceAll`.
- **Upsert replaces the whole record**, including `ExpireAt`. Writing a record with `ExpireAt` at or before `now` for an existing tuple acts as a **tombstone**: the row becomes invisible immediately and is reclaimed on the next compaction.
- **`SyncPosition` is unaffected** by `ExpireAt` (still `UpdatedAt`-based).
- The two mechanisms compose: either global TTL or per-record expiry dropping a row is fine.

Snapshot files are format **version 2** (an `expireAt` column follows `updated`). Version 1 files are rejected on load, which triggers the normal full-pull fallback.

## Dictionary hygiene

Dictionary ids are never recycled in place: rows evicted by TTL or tombstones
would leave their strings behind as garbage entries, inflating dictionary
memory and — because packed-key widths are computed from dictionary length,
not live cardinality — the index-key bit budget (64 bits across the index
dims).

So `Compact` also compacts the dictionaries, **gated on
`Config.DictCompactInterval`**: at most once per interval (every merge when
zero), a mark pass records which ids surviving rows reference, survivors are
renumbered monotonically per dimension (order-preserving, so the base stays
sorted and the merge remains a linear zip), and key widths are recomputed from
the live cardinality. Merges inside the window keep ids stable and skip the
mark/remap passes. Visible data and query results are identical either way.

The gate exists because the renumbering passes are not free: ~+70% of merge
wall time quiet (~88 vs ~52 ms at 1M rows; the id-stable path also shares
unchanged dictionaries and packed keys with the old snapshot instead of
copying them), similar under concurrent read load (reader latency is
unaffected either way). Detecting whether garbage
exists would cost the same mark pass as reclaiming it, so the trade is made
on time, not on a garbage estimate: set the interval to how long you can
tolerate garbage accumulating (minutes-scale is plenty; 0 keeps every merge
garbage-free).

Two consequences:

- **Bounded garbage.** Dictionary garbage and width inflation are bounded by
  one interval of churn — after that, the next merge reclaims them.
- **`ErrKeyOverflow` recovery is never delayed.** An id-stable merge refused
  for key overflow immediately retries with dictionary compaction, so a store
  whose *live* cardinality fits the 64-bit budget always recovers in-process,
  gate or no gate. Only genuine live-cardinality overflow keeps refusing (old
  view keeps serving; tombstones/TTL shrinkage then recovers it).

`Stats().DictEntries` (per-dim dictionary length) and `Stats().IndexKeyBits`
(width the index dims would need if compacted now, budget 64) expose the water
levels; alert around `IndexKeyBits > 56`.

What dictionary compaction does **not** do: converge upstream hard-deletes
that bypassed tombstones. Periodic host-driven `ReplaceAll` remains the only
drift-repair path, and its cadence can be chosen purely from drift-tolerance
requirements.

## Metric precision

Aggregation is a plain sequential `float64` sum. Integer-valued metrics (counts, or money in minor units) are exact up to 2^53, so their sums are bit-exact at any realistic scale. Fractional metrics are accurate enough for monitoring at millions of rows, but the sum is **order-dependent**: compaction reorders rows physically, so the same logical data may differ in the last few bits before and after a `Compact`. If a metric must be exact — money is the typical case — have the host store it as an integer count of the smallest unit (e.g. cents); this is bit-exact and needs no engine change.

## Operations

- `Store.ReplaceAll(recs)` is the host-owned full-reload / drift-repair path: it fetches the complete table and replaces the whole view, converging any upstream deletes immediately. Run it periodically (host-owned cadence) to bound drift, and it is safe to call concurrently with a running `Compactor`.
- `Store.NeedsCompaction()` reports whether a `Compact` would change the current view (delta rows present, or global-TTL / per-record expiry has aged out base rows). It is lock-free and zero-alloc. The `Compactor`'s `CompactInterval` tick gates on this rather than on `DeltaRows() > 0`, so read-skipped expired rows are still physically reclaimed even when ingestion is idle. When driving a bare `Store` yourself (no `Compactor`), call `Compact` when `NeedsCompaction()` returns true to reclaim expired memory.
- `Store.SyncPosition()` is the resume cursor for a host incremental-pull loop (max `UpdatedAt` of visible rows). It may regress after a compaction reclaims the newest rows; re-pulling from the regressed position only re-ingests already-expired records, which stay invisible and drop at the next compaction — no observable inconsistency.
- `Store.Stats()` returns a monitoring snapshot (row counts, compaction/reconcile/full-scan counters, snapshot save/load outcomes, `DroppedOverCap`, `ScanBudgetRefusals`, `DictEntries`, `IndexKeyBits`, and last compaction duration) for observability.

## Benchmarks

Measured on an Intel Core i9-9880H (2.30GHz), `go test -bench . -benchmem -run xxx`:

| Benchmark | Time/op | Bytes/op | Allocs/op |
|-----------|---------|----------|-----------|
| `BenchmarkQueryIndexed1M` (1M rows, 2 filter groups) | ~350-380 ns/op | 0 B/op | 0 allocs/op |
| `BenchmarkQueryFullScan1M` (1M rows, 1 group on a non-index dim) | ~5.1-5.6 ms/op | 0 B/op | 0 allocs/op |
| `BenchmarkQueryMultiGroup1M` (1M rows, 8 indexed groups unioned) | ~1.8 µs/op | 0 B/op | 0 allocs/op |
| `BenchmarkQueryWithDelta1M` (1M base + 10k delta, 2 indexed groups) | ~47 µs/op | 0 B/op | 0 allocs/op |
| `BenchmarkQueryAggsIndexed1M` (as the indexed query, 4 aggregate columns) † | ~1.7× the indexed sum query | 0 B/op | 0 allocs/op |
| `BenchmarkGroupByIndexed1M` (~20k-row indexed range grouped by an 8-value dim, reused `GroupedResult`) † | ~1.0 ms/op | ~89 B/op | 10 allocs/op |
| `BenchmarkApply1K` (1000-record batch onto 1M rows) | ~4 ms/op averaged as the resident delta approaches the 50k compaction threshold (Apply copies the delta columns; the tuple index is map-cloned, not rebuilt) | ~3.9 MB/op | ~1.1k allocs/op |
| `BenchmarkApplySmallOnLargeDelta` (10-record batch onto a ~100k-row delta) | ~1.9 ms/op (the O(DeltaRows) column copy dominates) | ~8.9 MB/op | ~280 allocs/op |
| `BenchmarkReplaceAll1M` (full reconcile build, 1M records) | ~560-600 ms/op | ~202 MB/op | ~490 allocs/op |
| `BenchmarkSaveSnapshot1M` (1M rows) | ~98-108 ms/op | ~1 MB/op | 15 allocs/op |
| `BenchmarkLoadSnapshot1M` (1M rows, fresh `Store` per iteration) | ~52 ms/op | ~119 MB/op | ~32.7k allocs/op |
| `BenchmarkCompact1M` (1M base + 1k delta merged per iteration, with dictionary compaction, `-benchtime=5x`) | ~88 ms/op | ~67 MB/op | ~424 allocs/op |
| `BenchmarkCompactIDStable1M` (as above on the gated id-stable path, `DictCompactInterval` open) | ~52 ms/op | ~60 MB/op | ~34 allocs/op |

† measured in a later session whose machine ran globally slower (same-day A/B put the pre-existing rows at their old relative levels, unregressed within ±3% noise); the ratio and the allocation counts are the durable numbers.

`BenchmarkQueryFullScan1M` shows the degraded full-scan path costs roughly 14,000x an indexed lookup (~5.3 ms vs ~370 ns); `BenchmarkQueryWithDelta1M` shows a 10k-row delta overlay adds ~120x over the delta-free indexed query (~47 µs vs ~370 ns) from its linear scan. Queries against views with no expirable rows skip all per-row expiry checks (and the clock sample), so tables that never set `ExpireAt` pay nothing for the per-record TTL feature. Base rows are also exempt from per-row checks while the earliest base expiry is still in the future — the expire column is only touched once a row could actually be expired.

`TestCapacity5M` (5M rows, `go test -run TestCapacity5M -v`):

| Metric | Measured | Target |
|--------|----------|--------|
| Resident memory (heap growth) | ~290 MB (incl. the per-record expiry column, ~8 B/row) | < 400 MB |
| Heap objects growth | ~32,180 (tracks the ~30k publisher cardinality, not row count) | < 1,000,000 |
| Full build (`ReplaceAll`, 5M rows) | ~3.2-3.9 s | informational |
| Compact (5M base + 50k delta merge, incl. dictionary compaction) | ~430 ms | < 2 s |
| Indexed query | ~460-660 ns/op | <= 5 µs |

### Design Note

The columnar layout uses `float64` for metric columns and `uint32` for dimension columns, enabling efficient SIMD operations. When Go's experimental `simd` package matures, metric aggregation can leverage vector instructions for throughput on large result sets.
