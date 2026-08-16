> 中文版:[architecture.zh.md](architecture.zh.md)

# Implementation Walkthrough: How the Code Works

An implementation walkthrough for people reading the code. The README covers "how to use
it", [design-rationale.md](design-rationale.md) covers "why it was designed this way", and
this document covers "what the code actually does". The entire source is about 1700 lines (excluding tests); after
reading this document, the source should present no obstacles.

## One-Minute Overview

This is an in-process read-only query acceleration layer: the external primary database
is the sole source of truth, and this library mirrors one of its statistics tables as a
**dictionary-encoded columnar in-memory structure**, supporting exactly one query shape —
"union of multiple equality-filter groups + metric summation" — in exchange for a
microsecond-level, zero-heap-allocation, fully lock-free read path.

```
                    ┌───────────────────────────── Store ──────────────────────────────┐
                    │                                                                    │
   QueryGroups ───▶ │  view.Load() ──▶ ┌──────────── view (immutable) ────────────┐     │
   (lock-free,      │   (atomic ptr)   │ base: snapshot   sorted columnar base    │     │
    0 alloc)        │                  │ delta:           small unsorted overlay  │     │
                    │                  │ extras:          new incremental dicts   │     │
                    │                  │ overridden:      base masking bitmap     │     │
                    │                  └──────────────────────────────────────────┘     │
   Apply ─────────▶ │  mu ─▶ copy delta wholesale ─▶ atomically swap in new view        │
   Compact ───────▶ │  mu ─▶ merge base+delta into new snapshot ─▶ atomic swap          │
   ReplaceAll ────▶ │  mu ─▶ rebuild from full dump ─▶ atomic swap                      │
                    └────────────────────────────────────────────────────────────────────┘
```

Three pillars:

1. **Immutability + atomic switching**. A `view`, once published, is never modified; every
   write operation constructs a new `view` and swaps it in via `atomic.Pointer`. The
   entire world a reader gets from a single `Load()` is internally consistent, and reads
   and writes never block each other.
2. **Dictionary-encoded columnar storage**. Dimension strings are encoded as dense
   `uint32` ids, and row data consists entirely of numeric slices with no pointers
   anywhere in the data region — the number of objects GC must scan depends only on
   dictionary cardinality (tens of thousands), not row count (millions).
3. **Sorted base + linear delta**. The base is sorted by packed key and supports binary
   search; increments first go into a small delta that is scanned linearly, and are
   periodically merged back into the base. A single-tier in-memory version of the classic
   LSM idea.

## File Map

| File | Lines | Responsibility |
|------|-----:|------|
| `schema.go` | 135 | `Schema`/`Record`/`Config` definitions and validation, error sentinels, schema fingerprint |
| `dict.go` | 38 | Bidirectional string ↔ uint32 id dictionary, immutable once published, writers clone first |
| `snapshot.go` | 424 | Immutable columnar base; full build (`buildFromRecords`) and merge (`mergeView`/`zipMerge`) |
| `view.go` | 227 | `view`/`delta` structures; write path `applyDelta` (copy-on-write) |
| `store.go` | 193 | `Store` facade: atomic pointer, write lock, `Apply`/`Compact`/`ReplaceAll` |
| `query.go` | 404 | `Cond` (equality + IN), shared planner (`planGroups`), `QueryGroups`, scan budget |
| `agg.go` | 179 | `Agg`/`AggOp` aggregate selection, `QueryAggs` (zero-alloc) |
| `groupby.go` | 226 | `QueryGroupBy` hash aggregation into a reusable `GroupedResult` |
| `compactor.go` | 89 | Optional background compaction policy (when to call `Compact`) |
| `persist.go` | 272 | Snapshot persistence/loading, versioned binary format + CRC |
| `stats.go` | 70 | Monitoring counter snapshot |
| `reference_test.go` | — | Naive oracle: the sole baseline for query semantics |
| `equivalence_test.go` | — | Bit-for-bit parity of engine vs oracle under random workloads |

## Core Data Structures

### dict (`dict.go`)

A bidirectional mapping of `map[string]uint32` + `[]string`, with ids densely assigned in
insertion order. Convention: **immutable once published into a view**; to extend it,
`clone()` first. This is the only place in the entire library that holds pointers
(strings).

### snapshot (`snapshot.go:13`)

The immutable columnar base, sorted by row primary key:

```go
dims    [][]uint32    // [dimension][row] dictionary ids
mets    [][]float64   // [metric][row]
updated []int64       // unix milliseconds, upsert timestamp
expire  []int64       // unix milliseconds, 0 = never expires
keys    []uint64      // per row, the first IndexDims dimension ids packed into an index key, ascending
```

**packed key**: the ids of the first `IndexDims` dimensions are shifted by their
respective bit widths (`dictBits`, determined by dictionary cardinality) and packed into
one uint64 (`computeShifts`/`packKey`, `snapshot.go:58-83`). If the bit widths sum to more
than 64, that is `ErrKeyOverflow`. `keys` is in ascending order, so an equality-prefix
query becomes two binary searches.

**Row identity = the combination of all dimensions**; the index key is only a prefix, and
rows with the same key (same index prefix, different non-index dimensions) are adjacent
in sort order and distinguished by a linear scan (`findRow`, `view.go:86`). Therefore
`IndexDims` should place highly selective dimensions first, otherwise both query planning
and upsert matching degrade toward linear scans.

Three scalars are also maintained for O(1) predicates: `maxUpdated` (sync position),
`minUpdated` (global TTL expiry check), and `minExpire` (per-row expiry check, also used
to skip the expire column entirely at query time).

### delta (`view.go:14`)

A small overlay of newly arrived upserts: also columnar but **unsorted**, always scanned
linearly at query time. It carries a `map[packed id tuple]row index` so that repeated
upserts of the same tuple overwrite in place. Dimension ids live in the **combined id
space**: `id < base dictionary length` looks up the base dictionary, otherwise subtract
the length and look up `extras`.

### view (`view.go:35`)

The complete world a reader gets from a single `Load()`:

- `base *snapshot` — the sorted base;
- `delta *delta` — the overlay;
- `extras []*dict` — per dimension, a dictionary of "strings that first appeared since
  the last compaction";
- `overridden []uint64` — a bitmap marking base rows "masked by some delta row", skipped
  at query time so the same logical row is never counted twice.

## Read Path: What Happens in One QueryGroups (`query.go:117`)

```
view.Load()                          ── the only atomic read; everything after operates on this immutable snapshot
  │
  ├─ plan() per condition group      ── string conditions → dictionary ids; find the longest
  │    (query.go:31)                    fully-specified prefix of index dimensions, binary-search
  │                                     the candidate range [lo,hi)
  │                                     · value not found in any dictionary → dead, group can match no rows
  │                                     · value only in extras → basePossible=false, skip base and scan only delta
  │                                     · no usable prefix → scan (full base scan, counted in Stats.FullScans)
  │
  ├─ range union                     ── ≤16 ranges insertion-sorted by lo (stack array),
  │                                     deduplicated with a high-water mark `done`, overlapping
  │                                     ranges scanned only once
  │
  ├─ scan budget (optional)          ── after planning, before touching any row, the number of rows
  │                                     to be visited is fully known: deduplicated base candidate
  │                                     rows + delta rows > MaxScanRows
  │                                     → return bare ErrScanBudget (not wrapped; the rejection path
  │                                     is also zero-allocation)
  │
  ├─ scan base candidate ranges      ── per row: skip via overridden bitmap → skip if expired →
  │                                     matchBase per group, accumulate on first hit and break
  │                                     (union does not double-count)
  │
  └─ linear scan of delta            ── same expiry skip + per-group matching
```

**How zero heap allocation is achieved**: all scratch space (`plans`, `ivs`) consists of
fixed-size stack arrays of `maxGroups`/`maxConds`; `dst` is reused by the caller; error
sentinels are preallocated. `TestQueryZeroAlloc` is the tripwire — any hot-path change
must keep it green.

**Expiry checks are also on demand**: when the view contains no expirable rows at all
(`base.minExpire==0 && !delta.hasExpiry`), `now` is not even sampled and the expire
column is never touched; likewise the whole check is skipped when the base's earliest
expiry moment is still in the future. Tables that don't use per-row TTL pay zero cost for
the feature. A query samples `now` exactly once, so results are internally consistent.

### The Other Query Entry Points: QueryAggs and QueryGroupBy

All three entry points share the pipeline above through `planGroups`
(`query.go`): plan every group, union the candidate ranges, enforce the scan
budget. Only the per-row action differs — `QueryGroups` sums every metric,
`QueryAggs` (`agg.go`) folds the requested aggregate columns into a
fixed-size stack accumulator (still zero-alloc; Min/Max initialize from the
first matched row, Count/Avg derive from a shared row counter —
`AggDistinct` columns are the exception: each allocates one id bitmap sized
by the dim's combined cardinality and counts new ids by test-and-set, exact
with no popcount pass), and
`QueryGroupBy` (`groupby.go`) hash-aggregates into a reusable
`GroupedResult`: the group key is the packed id tuple of the by dims (ids
identify strings uniquely within one view, across base and delta), map
lookups use the non-allocating `m[string(bytes)]` form, groups are created
from their first row (no sentinel init), and the output is sorted by key
strings for determinism. Distinct columns in group-by dedup per group
through one shared seen-(column, group, id) set kept on the result. Its allocations are O(result groups) per call and
amortize on a reused result — the one documented exception to the
zero-allocation rule.

IN conditions (`Cond.In`) are resolved into a per-query id pool (`queryIns`)
rather than into `groupPlan`, and checked by `matchIns` at the call sites
rather than inside `matchBase`/`matchDelta`. Both placements are
deliberate hot-path protection, measured before merging: a per-plan id array
inflates the plans' stack scratch by an order of magnitude (all of which is
zero-initialized on every query), and folding the IN check into the matchers
pushes them past the compiler's inline budget — either costs double-digit
percentages on the indexed query. IN-free queries pass a nil pool and pay
one never-taken branch per matched row.

## Write Path: Apply's Copy-on-Write (`view.go:113`)

`Apply` holds `mu` and performs **copy-then-modify on the whole** of the current view:
each delta column is copied via append, the tuple index via `maps.Clone` (row order is
unchanged, so the old index is directly usable — no per-row re-encoding), and `extras`
are cloned **lazily** per dimension (if this batch adds no new words to a dimension, the
old dictionary is shared as-is — unchanged dictionaries are immutable, so sharing across
views is safe).

The upsert semantics per record (aligned record-by-record with the oracle):

1. `UpdatedAt` earlier than the TTL cutoff → drop immediately;
2. tuple already in the delta index → if the timestamp is not older, **replace the whole
   row** (including `ExpireAt`); if older, ignore;
3. tuple not in delta, and all ids exist in the base dictionary → `findRow` in the base:
   if the base row is strictly newer, ignore (out-of-order stale record), otherwise
   append to delta and set the `overridden` bit to mask the base row;
4. already-expired records (`ExpireAt <= now`) are **stored as usual** and hidden by the
   read-time skip — store first, filter later, so deduplication order is bit-for-bit
   identical to the oracle; physical reclamation is left to the next merge.

The cost of one `Apply` is O(current delta row count + batch size), so the host should
**batch** its calls and use `CompactorConfig.MaxDeltaRows`/`DeltaRatio` to bound the
resident delta, capping the cost per call.

## Compaction: Merge and Dictionary Compaction (`store.go:140`, `snapshot.go:229`)

`Compact` = minor compaction. It holds `mu` (mutually exclusive with Apply/ReplaceAll —
**queries are unaffected**), merges base+delta into a brand-new snapshot, and atomically
swaps it in; on failure (row cap exceeded / key overflow) the old view keeps serving.

The core is `zipMerge` (`snapshot.go:300`) — one linear two-way zip merge:

1. delta rows are sorted by their new packed keys (the base is already sorted);
2. two-pointer zip, dropping three classes of rows along the way: base rows masked by
   `overridden`, rows whose `UpdatedAt` is past the global TTL, and rows whose `ExpireAt`
   has arrived (physical reclamation);
3. the output is a new snapshot still ordered by key, with the max/min scalars recomputed.

**Dictionary compaction** (the `renumber` parameter of `mergeView`) is gated by
`Config.DictCompactInterval`:

- **id-stable path** (inside the window): new dictionary = base dictionary + extras
  appended, ids unchanged, row data carried over as-is; dimensions with empty extras
  share the base dictionary object directly, and when shifts are unchanged the base keys
  column is reused as-is too.
- **compaction path** (window expired): a mark pass records which ids live rows reference
  in the combined id space → surviving entries are monotonically renumbered in id order
  (monotonic ⇒ the base sort order is not disturbed, so the merge remains the same
  linear zip) → bit widths are recomputed from the active cardinality. This reclaims both
  the "ghost entries" left behind by evicted rows and the bit-width inflation in one go.

  The two renumbering passes account for roughly 40–60% of merge time (88ms vs 52ms @1M),
  which is why the gate exists.

**Overflow self-healing**: when the id-stable merge reports `ErrKeyOverflow`, the gate is
ignored and the merge is immediately retried once with dictionary compaction
(`store.go:148`) — bit width is computed from the garbage-inclusive dictionary length,
and after compaction it usually fits. Only when the **active** cardinality genuinely
exceeds 64 bits does it keep rejecting (the old view keeps serving, self-healing once
TTL/tombstones shrink the set).

`ReplaceAll` = major compaction / reconcile: rebuilds wholesale from a full dump via
`buildFromRecords` (`snapshot.go:105`) — encode, sort (newest first among identical
tuples), deduplicate, discard survivors that are already expired; the dictionaries are
naturally fresh. This is the **only** path that can converge "upstream hard deletes that
bypass tombstones"; its cadence is decided by the host according to its drift tolerance.

`NeedsCompaction` (`store.go:59`) is an O(1) lock-free predicate: delta non-empty ∥
`minUpdated` past the global TTL ∥ `minExpire` has arrived. The Compactor's periodic tick
uses it as a backstop, guaranteeing that expired rows hidden by read-time skips are
physically reclaimed even when ingestion is idle.

## Compactor: Policy Separated from Mechanism (`compactor.go`)

`Store` provides only mechanism; `Compactor` is the optional default policy, knowing
nothing about where data comes from:

- Every `CheckInterval` (default 10s): delta/base exceeds `DeltaRatio` (relative bound,
  default 0.1) or delta row count reaches `MaxDeltaRows` (absolute bound, constraining
  per-query scan cost) → compact. Skipped when cap-blocked — retrying would only hit the
  cap again;
- Every `CompactInterval` (default 5m): `NeedsCompaction()` holds → compact. When
  cap-blocked it **still tries** — this is the recovery probe after exceeding the cap
  (TTL shrinkage / upstream deletes may have made the table fit again);
- On successful compaction with `SnapshotPath` configured → persist to disk as well.

## Lifecycle: TTL, Tombstones, and the "Resurrection" Boundary

Two orthogonal expiry mechanisms overlap; a row is evicted if either declares it dead:

| | Global `Config.TTL` | Per-row `Record.ExpireAt` |
|---|---|---|
| Basis | `UpdatedAt` (retention window) | absolute moment |
| When invisible | disappears at the next merge | `now >= ExpireAt`, immediately invisible at read time |
| Physical reclamation | at merge/rebuild | at merge/rebuild |

Upstream has no delete events; a soft delete = writing a record with `ExpireAt <= now`
(a tombstone): immediately invisible, physically reclaimed at the next merge. **Defined
boundary**: physical reclamation forgets the tuple's `UpdatedAt` water mark, so an
out-of-order **older** record arriving afterwards resurrects the row — remembering all
tombstones forever would consume unbounded memory, so this is deliberately not done; the
oracle's `reclaimExpired` models this in sync, and this class of drift belongs to the
same `ReplaceAll`-repaired category as hard-delete drift.

`SyncPosition` (the host's resume position for incremental pulls) = max `UpdatedAt` of
visible rows, and **may move backwards** after a merge reclaims the newest rows; the
host's re-pull only re-ingests already-expired records, which remain invisible and are
dropped again at the next merge — no observable inconsistency (comment at `store.go:39`).

## Persistence (`persist.go`)

It has exactly one purpose: faster restarts. Format v2, little-endian binary:

```
"OLSNAP01" | version(u32) | schema fingerprint(u64) | maxUpdated(u64) | row count(u64)
| per-dimension dictionaries (count + strings) | dims columns | mets columns | updated column | expire column | CRC32C
```

- **Base only**: delta rows are all newer than the persisted sync position, so the
  incremental re-pull after restart naturally backfills them;
- **Atomic write**: temp file + fsync + rename;
- **Validate on load**: CRC, magic, version, schema fingerprint, id out-of-range, keys
  ordering, maxUpdated consistent with per-row recomputation — if any check fails, the
  whole file is rejected, the store is untouched, and the host falls back to a full pull.
  A stale snapshot whose position predates the TTL cutoff is likewise rejected
  (`errSnapshotStale`);
- **No cross-version compatibility**: a version mismatch is rejected outright (falling
  back to a full pull is designed behavior), in exchange for an extremely simple format
  codebase. Any column/header change must increment `snapVersion`.

## Concurrency Model

- **Readers**: one `view.Load()`, then fully lock-free. An old view swapped out is still
  held by in-flight queries and reclaimed naturally by GC — immutability makes
  epoch/reference-counting mechanisms entirely unnecessary.
- **Writers**: `mu` serializes Apply/Compact/ReplaceAll/LoadSnapshot. Compact holds the
  lock throughout (a few hundred ms at 5M rows), during which Apply waits — a documented
  trade-off, in exchange for queries never being blocked by writes.
- **Counters**: all `atomic`; `capBlocked` is atomic too so it can be read outside Apply
  (the Compactor's lock-free probe).

## Self-Protection

- **`MaxRows`**: if the merge/rebuild output exceeds the cap → refuse to swap in, the old
  view keeps serving, `capBlocked` is set. Meanwhile `Apply` drops records that "would
  create new rows" (updates to existing rows still pass, counted in
  `Stats.DroppedOverCap`) to prevent unbounded delta growth; the block lifts
  automatically once some merge fits again.
- **`MaxScanRows`**: see the read path. Turns the O(N) worst case into an immediate
  rejection (`ErrScanBudget` = "this layer is too expensive, fall back to the primary
  database fast"); it constrains **workload** (rows visited), not wall-clock time.
- **Dictionary/bit-width water marks**: `Stats.DictEntries`, `Stats.IndexKeyBits` (how
  many bits the index key would need if compacted right now; budget is 64, alerting at
  >56 is recommended).

## How Correctness Is Guaranteed

- **Oracle first** (`reference_test.go`): the naive implementation — one heap object per
  row plus linear scans — is the **sole baseline** for query semantics. Changing engine
  semantics requires changing the oracle in lockstep.
- **Randomized equivalence** (`equivalence_test.go`): the same random workload
  (out-of-order timestamps, tri-state ExpireAt, mixed Apply/Compact/ReplaceAll, injected
  clock advancing mid-run, dictionary compaction gated on odd seeds to cover both merge
  paths) drives engine and oracle simultaneously, with 20 random queries compared
  bit-for-bit at every step.
- **Tripwires**: `TestQueryZeroAlloc` (hot path 0 alloc), `TestCapacity5M` (5M-row
  memory/latency red lines), `TestQueryLatencyDuringCompact` (reads not blocked by
  writes), `TestStressScale` (on-demand scale stress test, gated by
  `FACETTA_STRESS_ROWS`).

## The Full Picture of Host Integration

Library boundary = pure storage + compaction policy; everything below is **the host's
job** — do not add it back into the library:

```
Startup:   LoadSnapshot(path) succeeds ──▶ resume incremental sync from SyncPosition()
           fails (missing/corrupt/version mismatch/stale) ──▶ ReplaceAll (full pull)

Steady:    loop fetchSince(SyncPosition()) ──▶ Apply (batched)
           go compactor.Run(ctx)               (background merge + persist to disk)

Reconcile: periodic / manual ReplaceAll (full)  (the only drift-repair path)
```
