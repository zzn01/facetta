> 中文版:[design-rationale.zh.md](design-rationale.zh.md)

# Design Rationale

This document explains **why** facetta is designed the way it is — the decisions,
the constraints that forced them, and the trade-offs deliberately accepted.
For **how** the implementation works, see [architecture.md](architecture.md);
for **what** the library does and how to use it, see the [README](../README.md).

## A mirror, not a database

facetta is an in-process, read-only query acceleration layer. The external
primary database is the sole source of truth; facetta owns no data and can be
fully rebuilt from the primary at any time.

This single decision eliminates the hardest problems a store can have: there is
no WAL, no crash-recovery semantics, no cross-instance replication (each
instance syncs independently), and persistence is merely a restart accelerator
— a snapshot that fails to load falls back to a full pull and must never block
startup. Everything below leans on this: whenever a mechanism would need
complexity to be *correct on its own*, the design instead accepts a bounded
window of divergence and lets a rebuild from the source of truth converge it.

## Why dictionary-encoded columnar storage

The motivating workload is a high-QPS request path querying a table of
low-cardinality string dimensions and numeric metrics, with one fixed query
shape: equality filters, unioned across groups, metrics summed. A naive layout
(one heap object per row, one string per field, map-organized, linear scan)
fails three ways at once as rows grow into the millions: resident memory
bloats, GC mark cost rises linearly with object count, and O(N) scans exhaust
CPU on the request path.

Dictionary encoding plus a columnar layout attacks all three with one
structure: dimension strings become dense `uint32` ids, so the data region
contains no pointers at all — the object count the GC must scan depends only on
dictionary cardinality (tens of thousands), not row count (millions). Rows
sorted by a packed index key make equality-prefix queries two binary searches
instead of a scan. At 5 million rows the whole table fits in ~290 MB with
~32k heap objects.

## Why immutable snapshots + one atomic pointer

Readers load one `atomic.Pointer` and then operate on a world that can never
change under them. This is the entire concurrency model for the read path: no
locks, no epochs, no reference counting, no hazard pointers. Old views are
simply garbage-collected once in-flight queries drop them.

The accepted cost is that every mutation publishes a full new view, and a
rebuild briefly holds two copies of the table (~2× peak memory during
compaction). That trade is right for this workload because reads outnumber
writes by orders of magnitude and the quantitative targets demand that reads
are *never* blocked by writes.

A consciously rejected alternative: an append-only delta with shared prefixes
would make `Apply` O(batch) instead of O(delta), but it breaks "published views
are fully immutable" with a subtle readers-see-prefix memory-model contract.
The measured fix (clone the tuple index map instead of re-encoding it; clone
`extras` dictionaries lazily per dimension) removed the quadratic allocation
cost while keeping the invariant, so the remaining O(DeltaRows) column copy per
`Apply` is accepted, documented, and bounded by compactor policy — hosts batch
records instead of calling `Apply` per record.

## Why the library boundary is "pure store + compaction policy"

An earlier iteration had a `Syncer` that owned ingestion, incremental pull
positions, reconcile cadence, and startup load-or-fallback orchestration, and
required users to implement a `Source` interface. All of that is the host's
integration contract — it varies per deployment and has nothing to do with a
read-optimized aggregation table. The library boundary was cut back to Store
primitives (`Apply` / `ReplaceAll` / `Compact` / snapshot save/load /
`SyncPosition` / `Stats` / `NeedsCompaction`) plus an optional background
`Compactor` that only decides *when* to compact and knows nothing about where
data comes from.

Within that boundary, mechanism and policy stay separated too: the absolute
delta cap (`MaxDeltaRows`) lives in the Compactor, not in `NeedsCompaction`,
because the latter answers a state question ("would compacting change the
view?") while the cap is a latency policy knob.

## Eviction without delete events

The upstream sync stream carries upserts only — no delete notifications. Rather
than requiring them, consistency is guaranteed by convergence, in three layers:

1. **Global TTL**: rebuilds drop rows whose `UpdatedAt` fell out of the
   retention window.
2. **Per-record tombstones**: writing a record with `ExpireAt <= now` makes the
   row invisible immediately and physically reclaimed at the next compaction.
3. **Host-driven `ReplaceAll`**: a periodic or on-demand full rebuild from the
   primary is the one path that converges *everything*, including upstream hard
   deletes that bypassed tombstones.

One edge is deliberately defined rather than prevented: physical reclamation
forgets a tuple's `UpdatedAt` watermark, so an out-of-order **older** record
arriving afterwards resurrects the row. Remembering every reclaimed tombstone
forever would unbound memory; the resurrection falls into exactly the drift
category `ReplaceAll` repairs, so the reference oracle models it and the
library documents it instead of fighting it.

For the same reason, `SyncPosition` (the host's incremental resume cursor) is
allowed to regress when compaction reclaims the newest rows: re-pulling from
the regressed position only re-ingests expired records, which stay invisible
and are dropped again — harmless, so no extra bookkeeping is spent preventing
it.

## Per-record TTL: read-time visibility, rebuild-time reclamation

`Record.ExpireAt` had to satisfy two clocks at once: users want expiry to take
effect *immediately* (Redis-style), while physical reclamation is only cheap at
rebuild time. So visibility and reclamation are split: a query samples `now`
once (internally consistent results) and skips expired rows — one `int64`
comparison per candidate row, and skipped entirely for views that contain no
expiring rows — while compaction physically drops them later.

A subtle ordering rule keeps the engine bit-for-bit equal to the oracle:
already-expired records are still *stored* through the normal upsert/dedup path
and hidden by the read-time check, rather than being dropped at ingest. Dedup
must see them — a newer-but-expired record must still shadow an older
non-expiring duplicate — so "store first, filter at read" is the only order
that matches the reference semantics.

The snapshot format bump this feature required (v2 adds the expire column) set
the persistence policy: **no backward read compatibility**. A version-mismatched
file is rejected and the host falls back to a full pull — the snapshot is a
cache, the fallback already has to exist and be reliable, so format-migration
code would be complexity with no payoff (KISS).

## Bounded query latency: refuse fast instead of queueing

Deadline-bound callers consider a slow answer worse than an instant refusal.
The worst case used to be unbounded: a degraded full scan is O(N) (~174 ms at
30M rows), and the ratio-triggered delta bounds only its *relative* size.

The fix exploits a structural property: after planning and before touching any
row, a query's total work is already known — the merged base candidate
intervals plus the delta row count. `Config.MaxScanRows` turns that into a
fail-fast budget check using the exact arithmetic of the real sweep
(overlapping intervals counted once), O(#intervals) and allocation-free, before
any row is visited. `CompactorConfig.MaxDeltaRows` complements it by capping
the delta's unconditional linear scan in absolute terms.

Two details are deliberate:

- `ErrScanBudget` is a bare sentinel, never wrapped — `fmt.Errorf` allocates,
  and the refusal path must stay zero-alloc. Callers who need the numbers read
  `Stats()`.
- The guarantee is on **work** (rows visited), not wall-clock: GC and scheduler
  jitter sit on top. The honest contract is "this query touches at most K
  rows"; the caller's reaction to `ErrScanBudget` should be to fall back (to
  the primary or a degraded answer), not to retry.

Budget refusal is an availability feature, not a semantics change: the oracle
has no budget concept, and equivalence tests run with the budget disabled.

## Aggregates, IN, and GROUP BY: extending without unfreezing

The original query shape (equality filters, sum every metric) is a frozen
contract — its semantics, zero-allocation guarantee, and measured latency must
survive every later feature. The richer query forms were added under that
constraint, which drove four decisions:

- **New capability, new entry points.** `QueryAggs` and `QueryGroupBy` are
  separate methods sharing one planner, instead of options on `QueryGroups` —
  an options struct would put branches and larger scratch on the frozen path.
  The one addition to the existing surface, `Cond.In`, is engineered to cost
  IN-free queries nothing measurable: its resolved ids live in a per-query
  pool handed over as a nil pointer when no IN is present (a per-plan array
  would grow every query's zero-initialized stack scratch tenfold), and the
  membership check stays out of the row matchers because even a call to it
  pushes them past the compiler's inline budget — both variants were measured
  at double-digit percentage regressions and rejected.
- **NaN is the empty aggregate.** Min/Max/Avg over zero matched rows return
  NaN — float64's stand-in for SQL NULL. Any numeric sentinel (0, ±Inf) is a
  legal data value; an extra "valid" flag would change the output shape. The
  oracle produces the same NaN, and equivalence tests compare NaN-aware.
- **IN filters, it does not plan.** IN conditions participate in row matching
  but not in index-prefix planning: expanding a prefix dim's IN values into
  multiple candidate ranges multiplies interval bookkeeping for a capability
  the group-union API already expresses (one group per value). A group whose
  leading dims carry only INs degrades to a scan and lands in the existing
  `MaxScanRows` safety net.
- **Distinct counting is exact, not sketched.** `COUNT(DISTINCT dim)` is
  normally where engines reach for HyperLogLog, trading error bounds for
  memory. Here dictionary encoding already collapsed the value space: the
  distinct set of a dimension IS a bitmap over its ids, whose size is known
  when the query starts (combined base+extras cardinality) and small
  (~4 KB for 30k values). Exact costs one allocation per distinct column
  and one test-and-set per row — an approximation would be slower to merge
  and strictly worse. Per-group distinct in GROUP BY dedups through a
  seen-triple set instead, and its documented cost grows with unique
  (group, value) pairs — worst case O(matched rows), stated rather than
  hidden.
- **Integer dims: identity is the int64 value, enforced by
  canonicalization.** Range predicates over string-spelled numbers initially
  shipped with a split personality — identity by spelling, ranges by parsed
  value — and were rejected in review: `"1"` and `"1.0"` being different
  rows that the same range counts twice is wrong. The fix is not a typed
  column (8 bytes per row, and index packing still needs a dictionary) but
  canonical encoding: every write boundary rewrites integer-dim values to
  plain base-10 form (rejecting non-integer input outright, the same way
  arity mismatches are rejected), and every query boundary canonicalizes
  condition values — allocation-free, via a stack-rendered key — so
  equality, IN, ranges, dedup and group-by keys all agree on the integer.
  int64 was chosen over float64 deliberately: dimension identity demands
  exactness, and float identity has a silent precision cliff above 2^53
  where distinct values (nanosecond timestamps, large IDs) collapse into
  one — along with NaN/±Inf/negative-zero edge cases that integers simply
  do not have. Fractional buckets follow the same discipline as exact
  metrics: scale to minor units. Ordered dictionaries were rejected because
  compaction's renumbering must stay monotonic in the old ids
  (value-ordering would force a full re-sort per compaction); per-query
  dictionary scans were rejected as milliseconds of parsing per call.
  Instead, integer dictionaries are keyed by the int64 value itself and
  store no strings at all — identity is enforced by the map key, spellings
  are rendered only at output boundaries (group-by keys, snapshot save) —
  and their `vals []int64` column makes a range check two integer
  comparisons per row. Time support is by encoding (unix timestamps), not
  layout parsing. The type
  lives on the dimension declaration itself (`Dim{Name, Type}` — one struct
  per dim rather than parallel tag lists, so a future `DimFloat` would be
  additive). `DimType` stays out of the schema fingerprint; loading
  validates canonicality instead, so a snapshot written before the
  declaration is refused into the normal full-pull fallback.
- **GROUP BY relaxes the allocation rule — explicitly, and only there.**
  Group-by output is inherently variable-size, so "zero allocations" is
  unattainable; the honest contract is O(result groups) allocations per call
  (map-key interning, sort scratch), never O(scanned rows), amortized on a
  reused `GroupedResult`. Hash aggregation over packed id tuples was chosen
  over a streaming sort-order variant because the delta overlay is unsorted
  anyway — one code path that always works beats two paths where one only
  sometimes applies. Output is sorted by key strings so results are
  deterministic; keys alias the immutable dictionaries rather than copying.

## Time-gated dictionary compaction

Dictionary ids, once issued, are baked into row data, old views, and disk
snapshots — they cannot be recycled in place. Rows evicted by TTL or tombstones
therefore leave "ghost entries" behind, which cause three chronic problems:
dictionary memory grows monotonically; packed-key bit widths are computed from
dictionary *length* rather than live cardinality, so they inflate; and once the
index dims' widths sum past 64 bits, `Compact` fails permanently
(`ErrKeyOverflow`) while `Apply` keeps filling the delta — a deadlock only a
host `ReplaceAll` could break.

So `Compact` reclaims dictionaries in-process: mark ids referenced by surviving
rows, renumber survivors monotonically per dimension (monotonic ⇒ the sorted
base stays sorted, so the merge remains the same linear zip), and recompute key
widths from live cardinality.

The renumbering passes cost ~40–60% of merge wall time, which forced the
question of *when* to run them. Estimating accumulated garbage first would cost
the same mark pass as reclaiming it — detection is as expensive as the cure —
so the gate is on **time**, not on a garbage estimate: `DictCompactInterval`
bounds garbage to one interval of churn, and the operator chooses the interval
purely by how long garbage may accumulate. One exception overrides the gate: a
merge refused with `ErrKeyOverflow` immediately retries with dictionary
compaction, because widths computed over garbage-inflated dictionaries may fit
after reclaim. The result is that a store whose *live* cardinality fits the
64-bit key budget always recovers in-process, gate or no gate.

## Oracle-first testing

The naive model that facetta replaces — one heap object per row, keyed by the
full dimension tuple, linear scan — is kept in the repository as
`reference_test.go`, and it is the **sole semantic authority**. Any change to
engine semantics must change the oracle first; `TestEquivalenceRandomized`
drives both sides with identical randomized workloads (out-of-order
timestamps, three-state expiry, mixed apply/compact/replace, an injected shared
clock advanced mid-run) and asserts every query agrees exactly.

This inverts the usual risk of a heavily optimized engine: correctness is
defined by ~150 lines of obviously-correct code, and the thousands of lines of
columnar machinery merely have to agree with it. Alongside the oracle, tripwire
tests pin the non-functional guarantees so they cannot regress silently:
`TestQueryZeroAlloc` (the hot path performs zero heap allocations),
`TestCapacity5M` (memory / latency red lines at 5M rows), and
`TestQueryLatencyDuringCompact` (reads are never blocked by writes).

## Quantitative red lines

The design targets are fixed numbers, locked in by benchmarks rather than
aspirations: 5 million rows in ≤ ~400 MB resident; typical indexed query
≤ 5 µs with 0 allocations; compaction < 2 s; reads never blocked by writes.
The README's benchmark table must contain measured values only; any hot-path
change re-runs the performance layer and updates the table.

## Metric precision

Metric aggregation is a plain sequential `float64` sum. Integer-valued metrics
(counts, money in minor units) are exact up to 2^53 — bit-exact at any
realistic scale. Fractional metrics are order-dependent in the last bits,
because compaction physically reorders rows. The engine does not compensate
(Kahan summation etc.); if a metric must be exact — money being the typical
case — the host stores it as an integer count of the smallest unit. This keeps
the hot loop trivial and the exactness rule easy to state.
