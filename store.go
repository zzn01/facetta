package facetta

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Store is the embedded aggregation table. Reads are lock-free (one atomic
// view pointer); all mutation goes through mu in a single writer at a time.
type Store struct {
	sc         Schema
	cfg        Config
	view       atomic.Pointer[view]
	mu         sync.Mutex
	st         stats
	capBlocked atomic.Bool // set when a compaction/reconcile is refused over MaxRows
	// lastDictCompact is the unix-milli time of the last dictionary-compacting
	// merge, driving the Config.DictCompactInterval gate. Guarded by mu.
	lastDictCompact int64
	now             func() time.Time // injectable for tests
}

func New(sc Schema, cfg Config) (*Store, error) {
	if err := sc.validate(); err != nil {
		return nil, err
	}
	s := &Store{sc: sc, cfg: cfg, now: time.Now}
	s.view.Store(newView(emptySnapshot(&s.sc)))
	return s, nil
}

func (s *Store) Rows() int      { return s.view.Load().base.rows() }
func (s *Store) DeltaRows() int { return s.view.Load().delta.rows() }

// SyncPosition is the max UpdatedAt across all visible rows; the host
// resumes incremental pulls from here. Zero time when the store is empty.
//
// The position may REGRESS after a compaction reclaims expired rows: it is
// the max UpdatedAt of the KEPT rows, so dropping the newest row lowers it.
// A host that re-pulls from the regressed position simply re-ingests the
// expired records, which stay invisible (read-time expiry) and are dropped
// again at the next compaction. No observable inconsistency results.
func (s *Store) SyncPosition() time.Time {
	mu := s.view.Load().maxUpdated()
	if mu == 0 {
		return time.Time{}
	}
	return time.UnixMilli(mu).UTC()
}

// NeedsCompaction reports whether a Compact would change the current view:
// there are delta rows to merge, global Config.TTL has aged out base rows, or
// per-record expiry has hit base rows. The Compactor uses this to trigger a
// compaction even when the upstream is idle, so read-skipped expired rows still
// get physically reclaimed. Lock-free and zero-alloc.
func (s *Store) NeedsCompaction() bool {
	v := s.view.Load()
	if v.delta.rows() > 0 {
		return true
	}
	base := v.base
	now := s.now().UnixMilli()
	if s.cfg.TTL > 0 && base.minUpdated != 0 {
		if base.minUpdated < now-s.cfg.TTL.Milliseconds() {
			return true
		}
	}
	return base.minExpire != 0 && base.minExpire <= now
}

func (s *Store) ttlCutoff() int64 {
	if s.cfg.TTL <= 0 {
		return 0
	}
	return s.now().Add(-s.cfg.TTL).UnixMilli()
}

// swap publishes a new view atomically.
func (s *Store) swap(v *view) {
	s.view.Store(v)
}

// isCapBlocked reports whether ingestion is gated because a compaction/reconcile
// was refused for exceeding MaxRows. Read only on write paths, never on the
// query path (so QueryGroups stays zero-alloc).
func (s *Store) isCapBlocked() bool { return s.capBlocked.Load() }

// Apply upserts incremental records into the delta overlay; they become
// visible to queries immediately. Concurrent-safe with reads and compactions.
//
// Cost: publishing the new view copies the current delta columns, so one
// call is O(DeltaRows + len(recs)). Batch records rather than calling Apply
// per record, and bound the standing delta with CompactorConfig.MaxDeltaRows
// or DeltaRatio to cap the per-call cost.
func (s *Store) Apply(recs []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	nv, dropped, err := s.view.Load().applyDelta(&s.sc, recs, s.ttlCutoff(), s.isCapBlocked())
	if err != nil {
		return err
	}
	if dropped > 0 {
		s.st.droppedOverCap.Add(uint64(dropped))
	}
	s.view.Store(nv)
	return nil
}

// seed replaces the base snapshot without cap checks; test/bootstrap helper.
func (s *Store) seed(recs []Record) error {
	snap, err := buildFromRecords(&s.sc, recs, s.ttlCutoff(), s.now().UnixMilli())
	if err != nil {
		return err
	}
	s.swap(newView(snap))
	return nil
}

// Compact merges the delta into the base, producing a fresh immutable
// snapshot. On failure (key overflow, row cap) the old view keeps serving.
//
// Compact holds the writer lock for the whole merge (hundreds of ms at 5M
// rows), so concurrent Apply/ReplaceAll calls stall for that long. Queries
// are never blocked. Latency-sensitive ingestion loops should expect this
// pause after a compaction trigger fires.
// Compact also performs dictionary compaction, gated on
// Config.DictCompactInterval: at most once per interval (every merge when the
// interval is zero), strings referenced by no surviving row are dropped and
// the remaining ids renumbered, keeping dictionary memory and packed-key
// widths (Stats().DictEntries/IndexKeyBits) bounded by the live data plus one
// interval of churn. Visible data and query semantics are unchanged either
// way. A merge refused with ErrKeyOverflow retries once with dictionary
// compaction regardless of the gate, so a store whose LIVE cardinality fits
// the key budget always recovers in-process. Compaction cannot converge
// upstream deletes that bypassed tombstones; periodic host-driven ReplaceAll
// remains the only drift-repair path.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := time.Now()
	nowMilli := s.now().UnixMilli()
	renumber := s.cfg.DictCompactInterval <= 0 ||
		nowMilli-s.lastDictCompact >= s.cfg.DictCompactInterval.Milliseconds()
	snap, err := mergeView(&s.sc, s.view.Load(), s.ttlCutoff(), nowMilli, renumber)
	if !renumber && errors.Is(err, ErrKeyOverflow) {
		// Widths were computed over garbage-inflated dictionaries; compacting
		// them may fit. Overflow recovery must never wait out the gate.
		renumber = true
		snap, err = mergeView(&s.sc, s.view.Load(), s.ttlCutoff(), nowMilli, true)
	}
	if err != nil {
		s.st.compactionFailures.Add(1)
		return err
	}
	if s.cfg.MaxRows > 0 && snap.rows() > s.cfg.MaxRows {
		s.st.compactionFailures.Add(1)
		s.capBlocked.Store(true)
		return fmt.Errorf("%w: %d rows > max %d", ErrRowLimit, snap.rows(), s.cfg.MaxRows)
	}
	s.view.Store(newView(snap))
	s.capBlocked.Store(false)
	if renumber {
		s.lastDictCompact = nowMilli
	}
	s.st.compactions.Add(1)
	s.st.lastCompactionMillis.Store(time.Since(start).Milliseconds())
	return nil
}

// ReplaceAll rebuilds the whole table from a full source dump (reconcile
// path). Fresh dictionaries: this is also where dictionary garbage from
// evicted rows gets compacted away.
func (s *Store) ReplaceAll(recs []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, err := buildFromRecords(&s.sc, recs, s.ttlCutoff(), s.now().UnixMilli())
	if err != nil {
		s.st.compactionFailures.Add(1)
		return err
	}
	if s.cfg.MaxRows > 0 && snap.rows() > s.cfg.MaxRows {
		s.st.compactionFailures.Add(1)
		s.capBlocked.Store(true)
		return fmt.Errorf("%w: %d rows > max %d", ErrRowLimit, snap.rows(), s.cfg.MaxRows)
	}
	s.view.Store(newView(snap))
	s.capBlocked.Store(false)
	s.st.reconciles.Add(1)
	return nil
}
