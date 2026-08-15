package facetta

import (
	"sync/atomic"
	"time"
)

type stats struct {
	fullScans             atomic.Uint64
	compactions           atomic.Uint64
	reconciles            atomic.Uint64
	compactionFailures    atomic.Uint64
	snapshotSaves         atomic.Uint64
	snapshotSaveFailures  atomic.Uint64
	snapshotLoadFallbacks atomic.Uint64
	droppedOverCap        atomic.Uint64
	scanBudgetRefusals    atomic.Uint64
	lastCompactionMillis  atomic.Int64
}

// Stats is a monitoring snapshot (FR-6).
type Stats struct {
	Rows, DeltaRows int
	// DictEntries is the per-dimension dictionary length (base + extras),
	// including entries no longer referenced by any live row. Monotonic
	// between FullCompact/ReplaceAll calls; sustained growth past the live
	// cardinality signals dictionary garbage accumulating.
	DictEntries []int
	// IndexKeyBits is the packed-key width the index dims would need if
	// compacted right now (sum of per-dim bit widths, budget 64). Alert well
	// before 64: a compaction that exceeds it fails with ErrKeyOverflow.
	IndexKeyBits            int
	FullScans               uint64
	Compactions, Reconciles uint64
	CompactionFailures      uint64
	SnapshotSaves           uint64
	SnapshotSaveFailures    uint64
	SnapshotLoadFallbacks   uint64
	DroppedOverCap          uint64 // records dropped because the row cap blocked compaction
	ScanBudgetRefusals      uint64 // queries refused because they exceeded Config.MaxScanRows
	LastCompaction          time.Duration
}

func (s *Store) Stats() Stats {
	v := s.view.Load()
	dictEntries := make([]int, len(s.sc.Dims))
	indexKeyBits := 0
	for i := range s.sc.Dims {
		dictEntries[i] = v.base.dicts[i].len() + v.extras[i].len()
		if i < s.sc.IndexDims {
			indexKeyBits += int(dictBits(dictEntries[i]))
		}
	}
	return Stats{
		Rows:                  v.base.rows(),
		DeltaRows:             v.delta.rows(),
		DictEntries:           dictEntries,
		IndexKeyBits:          indexKeyBits,
		FullScans:             s.st.fullScans.Load(),
		Compactions:           s.st.compactions.Load(),
		Reconciles:            s.st.reconciles.Load(),
		CompactionFailures:    s.st.compactionFailures.Load(),
		SnapshotSaves:         s.st.snapshotSaves.Load(),
		SnapshotSaveFailures:  s.st.snapshotSaveFailures.Load(),
		SnapshotLoadFallbacks: s.st.snapshotLoadFallbacks.Load(),
		DroppedOverCap:        s.st.droppedOverCap.Load(),
		ScanBudgetRefusals:    s.st.scanBudgetRefusals.Load(),
		LastCompaction:        time.Duration(s.st.lastCompactionMillis.Load()) * time.Millisecond,
	}
}
