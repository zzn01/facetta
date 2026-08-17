package facetta

import (
	"fmt"
	"math/bits"
	"slices"
	"time"
)

// snapshot is an immutable, pointer-free columnar table sorted by packed
// index key. Only the dictionaries contain pointers (strings), so GC work
// scales with dictionary cardinality, not row count.
type snapshot struct {
	sc         *Schema
	dicts      []*dict     // per dimension
	dims       [][]uint32  // [dim][row]
	mets       [][]float64 // [metric][row]
	updated    []int64     // unix milli, [row]
	expire     []int64     // unix milli, [row]; 0 = never expires
	keys       []uint64    // packed index prefix per row, ascending
	shifts     []uint      // left shift per index dim (dim 0 highest)
	maxUpdated int64
	minUpdated int64 // smallest updated over kept rows; 0 when empty
	minExpire  int64 // smallest non-zero expireAt over kept rows; 0 when none
}

func (s *snapshot) rows() int { return len(s.updated) }

// trackMin folds one kept row's updated/expire into the snapshot minima:
// minUpdated is the smallest updated seen, minExpire the smallest non-zero
// expireAt seen (0 means no expiring row).
func (s *snapshot) trackMin(updated, expire int64) {
	if s.minUpdated == 0 || updated < s.minUpdated {
		s.minUpdated = updated
	}
	if expire != 0 && (s.minExpire == 0 || expire < s.minExpire) {
		s.minExpire = expire
	}
}

// expireMilli maps a Record.ExpireAt to its stored unix-milli form: zero time
// stays 0 (never expires). time.Time zero value has a negative UnixMilli, so
// treat the zero value explicitly rather than round-tripping it.
func expireMilli(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func dictBits(n int) uint {
	if n <= 2 {
		return 1
	}
	return uint(bits.Len(uint(n - 1)))
}

func computeShifts(cards []int, indexDims int) ([]uint, error) {
	total := uint(0)
	widths := make([]uint, indexDims)
	for i := range indexDims {
		widths[i] = dictBits(cards[i])
		total += widths[i]
	}
	if total > 64 {
		return nil, ErrKeyOverflow
	}
	shifts := make([]uint, indexDims)
	s := uint(0)
	for i := indexDims - 1; i >= 0; i-- {
		shifts[i] = s
		s += widths[i]
	}
	return shifts, nil
}

func packKey(shifts []uint, ids []uint32) uint64 {
	var k uint64
	for i, sh := range shifts {
		k |= uint64(ids[i]) << sh
	}
	return k
}

func emptySnapshot(sc *Schema) *snapshot {
	s := &snapshot{
		sc:    sc,
		dicts: make([]*dict, len(sc.Dims)),
		dims:  make([][]uint32, len(sc.Dims)),
		mets:  make([][]float64, len(sc.Metrics)),
	}
	for i := range s.dicts {
		s.dicts[i] = newDict(sc.isNumeric(i))
	}
	s.shifts, _ = computeShifts(make([]int, sc.IndexDims), sc.IndexDims)
	return s
}

// buildFromRecords builds a fresh snapshot (fresh dictionaries) from source
// records: validates arity, drops rows older than ttlCutoff (unix milli, 0
// keeps all), dedupes by full dimension tuple keeping the newest UpdatedAt,
// then drops any deduped survivor already expired at build-time now (unix
// milli). Expiry is applied AFTER dedup so a newer expired record still
// shadows an older non-expiring duplicate, matching the read-time oracle.
func buildFromRecords(sc *Schema, recs []Record, ttlCutoff, now int64) (*snapshot, error) {
	nd, nm := len(sc.Dims), len(sc.Metrics)
	dicts := make([]*dict, nd)
	for i := range dicts {
		dicts[i] = newDict(sc.isNumeric(i))
	}
	ids := make([]uint32, 0, len(recs)*nd)
	ups := make([]int64, 0, len(recs))
	exps := make([]int64, 0, len(recs))
	metsIn := make([]float64, 0, len(recs)*nm)
	for i, r := range recs {
		if len(r.Dims) != nd || len(r.Metrics) != nm {
			return nil, fmt.Errorf("facetta: record %d arity mismatch (%d dims, %d metrics)", i, len(r.Dims), len(r.Metrics))
		}
		u := r.UpdatedAt.UnixMilli()
		if u < ttlCutoff {
			continue
		}
		for d := range nd {
			sv := r.Dims[d]
			if sc.isNumeric(d) {
				cs, ok := canonNum(sv)
				if !ok {
					return nil, fmt.Errorf("facetta: record %d: non-numeric value %q for numeric dimension %q", i, sv, sc.Dims[d].Name)
				}
				sv = cs
			}
			ids = append(ids, dicts[d].getOrAdd(sv))
		}
		metsIn = append(metsIn, r.Metrics...)
		ups = append(ups, u)
		exps = append(exps, expireMilli(r.ExpireAt))
	}
	n := len(ups)
	cards := make([]int, nd)
	for d := range nd {
		cards[d] = dicts[d].len()
	}
	shifts, err := computeShifts(cards, sc.IndexDims)
	if err != nil {
		return nil, err
	}
	keys := make([]uint64, n)
	for r := range n {
		keys[r] = packKey(shifts, ids[r*nd:r*nd+sc.IndexDims])
	}
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	slices.SortFunc(perm, func(a, b int) int {
		if keys[a] != keys[b] {
			if keys[a] < keys[b] {
				return -1
			}
			return 1
		}
		for d := range nd {
			ia, ib := ids[a*nd+d], ids[b*nd+d]
			if ia != ib {
				if ia < ib {
					return -1
				}
				return 1
			}
		}
		// equal tuple: newest first so dedupe keeps it
		if ups[a] != ups[b] {
			if ups[a] > ups[b] {
				return -1
			}
			return 1
		}
		return 0
	})
	snap := &snapshot{sc: sc, dicts: dicts, shifts: shifts}
	snap.dims = make([][]uint32, nd)
	for d := range snap.dims {
		snap.dims[d] = make([]uint32, 0, n)
	}
	snap.mets = make([][]float64, nm)
	for m := range snap.mets {
		snap.mets[m] = make([]float64, 0, n)
	}
	snap.expire = make([]int64, 0, n)
	sameTuple := func(a, b int) bool {
		for d := range nd {
			if ids[a*nd+d] != ids[b*nd+d] {
				return false
			}
		}
		return true
	}
	for i, p := range perm {
		if i > 0 && sameTuple(perm[i-1], p) {
			continue // duplicate tuple; the first (newest) already kept
		}
		if e := exps[p]; e != 0 && e <= now {
			continue // deduped survivor already expired: reclaim (drop physically)
		}
		for d := range nd {
			snap.dims[d] = append(snap.dims[d], ids[p*nd+d])
		}
		for m := range nm {
			snap.mets[m] = append(snap.mets[m], metsIn[p*nm+m])
		}
		snap.updated = append(snap.updated, ups[p])
		snap.expire = append(snap.expire, exps[p])
		snap.keys = append(snap.keys, keys[p])
		if ups[p] > snap.maxUpdated {
			snap.maxUpdated = ups[p]
		}
		snap.trackMin(ups[p], exps[p])
	}
	return snap, nil
}

func rowExpired(e, now int64) bool { return e != 0 && e <= now }

// mergeView merges the base snapshot (minus overridden, TTL-expired and
// per-row-expired rows) with the delta into a new sorted snapshot. now is
// build-time unix milli used to reclaim expired rows.
//
// With renumber false, dim ids are stable: merged dictionaries are base dicts
// extended by extras in id order, and rows are copied verbatim. With renumber
// true the dictionaries are compacted as well: a mark pass records which ids
// surviving rows reference, survivors are renumbered monotonically per dim
// (order-preserving, so the base stays sorted and the merge remains the same
// linear zip), and packed-key widths are recomputed from the live
// cardinality. The renumbering passes cost ~40-60% of merge wall time
// (BenchmarkCompact1M), which is why Compact gates them on
// Config.DictCompactInterval.
func mergeView(sc *Schema, v *view, ttlCutoff, now int64, renumber bool) (*snapshot, error) {
	nd := len(sc.Dims)
	if !renumber {
		dicts := make([]*dict, nd)
		for i := range nd {
			if v.extras[i].len() == 0 {
				// nothing to fold in: base dicts are immutable once
				// published, so the new snapshot can share them
				dicts[i] = v.base.dicts[i]
				continue
			}
			dicts[i] = v.base.dicts[i].clone()
			for _, s := range v.extras[i].strs {
				dicts[i].getOrAdd(s)
			}
		}
		return zipMerge(sc, v, ttlCutoff, now, dicts, nil)
	}
	base, d := v.base, v.delta
	// mark ids used by surviving rows, in the combined id space
	used := make([][]uint64, nd)
	for i := range nd {
		card := base.dicts[i].len() + v.extras[i].len()
		used[i] = make([]uint64, (card+63)/64)
	}
	for r := 0; r < base.rows(); r++ {
		if bitGet(v.overridden, r) || base.updated[r] < ttlCutoff || rowExpired(base.expire[r], now) {
			continue
		}
		for i := range nd {
			bitSet(used[i], int(base.dims[i][r]))
		}
	}
	for r := 0; r < d.rows(); r++ {
		if d.updated[r] < ttlCutoff || rowExpired(d.expire[r], now) {
			continue
		}
		for i := range nd {
			bitSet(used[i], int(d.dims[i][r]))
		}
	}
	// compacted dictionaries and the monotonic old->new remap
	dicts := make([]*dict, nd)
	remap := make([][]uint32, nd)
	for i := range nd {
		dicts[i] = newDict(sc.isNumeric(i))
		baseLen := base.dicts[i].len()
		card := baseLen + v.extras[i].len()
		remap[i] = make([]uint32, card)
		for old := range card {
			if !bitGet(used[i], old) {
				continue
			}
			s := ""
			if old < baseLen {
				s = base.dicts[i].strs[old]
			} else {
				s = v.extras[i].strs[old-baseLen]
			}
			remap[i][old] = dicts[i].getOrAdd(s)
		}
	}
	return zipMerge(sc, v, ttlCutoff, now, dicts, remap)
}

// zipMerge is the merge body: a linear two-way zip of the (sorted) base with
// the key-sorted delta, skipping overridden, TTL-expired and per-row-expired
// rows. remap translates combined-space ids into the compacted dicts (nil
// means identity: dicts must then extend the base dicts in id order); only
// ids of surviving rows are guaranteed to be mapped, and the map is monotonic
// per dim so the base stays sorted under the new ids.
func zipMerge(sc *Schema, v *view, ttlCutoff, now int64, dicts []*dict, remap [][]uint32) (*snapshot, error) {
	nd, nm := len(sc.Dims), len(sc.Metrics)
	base, d := v.base, v.delta
	cards := make([]int, nd)
	for i := range nd {
		cards[i] = dicts[i].len()
	}
	shifts, err := computeShifts(cards, sc.IndexDims)
	if err != nil {
		return nil, err
	}
	mapID := func(dim int, id uint32) uint32 {
		if remap == nil {
			return id
		}
		return remap[dim][id]
	}
	// delta rows sorted by new key (dropped rows get garbage keys; harmless,
	// the zip below skips them without looking at the key)
	dKeys := make([]uint64, d.rows())
	idBuf := make([]uint32, sc.IndexDims)
	for r := 0; r < d.rows(); r++ {
		for i := 0; i < sc.IndexDims; i++ {
			idBuf[i] = mapID(i, d.dims[i][r])
		}
		dKeys[r] = packKey(shifts, idBuf)
	}
	dPerm := make([]int, d.rows())
	for i := range dPerm {
		dPerm[i] = i
	}
	slices.SortFunc(dPerm, func(a, b int) int {
		if dKeys[a] < dKeys[b] {
			return -1
		}
		if dKeys[a] > dKeys[b] {
			return 1
		}
		return 0
	})
	out := &snapshot{sc: sc, dicts: dicts, shifts: shifts}
	total := base.rows() + d.rows()
	out.dims = make([][]uint32, nd)
	for i := range out.dims {
		out.dims[i] = make([]uint32, 0, total)
	}
	out.mets = make([][]float64, nm)
	for m := range out.mets {
		out.mets[m] = make([]float64, 0, total)
	}
	out.updated = make([]int64, 0, total)
	out.expire = make([]int64, 0, total)
	out.keys = make([]uint64, 0, total)
	appendBase := func(r int, key uint64) {
		for i := range nd {
			out.dims[i] = append(out.dims[i], mapID(i, base.dims[i][r]))
		}
		for m := range nm {
			out.mets[m] = append(out.mets[m], base.mets[m][r])
		}
		out.updated = append(out.updated, base.updated[r])
		out.expire = append(out.expire, base.expire[r])
		out.keys = append(out.keys, key)
	}
	appendDelta := func(r int, key uint64) {
		for i := range nd {
			out.dims[i] = append(out.dims[i], mapID(i, d.dims[i][r]))
		}
		for m := range nm {
			out.mets[m] = append(out.mets[m], d.mets[m][r])
		}
		out.updated = append(out.updated, d.updated[r])
		out.expire = append(out.expire, d.expire[r])
		out.keys = append(out.keys, key)
	}
	// with identity ids and unchanged shifts, base keys carry over verbatim
	keysStable := remap == nil && slices.Equal(shifts, base.shifts)
	baseKey := func(r int) uint64 {
		if keysStable {
			return base.keys[r]
		}
		for i := 0; i < sc.IndexDims; i++ {
			idBuf[i] = mapID(i, base.dims[i][r])
		}
		return packKey(shifts, idBuf)
	}
	bi, di := 0, 0
	var bk uint64
	bkValid := false
	for bi < base.rows() || di < d.rows() {
		// skip dropped base rows
		for bi < base.rows() && (bitGet(v.overridden, bi) || base.updated[bi] < ttlCutoff || rowExpired(base.expire[bi], now)) {
			bi++
			bkValid = false
		}
		dOK := di < d.rows()
		for dOK && (d.updated[dPerm[di]] < ttlCutoff || rowExpired(d.expire[dPerm[di]], now)) {
			di++
			dOK = di < d.rows()
		}
		bOK := bi < base.rows()
		if !bOK && !dOK {
			break
		}
		if bOK && !bkValid {
			bk = baseKey(bi)
			bkValid = true
		}
		if bOK && (!dOK || bk <= dKeys[dPerm[di]]) {
			appendBase(bi, bk)
			bi++
			bkValid = false
		} else if dOK {
			appendDelta(dPerm[di], dKeys[dPerm[di]])
			di++
		}
	}
	for r, u := range out.updated {
		if u > out.maxUpdated {
			out.maxUpdated = u
		}
		out.trackMin(u, out.expire[r])
	}
	return out, nil
}
