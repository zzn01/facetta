package facetta

import (
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
)

// delta is the small mutable-by-copy overlay of freshly upserted rows,
// columnar like the base but unsorted and scanned linearly. Dim ids live in
// the combined space: id < base card -> base dict, else extras[dim].
type delta struct {
	dims       [][]uint32
	mets       [][]float64
	updated    []int64
	expire     []int64        // unix milli, [row]; 0 = never expires
	index      map[string]int // packed id-tuple -> delta row
	maxUpdated int64
	hasExpiry  bool // true once any stored row has a non-zero expire; sticky
}

func emptyDelta(sc *Schema) *delta {
	return &delta{
		dims:  make([][]uint32, len(sc.Dims)),
		mets:  make([][]float64, len(sc.Metrics)),
		index: map[string]int{},
	}
}

func (d *delta) rows() int { return len(d.updated) }

// view is the immutable unit readers load via one atomic pointer.
type view struct {
	base       *snapshot
	delta      *delta
	extras     []*dict  // per dim: strings first seen after the last compaction
	overridden []uint64 // bitmap over base rows shadowed by delta rows
}

func newView(base *snapshot) *view {
	extras := make([]*dict, len(base.sc.Dims))
	for i := range extras {
		extras[i] = newDict(base.sc.isNumeric(i))
	}
	return &view{base: base, delta: emptyDelta(base.sc), extras: extras}
}

func (v *view) lookupID(dim int, s string) (uint32, bool) {
	if id, ok := v.base.dicts[dim].lookup(s); ok {
		return id, true
	}
	if id, ok := v.extras[dim].lookup(s); ok {
		return uint32(v.base.dicts[dim].len()) + id, true
	}
	return 0, false
}

// lookupNumID resolves a numeric dim's condition value: parse it, render the
// canonical spelling into a stack buffer, and look that up allocation-free
// (map indexing with string(bytes) does not allocate). valid is false when
// the value does not parse as a finite number — a caller error on a numeric
// dim, reported rather than silently matching nothing.
func (v *view) lookupNumID(dim int, s string) (id uint32, found, valid bool) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false, false
	}
	if f == 0 {
		f = 0 // collapse -0 into +0, mirroring canonNum
	}
	var buf [32]byte
	b := strconv.AppendFloat(buf[:0], f, 'g', -1, 64)
	if id, ok := v.base.dicts[dim].lookupB(b); ok {
		return id, true, true
	}
	if id, ok := v.extras[dim].lookupB(b); ok {
		return uint32(v.base.dicts[dim].len()) + id, true, true
	}
	return 0, false, true
}

// dimString resolves a combined-space dim id back to its string: below the
// base dictionary length it is a base id, above it an extras id.
func (v *view) dimString(dim int, id uint32) string {
	if n := uint32(v.base.dicts[dim].len()); id >= n {
		return v.extras[dim].strs[id-n]
	}
	return v.base.dicts[dim].strs[id]
}

func (v *view) maxUpdated() int64 {
	if v.delta.maxUpdated > v.base.maxUpdated {
		return v.delta.maxUpdated
	}
	return v.base.maxUpdated
}

func bitGet(b []uint64, i int) bool {
	return len(b) > i>>6 && b[i>>6]&(1<<(uint(i)&63)) != 0
}

func bitSet(b []uint64, i int) { b[i>>6] |= 1 << (uint(i) & 63) }

func idKey(buf []byte, ids []uint32) []byte {
	buf = buf[:0]
	for _, id := range ids {
		buf = binary.LittleEndian.AppendUint32(buf, id)
	}
	return buf
}

// findRow locates a row by its full dim id tuple; -1 if absent. Only valid
// for ids entirely within this snapshot's dictionaries. Rows sharing the
// packed index key are scanned linearly, so a low-cardinality index prefix
// with high-cardinality non-index dims degrades toward O(rows); pick
// Schema.IndexDims so the leading dims are selective.
func (s *snapshot) findRow(ids []uint32) int {
	key := packKey(s.shifts, ids[:s.sc.IndexDims])
	lo := sort.Search(len(s.keys), func(i int) bool { return s.keys[i] >= key })
	for r := lo; r < len(s.keys) && s.keys[r] == key; r++ {
		match := true
		for d := range ids {
			if s.dims[d][r] != ids[d] {
				match = false
				break
			}
		}
		if match {
			return r
		}
	}
	return -1
}

// applyDelta returns a new view with recs upserted into a copied delta, plus
// the count of records dropped because the row cap is blocked (see capBlocked).
// While capBlocked, records that would create a new row (tuple neither in the
// delta index nor the base) are dropped; updates to existing tuples still flow.
// Per-record ExpireAt is carried through verbatim: already-expired rows are
// stored (invisible via the query-time skip) and reclaimed at the next merge,
// so dedup/upsert ordering stays identical to the read-time oracle.
// Writer-path only; one call is O(DeltaRows + len(recs)) from the
// copy-on-write below, so hosts should batch records (see Store.Apply).
func (v *view) applyDelta(sc *Schema, recs []Record, ttlCutoff int64, capBlocked bool) (*view, int, error) {
	nd, nm := len(sc.Dims), len(sc.Metrics)
	nv := &view{base: v.base}
	// extras are shared with the previous view and cloned lazily, per dim,
	// only when this batch adds a string that dim has not seen: an unmutated
	// dict is immutable, so sharing across views is safe.
	nv.extras = slices.Clone(v.extras)
	var extrasOwned [maxDims]bool
	nv.overridden = append([]uint64(nil), v.overridden...)
	if nv.overridden == nil && v.base.rows() > 0 {
		nv.overridden = make([]uint64, (v.base.rows()+63)/64)
	}
	nd2 := &delta{
		dims:    make([][]uint32, nd),
		mets:    make([][]float64, nm),
		updated: append([]int64(nil), v.delta.updated...),
		expire:  append([]int64(nil), v.delta.expire...),
		// row order is preserved by the column copies above, so the old
		// index is correct as-is; cloning avoids re-encoding every row
		index:      maps.Clone(v.delta.index),
		maxUpdated: v.delta.maxUpdated,
		hasExpiry:  v.delta.hasExpiry,
	}
	for d := range nd {
		nd2.dims[d] = append([]uint32(nil), v.delta.dims[d]...)
	}
	for m := range nm {
		nd2.mets[m] = append([]float64(nil), v.delta.mets[m]...)
	}
	nv.delta = nd2
	ids := make([]uint32, nd)
	var kb []byte
	dropped := 0
	for i, rec := range recs {
		if len(rec.Dims) != nd || len(rec.Metrics) != nm {
			return nil, dropped, fmt.Errorf("facetta: record %d arity mismatch", i)
		}
		u := rec.UpdatedAt.UnixMilli()
		if u < ttlCutoff {
			continue
		}
		e := expireMilli(rec.ExpireAt)
		allInBase := true
		for d := range nd {
			sv := rec.Dims[d]
			if sc.isNumeric(d) {
				cs, ok := canonNum(sv)
				if !ok {
					return nil, dropped, fmt.Errorf("facetta: record %d: non-numeric value %q for numeric dimension %q", i, sv, sc.Dims[d].Name)
				}
				sv = cs
			}
			if id, ok := v.base.dicts[d].lookup(sv); ok {
				ids[d] = id
				continue
			}
			allInBase = false
			baseLen := uint32(v.base.dicts[d].len())
			if id, ok := nv.extras[d].lookup(sv); ok {
				ids[d] = baseLen + id
				continue
			}
			if !extrasOwned[d] {
				nv.extras[d] = nv.extras[d].clone()
				extrasOwned[d] = true
			}
			ids[d] = baseLen + nv.extras[d].getOrAdd(sv)
		}
		kb = idKey(kb, ids)
		if r, ok := nd2.index[string(kb)]; ok {
			if nd2.updated[r] <= u { // newer (or equal) upsert replaces whole row
				for m := range nm {
					nd2.mets[m][r] = rec.Metrics[m]
				}
				nd2.updated[r] = u
				nd2.expire[r] = e
				if e != 0 {
					nd2.hasExpiry = true
				}
			}
		} else {
			br := -1
			if allInBase {
				if br = v.base.findRow(ids); br >= 0 && v.base.updated[br] > u {
					continue // base row is strictly newer: stale upsert, matches oracle
				}
			}
			if capBlocked && br < 0 {
				dropped++ // would create a new row while the row cap is blocked
				continue
			}
			r := nd2.rows()
			for d := range nd {
				nd2.dims[d] = append(nd2.dims[d], ids[d])
			}
			for m := range nm {
				nd2.mets[m] = append(nd2.mets[m], rec.Metrics[m])
			}
			nd2.updated = append(nd2.updated, u)
			nd2.expire = append(nd2.expire, e)
			nd2.index[string(kb)] = r
			if e != 0 {
				nd2.hasExpiry = true
			}
			if br >= 0 {
				bitSet(nv.overridden, br)
			}
		}
		if u > nd2.maxUpdated {
			nd2.maxUpdated = u
		}
	}
	return nv, dropped, nil
}
