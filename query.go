package facetta

import (
	"errors"
	"fmt"
	"sort"
)

// Cond is one condition on a dimension: either equality (Value) or set
// membership (In).
type Cond struct {
	Dim, Value string
	// In, when non-empty, matches rows whose Dim equals ANY listed value;
	// Value must be empty then. IN conditions filter rows but do not
	// contribute to index-prefix planning: a group whose leading index dims
	// carry only IN conditions degrades to a full scan (guarded by
	// Config.MaxScanRows). For indexed multi-value queries use one group per
	// value instead. At most 16 values per condition, 16 IN conditions and
	// 128 resolved values per query.
	In []string
}

var (
	errBadGroupCount   = errors.New("facetta: need 1..16 filter groups")
	errTooManyConds    = errors.New("facetta: too many conditions in group")
	errCondValueAndIn  = errors.New("facetta: Cond.Value and Cond.In are mutually exclusive")
	errTooManyInValues = errors.New("facetta: too many IN values in query")
)

const (
	maxInConds = 16  // total IN conditions per query
	maxInIDs   = 128 // total resolved IN values per query
)

// queryIns is the per-query pool of resolved IN conditions, shared by all
// group plans: plan gi owns the contiguous window
// [pOff[gi], pOff[gi]+pN[gi]) of dims/offs/lens, and condition k's candidate
// ids live in pool[offs[k] : offs[k]+lens[k]]. Everything IN-related lives
// here rather than in groupPlan so the frozen query path keeps its exact
// stack scratch: declared arrays are zero-initialized on every call, and a
// per-plan id array was measured to cost double-digit percentages on the
// indexed query.
type queryIns struct {
	nConds int
	nIDs   int
	pOff   [maxGroups]uint8 // per-plan window start into dims/offs/lens
	pN     [maxGroups]uint8 // per-plan window length
	dims   [maxInConds]uint8
	offs   [maxInConds]uint16
	lens   [maxInConds]uint8
	pool   [maxInIDs]uint32
}

type groupPlan struct {
	nConds       int
	condDims     [maxConds]int
	condIDs      [maxConds]uint32
	lo, hi       int  // base candidate row interval
	scan         bool // full base scan
	dead         bool // matches nothing anywhere
	basePossible bool // some base row could satisfy every cond
}

// plan resolves one group against v: dict-encodes conditions, finds the
// longest fully-specified index-dim prefix and binary-searches its key range.
// ins may be nil only when no condition in g uses In (see hasInConds).
func (v *view) plan(sc *Schema, g []Cond, p *groupPlan, ins *queryIns) error {
	*p = groupPlan{basePossible: true}
	if len(g) > maxConds {
		return errTooManyConds
	}
	if len(g) == 0 {
		p.scan = true // empty group matches every row
		return nil
	}
	for _, c := range g {
		di := sc.dimIndex(c.Dim)
		if di < 0 {
			return fmt.Errorf("facetta: unknown dimension %q", c.Dim)
		}
		if len(c.In) > 0 {
			if c.Value != "" {
				return errCondValueAndIn
			}
			if len(c.In) > maxInVals || ins.nConds == maxInConds {
				return errTooManyInValues
			}
			off, cnt := ins.nIDs, 0
			anyBase := false
			for _, val := range c.In {
				id, ok := v.lookupID(di, val)
				if !ok {
					continue // value nowhere in the table: drop from the set
				}
				if off+cnt == maxInIDs {
					return errTooManyInValues
				}
				ins.pool[off+cnt] = id
				cnt++
				if int(id) < v.base.dicts[di].len() {
					anyBase = true
				}
			}
			if cnt == 0 {
				p.dead = true // no listed value exists anywhere
				return nil
			}
			k := ins.nConds
			ins.dims[k] = uint8(di)
			ins.offs[k] = uint16(off)
			ins.lens[k] = uint8(cnt)
			ins.nConds++
			ins.nIDs = off + cnt
			if !anyBase {
				p.basePossible = false // every candidate id is delta-only
			}
			continue
		}
		id, ok := v.lookupID(di, c.Value)
		if !ok {
			p.dead = true // value nowhere in the table
			return nil
		}
		p.condDims[p.nConds] = di
		p.condIDs[p.nConds] = id
		p.nConds++
	}
	base := v.base
	for i := 0; i < p.nConds; i++ {
		if int(p.condIDs[i]) >= base.dicts[p.condDims[i]].len() {
			p.basePossible = false // delta-only value: no base row can match
		}
	}
	if !p.basePossible {
		return nil // lo == hi == 0: skip base, delta scan still applies
	}
	var pref [maxDims]uint32
	var has [maxDims]bool
	for i := 0; i < p.nConds; i++ {
		if d := p.condDims[i]; d < sc.IndexDims {
			has[d], pref[d] = true, p.condIDs[i]
		}
	}
	k := 0
	for k < sc.IndexDims && has[k] {
		k++
	}
	if k == 0 {
		p.scan = true // no usable prefix: degraded full scan
		return nil
	}
	var lo64 uint64
	for i := 0; i < k; i++ {
		lo64 |= uint64(pref[i]) << base.shifts[i]
	}
	hi64 := lo64 | ((uint64(1) << base.shifts[k-1]) - 1)
	p.lo = sort.Search(len(base.keys), func(i int) bool { return base.keys[i] >= lo64 })
	p.hi = sort.Search(len(base.keys), func(i int) bool { return base.keys[i] > hi64 })
	return nil
}

// matchIns checks plan gi's IN conditions against one row's dim ids. It is
// deliberately NOT folded into matchBase/matchDelta: their bodies sit under
// the compiler's inline budget and the per-row sweeps rely on that; adding
// the IN logic (or even a call to it) pushes them over and costs ~20% on the
// indexed hot path (measured). Call sites short-circuit on `ins == nil ||
// ins.pN[gi] == 0` instead, so queries without IN conditions pay one
// never-taken branch per matched row and nothing else.
func (q *queryIns) matchIns(gi int, dims [][]uint32, r int) bool {
	off := int(q.pOff[gi])
	for k := off; k < off+int(q.pN[gi]); k++ {
		id := dims[q.dims[k]][r]
		lo, hi := int(q.offs[k]), int(q.offs[k])+int(q.lens[k])
		ok := false
		for j := lo; j < hi; j++ {
			if id == q.pool[j] {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

// matchBase checks equality conditions only; IN conditions are checked by the
// caller via matchIns (see its comment for why).
func (p *groupPlan) matchBase(v *view, r int) bool {
	if p.dead || !p.basePossible {
		return false
	}
	if !p.scan && (r < p.lo || r >= p.hi) {
		return false
	}
	for i := 0; i < p.nConds; i++ {
		if v.base.dims[p.condDims[i]][r] != p.condIDs[i] {
			return false
		}
	}
	return true
}

// matchDelta checks equality conditions only; IN conditions are checked by
// the caller via matchIns (see its comment for why).
func (p *groupPlan) matchDelta(d *delta, r int) bool {
	if p.dead {
		return false
	}
	for i := 0; i < p.nConds; i++ {
		if d.dims[p.condDims[i]][r] != p.condIDs[i] {
			return false
		}
	}
	return true
}

// iv is one base candidate row interval [lo, hi).
type iv struct{ lo, hi int }

// planGroups plans every group against v, collects the deduped base candidate
// intervals sorted by lo, and enforces the scan budget. It fills the caller's
// fixed-size arrays and performs no heap allocations, so every query entry
// point (QueryGroups, QueryAggs, QueryGroupBy) shares one planner.
func (s *Store) planGroups(v *view, groups [][]Cond, plans *[maxGroups]groupPlan, ivs *[maxGroups]iv, ins *queryIns) (int, error) {
	if len(groups) == 0 || len(groups) > maxGroups {
		return 0, errBadGroupCount
	}
	sc := &s.sc
	for gi := range groups {
		off := 0
		if ins != nil {
			off = ins.nConds
		}
		if err := v.plan(sc, groups[gi], &plans[gi], ins); err != nil {
			return 0, err
		}
		if ins != nil {
			ins.pOff[gi] = uint8(off)
			ins.pN[gi] = uint8(ins.nConds - off)
		}
		if plans[gi].scan {
			s.st.fullScans.Add(1)
		}
	}
	// collect base candidate intervals for the union sweep
	n := 0
	for gi := range groups {
		p := &plans[gi]
		if p.dead || !p.basePossible {
			continue
		}
		lo, hi := p.lo, p.hi
		if p.scan {
			lo, hi = 0, v.base.rows()
		}
		if lo < hi {
			ivs[n] = iv{lo, hi}
			n++
		}
	}
	// insertion sort by lo (n <= 16)
	for i := 1; i < n; i++ {
		for j := i; j > 0 && ivs[j].lo < ivs[j-1].lo; j-- {
			ivs[j], ivs[j-1] = ivs[j-1], ivs[j]
		}
	}
	// Scan budget: after planning, total work is known before any row is
	// touched. Sum the rows the sweep WILL visit, using the identical `done`
	// high-water dedup (overlapping/nested intervals counted once), add the
	// delta rows, and fail fast if it exceeds MaxScanRows. This is
	// O(#intervals) and allocation-free; ErrScanBudget is returned bare so the
	// refusal path stays zero-alloc (see spec). A full scan's [0,N) interval
	// naturally exceeds any sane budget on a large table.
	if s.cfg.MaxScanRows > 0 {
		scanRows := v.delta.rows()
		hw := 0 // mirrors the sweep's `done`
		for i := 0; i < n; i++ {
			lo, hi := ivs[i].lo, ivs[i].hi
			if lo < hw {
				lo = hw
			}
			if hi <= hw {
				continue
			}
			scanRows += hi - lo
			hw = hi
		}
		if scanRows > s.cfg.MaxScanRows {
			s.st.scanBudgetRefusals.Add(1)
			return 0, ErrScanBudget
		}
	}
	return n, nil
}

// queryClock samples the per-query expiry clock. baseExp/deltaExp report
// whether the base sweep and delta scan need per-row expire checks at all;
// the clock is only read when a row could actually be expired.
func (s *Store) queryClock(v *view) (baseExp, deltaExp bool, nowMilli int64) {
	baseExp = v.base.minExpire != 0
	deltaExp = v.delta.hasExpiry
	if baseExp || deltaExp {
		nowMilli = s.now().UnixMilli() // one now per query; results stay consistent
		// earliest base expiry still in the future: no base row can be
		// expired this query, skip the per-row expire-column reads entirely
		baseExp = baseExp && v.base.minExpire <= nowMilli
	}
	return baseExp, deltaExp, nowMilli
}

// hasInConds reports whether any group carries an IN condition.
func hasInConds(groups [][]Cond) bool {
	for _, g := range groups {
		for _, c := range g {
			if len(c.In) > 0 {
				return true
			}
		}
	}
	return false
}

// QueryGroups sums all metrics over rows matching ANY group. dst is reused
// when cap(dst) >= len(Metrics); the call performs zero heap allocations.
func (s *Store) QueryGroups(dst []float64, groups [][]Cond) ([]float64, error) {
	// The IN scratch pool lives in this thin wrapper and only when needed:
	// zero-initializing its ~600B on every call costs ~15% on the indexed
	// fast path (measured), so IN-free queries pass nil and never pay it.
	if !hasInConds(groups) {
		return s.queryGroups(dst, groups, nil)
	}
	var ins queryIns
	return s.queryGroups(dst, groups, &ins)
}

func (s *Store) queryGroups(dst []float64, groups [][]Cond, ins *queryIns) ([]float64, error) {
	v := s.view.Load()
	var plans [maxGroups]groupPlan
	var ivs [maxGroups]iv
	n, err := s.planGroups(v, groups, &plans, &ivs, ins)
	if err != nil {
		return nil, err
	}
	baseExp, deltaExp, nowMilli := s.queryClock(v)
	dst = dst[:0]
	for range s.sc.Metrics {
		dst = append(dst, 0)
	}
	done := 0
	for i := 0; i < n; i++ {
		lo, hi := ivs[i].lo, ivs[i].hi
		if lo < done {
			lo = done
		}
		if hi <= done {
			continue
		}
		for r := lo; r < hi; r++ {
			if bitGet(v.overridden, r) {
				continue // shadowed by a newer delta row
			}
			if baseExp {
				if e := v.base.expire[r]; e != 0 && e <= nowMilli {
					continue // expired: invisible to queries
				}
			}
			for gi := range groups {
				if plans[gi].matchBase(v, r) && (ins == nil || ins.pN[gi] == 0 || ins.matchIns(gi, v.base.dims, r)) {
					for m := range dst {
						dst[m] += v.base.mets[m][r]
					}
					break
				}
			}
		}
		done = hi
	}
	// delta overlay: always a linear scan, delta is small by construction
	d := v.delta
	for r := 0; r < d.rows(); r++ {
		if deltaExp {
			if e := d.expire[r]; e != 0 && e <= nowMilli {
				continue // expired: invisible to queries
			}
		}
		for gi := range groups {
			if plans[gi].matchDelta(d, r) && (ins == nil || ins.pN[gi] == 0 || ins.matchIns(gi, d.dims, r)) {
				for m := range dst {
					dst[m] += d.mets[m][r]
				}
				break
			}
		}
	}
	return dst, nil
}

// Query is the variadic form of QueryGroups: it sums all metrics over rows
// matching ANY group. dst is reused when cap(dst) >= len(Metrics); the call
// performs zero heap allocations.
func (s *Store) Query(dst []float64, groups ...[]Cond) ([]float64, error) {
	return s.QueryGroups(dst, groups)
}
