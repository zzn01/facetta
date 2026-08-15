package facetta

import (
	"errors"
	"fmt"
	"sort"
)

// Cond is one equality condition: dimension Dim must equal Value.
type Cond struct {
	Dim, Value string
}

var (
	errBadGroupCount = errors.New("facetta: need 1..16 filter groups")
	errTooManyConds  = errors.New("facetta: too many conditions in group")
)

type groupPlan struct {
	nConds       int
	condDims     [maxConds]int
	condIDs      [maxConds]uint32
	lo, hi       int  // base candidate row interval
	scan         bool // full base scan
	dead         bool // matches nothing anywhere
	basePossible bool // all cond ids exist in base dicts
}

// plan resolves one group against v: dict-encodes conditions, finds the
// longest fully-specified index-dim prefix and binary-searches its key range.
func (v *view) plan(sc *Schema, g []Cond, p *groupPlan) error {
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

// QueryGroups sums all metrics over rows matching ANY group. dst is reused
// when cap(dst) >= len(Metrics); the call performs zero heap allocations.
func (s *Store) QueryGroups(dst []float64, groups [][]Cond) ([]float64, error) {
	if len(groups) == 0 || len(groups) > maxGroups {
		return nil, errBadGroupCount
	}
	v := s.view.Load()
	sc := &s.sc
	baseExp := v.base.minExpire != 0
	deltaExp := v.delta.hasExpiry
	var nowMilli int64 // one now per query; expired rows are invisible
	if baseExp || deltaExp {
		nowMilli = s.now().UnixMilli() // sampled only when a row could expire
		// earliest base expiry still in the future: no base row can be
		// expired this query, skip the per-row expire-column reads entirely
		baseExp = baseExp && v.base.minExpire <= nowMilli
	}
	dst = dst[:0]
	for range sc.Metrics {
		dst = append(dst, 0)
	}
	var plans [maxGroups]groupPlan
	for gi := range groups {
		if err := v.plan(sc, groups[gi], &plans[gi]); err != nil {
			return nil, err
		}
		if plans[gi].scan {
			s.st.fullScans.Add(1)
		}
	}
	// collect base candidate intervals and union-iterate them
	type iv struct{ lo, hi int }
	var ivs [maxGroups]iv
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
	// touched. Sum the rows the sweep below WILL visit, using the identical
	// `done` high-water dedup (overlapping/nested intervals counted once), add
	// the delta rows, and fail fast if it exceeds MaxScanRows. This is
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
			return nil, ErrScanBudget
		}
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
				if plans[gi].matchBase(v, r) {
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
			if plans[gi].matchDelta(d, r) {
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
