package facetta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// GroupedResult is a reusable result buffer for QueryGroupBy. The layout is
// flat and row-major: group i's dimension values are
// Keys[i*len(by) : (i+1)*len(by)] and its aggregates
// Aggs[i*len(aggs) : (i+1)*len(aggs)]. Groups are sorted lexicographically by
// their key strings (in by-dim order), so output is deterministic.
//
// Reusing one GroupedResult across calls amortizes its internal storage; the
// remaining per-call allocations are O(result groups) — map key interning and
// sort scratch — never O(scanned rows). Key strings reference the store's
// immutable dictionaries (no copies) and stay valid after the call.
type GroupedResult struct {
	N    int       // number of result groups
	Keys []string  // row-major, N × len(by)
	Aggs []float64 // row-major, N × len(aggs)

	idx   map[string]int // packed id-tuple -> group ordinal (cleared per call)
	skeys []string       // unsorted key scratch
	saggs []float64      // unsorted agg scratch
	cnt   []int          // per-group matched row count
	ord   []int          // sort permutation scratch
	kb    []byte         // key encoding scratch
}

var (
	errNilResult = errors.New("facetta: nil GroupedResult")
	errBadByDims = errors.New("facetta: need at least one group-by dimension")
)

// QueryGroupBy computes the requested aggregates per distinct combination of
// the by dims, over rows matching ANY group (each row counted once, same
// union semantics as QueryGroups). Results are written into res, which must
// be non-nil and is reset first; see GroupedResult for layout, ordering and
// the allocation contract. The scan budget (Config.MaxScanRows) applies.
func (s *Store) QueryGroupBy(res *GroupedResult, by []string, aggs []Agg, groups [][]Cond) error {
	if res == nil {
		return errNilResult
	}
	var mets [maxAggs]int
	if err := s.resolveAggs(aggs, &mets); err != nil {
		return err
	}
	if len(by) == 0 || len(by) > len(s.sc.Dims) {
		return errBadByDims
	}
	var byDims [maxDims]int
	for i, name := range by {
		di := s.sc.dimIndex(name)
		if di < 0 {
			return fmt.Errorf("facetta: unknown dimension %q", name)
		}
		for j := 0; j < i; j++ {
			if byDims[j] == di {
				return fmt.Errorf("facetta: duplicate group-by dimension %q", name)
			}
		}
		byDims[i] = di
	}
	v := s.view.Load()
	var plans [maxGroups]groupPlan
	var ivs [maxGroups]iv
	var ins queryIns
	n, err := s.planGroups(v, groups, &plans, &ivs, &ins)
	if err != nil {
		return err
	}
	baseExp, deltaExp, nowMilli := s.queryClock(v)

	nb, na := len(by), len(aggs)
	res.N = 0
	res.Keys = res.Keys[:0]
	res.Aggs = res.Aggs[:0]
	res.skeys = res.skeys[:0]
	res.saggs = res.saggs[:0]
	res.cnt = res.cnt[:0]
	if res.idx == nil {
		res.idx = map[string]int{}
	} else {
		clear(res.idx) // buckets stay warm across calls
	}

	// visit folds one matched row into its group's accumulators, creating the
	// group from the row on first sight (so min/max need no sentinel init).
	visit := func(dimID func(d int) uint32, met func(m int) float64) {
		kb := res.kb[:0]
		for _, d := range byDims[:nb] {
			kb = binary.LittleEndian.AppendUint32(kb, dimID(d))
		}
		res.kb = kb
		g, ok := res.idx[string(kb)]
		if !ok {
			g = len(res.cnt)
			res.idx[string(kb)] = g
			for _, d := range byDims[:nb] {
				res.skeys = append(res.skeys, v.dimString(d, dimID(d)))
			}
			for j := 0; j < na; j++ {
				if mi := mets[j]; mi >= 0 {
					res.saggs = append(res.saggs, met(mi))
				} else {
					res.saggs = append(res.saggs, 0)
				}
			}
			res.cnt = append(res.cnt, 1)
			return
		}
		res.cnt[g]++
		row := res.saggs[g*na : (g+1)*na]
		for j := 0; j < na; j++ {
			mi := mets[j]
			if mi < 0 {
				continue
			}
			m := met(mi)
			switch aggs[j].Op {
			case AggSum, AggAvg:
				row[j] += m
			case AggMin:
				if m < row[j] {
					row[j] = m
				}
			case AggMax:
				if m > row[j] {
					row[j] = m
				}
			}
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
				continue
			}
			if baseExp {
				if e := v.base.expire[r]; e != 0 && e <= nowMilli {
					continue
				}
			}
			matched := false
			for gi := range groups {
				if plans[gi].matchBase(v, r) && (ins.pN[gi] == 0 || ins.matchIns(gi, v.base.dims, r)) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			visit(func(d int) uint32 { return v.base.dims[d][r] },
				func(m int) float64 { return v.base.mets[m][r] })
		}
		done = hi
	}
	d := v.delta
	for r := 0; r < d.rows(); r++ {
		if deltaExp {
			if e := d.expire[r]; e != 0 && e <= nowMilli {
				continue
			}
		}
		matched := false
		for gi := range groups {
			if plans[gi].matchDelta(d, r) && (ins.pN[gi] == 0 || ins.matchIns(gi, d.dims, r)) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		visit(func(dd int) uint32 { return d.dims[dd][r] },
			func(m int) float64 { return d.mets[m][r] })
	}

	// finalize count/avg columns (every group has cnt >= 1)
	G := len(res.cnt)
	for g := 0; g < G; g++ {
		row := res.saggs[g*na : (g+1)*na]
		for j := 0; j < na; j++ {
			switch aggs[j].Op {
			case AggCount:
				row[j] = float64(res.cnt[g])
			case AggAvg:
				row[j] /= float64(res.cnt[g])
			}
		}
	}
	// deterministic output: sort groups by their key strings in by-dim order
	res.ord = res.ord[:0]
	for g := 0; g < G; g++ {
		res.ord = append(res.ord, g)
	}
	sort.Slice(res.ord, func(a, b int) bool {
		ka := res.skeys[res.ord[a]*nb : (res.ord[a]+1)*nb]
		kb2 := res.skeys[res.ord[b]*nb : (res.ord[b]+1)*nb]
		for k := 0; k < nb; k++ {
			if ka[k] != kb2[k] {
				return ka[k] < kb2[k]
			}
		}
		return false
	})
	for _, g := range res.ord {
		res.Keys = append(res.Keys, res.skeys[g*nb:(g+1)*nb]...)
		res.Aggs = append(res.Aggs, res.saggs[g*na:(g+1)*na]...)
	}
	res.N = G
	return nil
}
