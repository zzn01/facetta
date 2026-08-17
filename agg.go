package facetta

import (
	"errors"
	"fmt"
	"math"
)

// AggOp selects the aggregate function of one output column.
type AggOp uint8

const (
	AggSum      AggOp = iota // sum of the metric over matched rows; 0 when none match
	AggCount                 // number of matched rows; Agg.Metric must be empty
	AggMin                   // minimum of the metric; NaN when no row matches
	AggMax                   // maximum of the metric; NaN when no row matches
	AggAvg                   // sum/count over matched rows; NaN when no row matches
	AggDistinct              // COUNT(DISTINCT Agg.Dim) over matched rows; 0 when none match
)

// Agg is one requested aggregate output column.
type Agg struct {
	Metric string // metric name from Schema.Metrics; empty for AggCount/AggDistinct
	Dim    string // dimension name; set exactly for AggDistinct
	Op     AggOp
}

const maxAggs = 16

var errBadAggCount = errors.New("facetta: need 1..16 aggregates")

// resolveAggs validates aggs and fills mets with each column's metric index
// (-1 for AggCount/AggDistinct) and ddims with each column's distinct-dim
// index (-1 for everything but AggDistinct). Fixed-size output, no
// allocations on success.
func (s *Store) resolveAggs(aggs []Agg, mets, ddims *[maxAggs]int) error {
	if len(aggs) == 0 || len(aggs) > maxAggs {
		return errBadAggCount
	}
	for i, a := range aggs {
		if a.Op > AggDistinct {
			return fmt.Errorf("facetta: unknown aggregate op %d", a.Op)
		}
		mets[i], ddims[i] = -1, -1
		if a.Op == AggDistinct {
			if a.Metric != "" {
				return fmt.Errorf("facetta: AggDistinct takes no metric, got %q", a.Metric)
			}
			di := s.sc.dimIndex(a.Dim)
			if di < 0 {
				return fmt.Errorf("facetta: unknown dimension %q", a.Dim)
			}
			ddims[i] = di
			continue
		}
		if a.Dim != "" {
			return fmt.Errorf("facetta: only AggDistinct takes a dimension, got %q", a.Dim)
		}
		if a.Op == AggCount {
			if a.Metric != "" {
				return fmt.Errorf("facetta: AggCount takes no metric, got %q", a.Metric)
			}
			continue
		}
		mi := -1
		for m, name := range s.sc.Metrics {
			if name == a.Metric {
				mi = m
				break
			}
		}
		if mi < 0 {
			return fmt.Errorf("facetta: unknown metric %q", a.Metric)
		}
		mets[i] = mi
	}
	return nil
}

// foldAgg folds one matched row into the accumulators. first marks the
// overall first matched row (min/max initialization). Distinct columns
// test-and-set the row's dim id in their bitmap and count new ids directly
// in acc, so no popcount pass is needed at the end.
func foldAgg(aggs []Agg, mets, ddims *[maxAggs]int, acc *[maxAggs]float64, bms *[maxAggs][]uint64, first bool, metric func(m int) float64, dimID func(d int) uint32) {
	for j := range aggs {
		if d := ddims[j]; d >= 0 {
			id := dimID(d)
			w, b := id>>6, uint64(1)<<(id&63)
			if bms[j][w]&b == 0 {
				bms[j][w] |= b
				acc[j]++
			}
			continue
		}
		mi := mets[j]
		if mi < 0 {
			continue // AggCount: derived from the row counter
		}
		v := metric(mi)
		switch aggs[j].Op {
		case AggSum, AggAvg:
			acc[j] += v
		case AggMin:
			if first || v < acc[j] {
				acc[j] = v
			}
		case AggMax:
			if first || v > acc[j] {
				acc[j] = v
			}
		}
	}
}

// QueryAggs computes the requested aggregates over rows matching ANY group
// (each row counted once, same union semantics as QueryGroups). dst is reused
// when cap(dst) >= len(aggs); the call performs zero heap allocations, except
// that each AggDistinct column allocates one id bitmap (O(dim cardinality/64)
// words). Over zero matched rows Sum, Count and Distinct are 0, and
// Min/Max/Avg are NaN.
func (s *Store) QueryAggs(dst []float64, aggs []Agg, groups [][]Cond) ([]float64, error) {
	var mets, ddims [maxAggs]int
	if err := s.resolveAggs(aggs, &mets, &ddims); err != nil {
		return nil, err
	}
	v := s.view.Load()
	// Distinct columns are the documented exception to zero allocations:
	// one id bitmap each, sized by the dim's combined cardinality (known up
	// front for this view). Queries without AggDistinct allocate nothing.
	var bms [maxAggs][]uint64
	for j := range aggs {
		if d := ddims[j]; d >= 0 {
			card := v.base.dicts[d].len() + v.extras[d].len()
			bms[j] = make([]uint64, (card+63)/64)
		}
	}
	var plans [maxGroups]groupPlan
	var ivs [maxGroups]iv
	var ins queryIns
	n, err := s.planGroups(v, groups, &plans, &ivs, &ins)
	if err != nil {
		return nil, err
	}
	baseExp, deltaExp, nowMilli := s.queryClock(v)
	var acc [maxAggs]float64
	rows := 0
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
				if plans[gi].matchBase(v, r) &&
					(ins.pN[gi] == 0 || ins.matchIns(gi, v.base.dims, r)) &&
					(ins.rN[gi] == 0 || ins.matchRanges(gi, v, v.base.dims, r)) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			foldAgg(aggs, &mets, &ddims, &acc, &bms, rows == 0,
				func(m int) float64 { return v.base.mets[m][r] },
				func(dd int) uint32 { return v.base.dims[dd][r] })
			rows++
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
			if plans[gi].matchDelta(d, r) &&
				(ins.pN[gi] == 0 || ins.matchIns(gi, d.dims, r)) &&
				(ins.rN[gi] == 0 || ins.matchRanges(gi, v, d.dims, r)) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		foldAgg(aggs, &mets, &ddims, &acc, &bms, rows == 0,
			func(m int) float64 { return d.mets[m][r] },
			func(dd int) uint32 { return d.dims[dd][r] })
		rows++
	}
	dst = dst[:0]
	for j := range aggs {
		var out float64
		switch aggs[j].Op {
		case AggCount:
			out = float64(rows)
		case AggSum, AggDistinct: // both accumulate directly; 0 over no rows
			out = acc[j]
		default: // AggMin, AggMax, AggAvg: NaN over zero rows
			out = math.NaN()
			if rows > 0 {
				out = acc[j]
				if aggs[j].Op == AggAvg {
					out /= float64(rows)
				}
			}
		}
		dst = append(dst, out)
	}
	return dst, nil
}
