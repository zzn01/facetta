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

var errBadAggCount = errors.New("facetta: need at least one aggregate")

// resolveAggs validates aggs and appends each column's metric index (-1 for
// AggCount/AggDistinct) to qs.mets and its distinct-dim index (-1 for
// everything but AggDistinct) to qs.ddims. qs.mets/qs.ddims are pre-sized by
// the caller's routing (scratchBack.fast or getPooledScratch), so these
// appends (written as manual reslice-then-set, not `append` — see the
// CAUTION on queryScratch) never reallocate.
func (s *Store) resolveAggs(aggs []Agg, qs *queryScratch) error {
	if len(aggs) == 0 {
		return errBadAggCount
	}
	for _, a := range aggs {
		if a.Op > AggDistinct {
			return fmt.Errorf("facetta: unknown aggregate op %d", a.Op)
		}
		qs.mets = qs.mets[:len(qs.mets)+1]
		qs.mets[len(qs.mets)-1] = -1
		qs.ddims = qs.ddims[:len(qs.ddims)+1]
		qs.ddims[len(qs.ddims)-1] = -1
		j := len(qs.mets) - 1
		if a.Op == AggDistinct {
			if a.Metric != "" {
				return fmt.Errorf("facetta: AggDistinct takes no metric, got %q", a.Metric)
			}
			di := s.sc.dimIndex(a.Dim)
			if di < 0 {
				return fmt.Errorf("facetta: unknown dimension %q", a.Dim)
			}
			qs.ddims[j] = di
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
		qs.mets[j] = mi
	}
	return nil
}

// foldAgg folds one matched row into the accumulators. first marks the
// overall first matched row (min/max initialization). Distinct columns
// test-and-set the row's dim id in their bitmap and count new ids directly
// in acc, so no popcount pass is needed at the end.
func foldAgg(aggs []Agg, mets, ddims []int, acc []float64, bms [][]uint64, first bool, metric func(m int) float64, dimID func(d int) uint32) {
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
	// Routing is inlined here rather than behind a shared helper: see the
	// comment on scratchBack for why.
	sh := measureShape(groups, len(aggs))
	var qs, pooled *queryScratch
	var back scratchBack
	var local queryScratch
	if sh.fits() {
		local = back.fast()
		qs = &local
	} else {
		pooled = getPooledScratch(sh)
		qs = pooled
	}
	out, err := s.queryAggs(dst, aggs, groups, qs)
	// release only the pooled pointer, never &local: see the comment in
	// QueryGroups for why calling release on the stack value would defeat
	// its zero-alloc guarantee.
	if pooled != nil {
		pooled.release()
	}
	return out, err
}

func (s *Store) queryAggs(dst []float64, aggs []Agg, groups [][]Cond, qs *queryScratch) ([]float64, error) {
	if err := s.resolveAggs(aggs, qs); err != nil {
		return nil, err
	}
	v := s.view.Load()
	// Distinct columns are the documented exception to zero allocations:
	// one id bitmap each, sized by the dim's combined cardinality (known up
	// front for this view). Queries without AggDistinct allocate nothing.
	// qs.bms may carry stale bitmaps (wrong size, or for a different dim)
	// from an earlier query on a pooled/reused scratch, so every slot is
	// reset to nil before the make loop below.
	qs.bms = qs.bms[:len(aggs)]
	for j := range qs.bms {
		qs.bms[j] = nil
	}
	for j := range aggs {
		if d := qs.ddims[j]; d >= 0 {
			card := v.base.dicts[d].len() + v.extras[d].len()
			qs.bms[j] = make([]uint64, (card+63)/64)
		}
	}
	n, err := s.planGroups(v, groups, qs)
	if err != nil {
		return nil, err
	}
	baseExp, deltaExp, nowMilli := s.queryClock(v)
	qs.acc = qs.acc[:len(aggs)]
	for j := range qs.acc {
		qs.acc[j] = 0 // pooled slice may carry a previous query's values
	}
	rows := 0
	done := 0
	for i := 0; i < n; i++ {
		lo, hi := qs.ivs[i].lo, qs.ivs[i].hi
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
			for gi := range qs.plans {
				p := &qs.plans[gi]
				if p.matchBase(qs, v, r) &&
					(p.insN == 0 || matchIns(qs.inWins[p.insOff:p.insOff+p.insN], qs.inPool, v.base.dims, r)) &&
					(p.rngN == 0 || matchRanges(qs.rWins[p.rngOff:p.rngOff+p.rngN], v, v.base.dims, r)) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			foldAgg(aggs, qs.mets, qs.ddims, qs.acc, qs.bms, rows == 0,
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
		for gi := range qs.plans {
			p := &qs.plans[gi]
			if p.matchDelta(qs, d, r) &&
				(p.insN == 0 || matchIns(qs.inWins[p.insOff:p.insOff+p.insN], qs.inPool, d.dims, r)) &&
				(p.rngN == 0 || matchRanges(qs.rWins[p.rngOff:p.rngOff+p.rngN], v, d.dims, r)) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		foldAgg(aggs, qs.mets, qs.ddims, qs.acc, qs.bms, rows == 0,
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
			out = qs.acc[j]
		default: // AggMin, AggMax, AggAvg: NaN over zero rows
			out = math.NaN()
			if rows > 0 {
				out = qs.acc[j]
				if aggs[j].Op == AggAvg {
					out /= float64(rows)
				}
			}
		}
		dst = append(dst, out)
	}
	return dst, nil
}
