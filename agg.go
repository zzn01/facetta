package facetta

import (
	"errors"
	"fmt"
	"math"
)

// AggOp selects the aggregate function of one output column.
type AggOp uint8

const (
	AggSum   AggOp = iota // sum of the metric over matched rows; 0 when none match
	AggCount              // number of matched rows; Agg.Metric must be empty
	AggMin                // minimum of the metric; NaN when no row matches
	AggMax                // maximum of the metric; NaN when no row matches
	AggAvg                // sum/count over matched rows; NaN when no row matches
)

// Agg is one requested aggregate output column.
type Agg struct {
	Metric string // metric name from Schema.Metrics; must be empty for AggCount
	Op     AggOp
}

const maxAggs = 16

var errBadAggCount = errors.New("facetta: need 1..16 aggregates")

// resolveAggs validates aggs and fills mets with each column's metric index
// (-1 for AggCount). Fixed-size output, no allocations on success.
func (s *Store) resolveAggs(aggs []Agg, mets *[maxAggs]int) error {
	if len(aggs) == 0 || len(aggs) > maxAggs {
		return errBadAggCount
	}
	for i, a := range aggs {
		if a.Op > AggAvg {
			return fmt.Errorf("facetta: unknown aggregate op %d", a.Op)
		}
		if a.Op == AggCount {
			if a.Metric != "" {
				return fmt.Errorf("facetta: AggCount takes no metric, got %q", a.Metric)
			}
			mets[i] = -1
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

// foldAgg folds one matched row's metric values into the accumulators.
// first marks the overall first matched row (min/max initialization).
func foldAgg(aggs []Agg, mets *[maxAggs]int, acc *[maxAggs]float64, first bool, metric func(m int) float64) {
	for j := range aggs {
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
// when cap(dst) >= len(aggs); the call performs zero heap allocations. Over
// zero matched rows Sum is 0, Count is 0, and Min/Max/Avg are NaN.
func (s *Store) QueryAggs(dst []float64, aggs []Agg, groups [][]Cond) ([]float64, error) {
	var mets [maxAggs]int
	if err := s.resolveAggs(aggs, &mets); err != nil {
		return nil, err
	}
	v := s.view.Load()
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
				if plans[gi].matchBase(v, r) && (ins.pN[gi] == 0 || ins.matchIns(gi, v.base.dims, r)) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			foldAgg(aggs, &mets, &acc, rows == 0, func(m int) float64 { return v.base.mets[m][r] })
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
			if plans[gi].matchDelta(d, r) && (ins.pN[gi] == 0 || ins.matchIns(gi, d.dims, r)) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		foldAgg(aggs, &mets, &acc, rows == 0, func(m int) float64 { return d.mets[m][r] })
		rows++
	}
	dst = dst[:0]
	for j := range aggs {
		var out float64
		switch aggs[j].Op {
		case AggCount:
			out = float64(rows)
		case AggSum:
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
