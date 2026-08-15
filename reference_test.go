package facetta

import (
	"math"
	"strings"
	"testing"
	"time"
)

// refTable is the naive reference implementation (spec §1): one heap object
// per row, keyed by the full dimension tuple, linear scan on query. It is the
// semantic oracle for the columnar engine.
type refTable struct {
	sc   Schema
	data map[string]Record
	now  func() time.Time // injectable clock for per-record expiry visibility
}

func newRefTable(sc Schema) *refTable {
	return &refTable{sc: sc, data: map[string]Record{}, now: time.Now}
}

func refKey(dims []string) string { return strings.Join(dims, "\x1f") }

func (t *refTable) apply(recs []Record) {
	for _, r := range recs {
		k := refKey(r.Dims)
		if old, ok := t.data[k]; ok && old.UpdatedAt.After(r.UpdatedAt) {
			continue
		}
		t.data[k] = r
	}
}

func (t *refTable) replaceAll(recs []Record) {
	t.data = map[string]Record{}
	t.apply(recs)
}

func (t *refTable) expire(cutoff time.Time) {
	for k, r := range t.data {
		if r.UpdatedAt.Before(cutoff) {
			delete(t.data, k)
		}
	}
}

// reclaimExpired models physical reclaim (Compact/FullCompact/ReplaceAll
// build): rows expired at the given instant are forgotten entirely, dropping
// the tuple's updated watermark. A later out-of-order OLDER record then
// recreates the row on both sides (defined resurrection semantics; such
// drift is repaired by host-driven ReplaceAll). Call this whenever the store
// under test performs a merge or full rebuild.
func (t *refTable) reclaimExpired(now time.Time) {
	for k, r := range t.data {
		if !r.ExpireAt.IsZero() && !now.Before(r.ExpireAt) {
			delete(t.data, k)
		}
	}
}

func (t *refTable) rows() int { return len(t.data) }

// visibleRows counts entries visible at the current clock (per-record expiry).
func (t *refTable) visibleRows() int {
	n := 0
	for _, r := range t.data {
		if t.visible(r) {
			n++
		}
	}
	return n
}

func (t *refTable) matches(r Record, g []Cond) bool {
	for _, c := range g {
		di := t.sc.dimIndex(c.Dim)
		if di < 0 {
			return false
		}
		if len(c.In) > 0 {
			ok := false
			for _, v := range c.In {
				if r.Dims[di] == v {
					ok = true
					break
				}
			}
			if !ok {
				return false
			}
		} else if r.Dims[di] != c.Value {
			return false
		}
	}
	return true
}

func (t *refTable) metricIndex(name string) int {
	for i, m := range t.sc.Metrics {
		if m == name {
			return i
		}
	}
	return -1
}

// queryAggs mirrors Store.QueryAggs: one output per requested aggregate over
// rows matching ANY group (each row counted once). Min/Max/Avg over zero
// matched rows are NaN.
func (t *refTable) queryAggs(aggs []Agg, groups [][]Cond) []float64 {
	nm := len(t.sc.Metrics)
	sums := make([]float64, nm)
	mins := make([]float64, nm)
	maxs := make([]float64, nm)
	n := 0
	for _, r := range t.data {
		if !t.visible(r) {
			continue
		}
		hit := false
		for _, g := range groups {
			if t.matches(r, g) {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		for m := range nm {
			v := r.Metrics[m]
			sums[m] += v
			if n == 0 || v < mins[m] {
				mins[m] = v
			}
			if n == 0 || v > maxs[m] {
				maxs[m] = v
			}
		}
		n++
	}
	out := make([]float64, len(aggs))
	for i, a := range aggs {
		switch a.Op {
		case AggCount:
			out[i] = float64(n)
		case AggSum:
			out[i] = sums[t.metricIndex(a.Metric)]
		case AggMin:
			out[i] = math.NaN()
			if n > 0 {
				out[i] = mins[t.metricIndex(a.Metric)]
			}
		case AggMax:
			out[i] = math.NaN()
			if n > 0 {
				out[i] = maxs[t.metricIndex(a.Metric)]
			}
		case AggAvg:
			out[i] = math.NaN()
			if n > 0 {
				out[i] = sums[t.metricIndex(a.Metric)] / float64(n)
			}
		}
	}
	return out
}

// queryGroupBy mirrors Store.QueryGroupBy: the requested aggregates per
// distinct combination of the by dims, over rows matching ANY group (each row
// counted once). Map keys are the by values joined with \x1f.
func (t *refTable) queryGroupBy(by []string, aggs []Agg, groups [][]Cond) map[string][]float64 {
	nm := len(t.sc.Metrics)
	type acc struct {
		sums, mins, maxs []float64
		n                int
	}
	accs := map[string]*acc{}
	for _, r := range t.data {
		if !t.visible(r) {
			continue
		}
		hit := false
		for _, g := range groups {
			if t.matches(r, g) {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		parts := make([]string, len(by))
		for i, d := range by {
			parts[i] = r.Dims[t.sc.dimIndex(d)]
		}
		k := strings.Join(parts, "\x1f")
		a := accs[k]
		if a == nil {
			a = &acc{sums: make([]float64, nm), mins: make([]float64, nm), maxs: make([]float64, nm)}
			accs[k] = a
		}
		for m := range nm {
			v := r.Metrics[m]
			a.sums[m] += v
			if a.n == 0 || v < a.mins[m] {
				a.mins[m] = v
			}
			if a.n == 0 || v > a.maxs[m] {
				a.maxs[m] = v
			}
		}
		a.n++
	}
	out := map[string][]float64{}
	for k, a := range accs {
		row := make([]float64, len(aggs))
		for i, g := range aggs {
			switch g.Op {
			case AggCount:
				row[i] = float64(a.n)
			case AggSum:
				row[i] = a.sums[t.metricIndex(g.Metric)]
			case AggMin:
				row[i] = a.mins[t.metricIndex(g.Metric)]
			case AggMax:
				row[i] = a.maxs[t.metricIndex(g.Metric)]
			case AggAvg:
				row[i] = a.sums[t.metricIndex(g.Metric)] / float64(a.n)
			}
		}
		out[k] = row
	}
	return out
}

// visible reports whether a record is visible at the oracle's current clock:
// invisible iff ExpireAt is non-zero and now is at or past it.
func (t *refTable) visible(r Record) bool {
	return r.ExpireAt.IsZero() || t.now().Before(r.ExpireAt)
}

func (t *refTable) query(groups [][]Cond) []float64 {
	out := make([]float64, len(t.sc.Metrics))
	for _, r := range t.data {
		if !t.visible(r) {
			continue // expired: invisible, matches store read-skip
		}
		for _, g := range groups {
			if t.matches(r, g) {
				for m := range out {
					out[m] += r.Metrics[m]
				}
				break // count each row once (union semantics)
			}
		}
	}
	return out
}

func ts(sec int64) time.Time { return time.Unix(sec, 0).UTC() }

func TestRefTableSemantics(t *testing.T) {
	sc := testSchema()
	rt := newRefTable(sc)
	rt.apply([]Record{
		rec(ts(100), []float64{10, 1.5}, "s1", "a1", "p1", "US", "ios"),
		rec(ts(100), []float64{20, 2.5}, "s1", "a1", "p2", "US", "android"),
		rec(ts(100), []float64{40, 4.0}, "s2", "a2", "p1", "DE", "ios"),
	})
	// upsert replaces (same full tuple, newer timestamp)
	rt.apply([]Record{rec(ts(200), []float64{11, 1.6}, "s1", "a1", "p1", "US", "ios")})
	if rt.rows() != 3 {
		t.Fatalf("rows = %d, want 3", rt.rows())
	}
	// stale upsert ignored
	rt.apply([]Record{rec(ts(50), []float64{999, 999}, "s1", "a1", "p1", "US", "ios")})

	got := rt.query([][]Cond{{{Dim: "source", Value: "s1"}}})
	want := []float64{31, 4.1}
	for m := range want {
		if got[m] != want[m] {
			t.Fatalf("query = %v, want %v", got, want)
		}
	}

	// subset match: unspecified dims not compared
	got = rt.query([][]Cond{{{Dim: "os", Value: "ios"}}})
	if got[0] != 51 {
		t.Fatalf("os=ios visits = %v, want 51", got[0])
	}

	// union without double counting: overlapping groups
	got = rt.query([][]Cond{
		{{Dim: "source", Value: "s1"}},
		{{Dim: "os", Value: "ios"}}, // s1/p1 row matches both groups
	})
	if got[0] != 71 { // 11+20+40, s1/p1 counted once
		t.Fatalf("union visits = %v, want 71", got[0])
	}

	// TTL expiry
	rt.expire(ts(150))
	if rt.rows() != 1 {
		t.Fatalf("after expire rows = %d, want 1", rt.rows())
	}
}
