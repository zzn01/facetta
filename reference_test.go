package facetta

import (
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
		if di < 0 || r.Dims[di] != c.Value {
			return false
		}
	}
	return true
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
