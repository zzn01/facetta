package facetta

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

// TestEquivalenceRandomized drives the columnar store and the naive oracle
// with identical random workloads (apply batches, compactions, full replaces)
// and asserts every random query agrees exactly (acceptance criterion 1).
// Metric values are integer-valued so float sums are exact in any order.
func TestEquivalenceRandomized(t *testing.T) {
	for seed := range int64(5) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			sc := testSchema()
			sc.Dims[3].Type = DimNumeric // country
			// odd seeds gate dictionary compaction so both merge paths
			// (id-stable and renumbering) run under the random workload
			cfg := Config{}
			if seed%2 == 1 {
				cfg.DictCompactInterval = time.Minute
			}
			s, err := New(sc, cfg)
			if err != nil {
				t.Fatal(err)
			}
			// Shared deterministic clock: the store and oracle apply the same
			// per-record expiry visibility rule; advancing it mid-run flips
			// future-expiring rows to expired without any compaction.
			nowSec := int64(2000)
			clock := func() time.Time { return ts(nowSec) }
			s.now = clock
			rt := newRefTable(sc)
			rt.now = clock
			card := 4 // low cardinality to force collisions and overlaps

			randGroups := func() [][]Cond {
				ng := 1 + rng.Intn(3)
				groups := make([][]Cond, ng)
				for g := range groups {
					nc := rng.Intn(4)
					for range nc {
						d := rng.Intn(len(sc.Dims))
						val := func() string {
							if sc.Dims[d].Name == "country" {
								// numeric dim: random spelling of the same number
								n := rng.Intn(card + 1)
								switch rng.Intn(3) {
								case 0:
									return fmt.Sprintf("%d.0", n)
								case 1:
									return fmt.Sprintf("%02d", n)
								default:
									return fmt.Sprintf("%d", n)
								}
							}
							return fmt.Sprintf("%s%d", sc.Dims[d].Name[:1], rng.Intn(card+1)) // sometimes absent
						}
						switch {
						case rng.Intn(4) == 0:
							// ~1 in 4 conditions is an IN set of 1-3 values
							in := make([]string, 1+rng.Intn(3))
							for i := range in {
								in[i] = val()
							}
							groups[g] = append(groups[g], Cond{Dim: sc.Dims[d].Name, In: in})
						case rng.Intn(4) == 0 && sc.Dims[d].Name == "country":
							// numeric dim: sometimes a range (possibly inverted -> empty)
							lo := float64(rng.Intn(card + 2))
							hi := float64(rng.Intn(card + 2))
							groups[g] = append(groups[g], Cond{Dim: "country", Range: &Range{Min: lo, Max: hi}})
						default:
							groups[g] = append(groups[g], Cond{Dim: sc.Dims[d].Name, Value: val()})
						}
					}
				}
				return groups
			}
			randAggs := func() []Agg {
				aggs := make([]Agg, 1+rng.Intn(4))
				for i := range aggs {
					op := AggOp(rng.Intn(int(AggDistinct) + 1))
					a := Agg{Op: op}
					switch op {
					case AggCount:
					case AggDistinct:
						a.Dim = sc.Dims[rng.Intn(len(sc.Dims))].Name
					default:
						a.Metric = sc.Metrics[rng.Intn(len(sc.Metrics))]
					}
					aggs[i] = a
				}
				return aggs
			}
			sameNaN := func(a, b float64) bool {
				return a == b || (math.IsNaN(a) && math.IsNaN(b))
			}
			var res GroupedResult // reused across all group-by checks
			check := func(step string) {
				t.Helper()
				for range 20 {
					groups := randGroups()
					got, err := s.QueryGroups(nil, groups)
					if err != nil {
						t.Fatalf("%s: %v", step, err)
					}
					want := rt.query(groups)
					for m := range want {
						if got[m] != want[m] {
							t.Fatalf("%s: query %v: got %v want %v", step, groups, got, want)
						}
					}

					aggs := randAggs()
					gotA, err := s.QueryAggs(nil, aggs, groups)
					if err != nil {
						t.Fatalf("%s: aggs: %v", step, err)
					}
					wantA := rt.queryAggs(aggs, groups)
					for i := range wantA {
						if !sameNaN(gotA[i], wantA[i]) {
							t.Fatalf("%s: aggs %v query %v: got %v want %v", step, aggs, groups, gotA, wantA)
						}
					}

					by := make([]string, 0, 2)
					for _, d := range rng.Perm(len(sc.Dims))[:1+rng.Intn(2)] {
						by = append(by, sc.Dims[d].Name)
					}
					if err := s.QueryGroupBy(&res, by, aggs, groups); err != nil {
						t.Fatalf("%s: group-by: %v", step, err)
					}
					gotG := groupedToMap(&res, len(by), len(aggs))
					wantG := rt.queryGroupBy(by, aggs, groups)
					if len(gotG) != len(wantG) {
						t.Fatalf("%s: group-by %v query %v: got %d groups want %d", step, by, groups, len(gotG), len(wantG))
					}
					for k, w := range wantG {
						g, ok := gotG[k]
						if !ok {
							t.Fatalf("%s: group-by %v query %v: missing group %q", step, by, groups, k)
						}
						for i := range w {
							if !sameNaN(g[i], w[i]) {
								t.Fatalf("%s: group-by %v aggs %v query %v: group %q got %v want %v", step, by, aggs, groups, k, g, w)
							}
						}
					}
				}
			}

			tsCounter := int64(1000)
			randBatch := func(n int) []Record {
				recs := make([]Record, n)
				for i := range recs {
					tsCounter++
					r := randomRecord(rng, card)
					// numeric country in a random spelling: canonical identity
					// must collapse them identically on both sides
					n := rng.Intn(card + 1)
					switch rng.Intn(3) {
					case 0:
						r.Dims[3] = fmt.Sprintf("%d.0", n)
					case 1:
						r.Dims[3] = fmt.Sprintf("%02d", n)
					default:
						r.Dims[3] = fmt.Sprintf("%d", n)
					}
					if rng.Intn(10) == 0 {
						// ~1 in 10 records are out-of-order older timestamps so
						// stale upserts against newer rows get exercised.
						r.UpdatedAt = ts(tsCounter - int64(1+rng.Intn(50)))
					} else {
						r.UpdatedAt = ts(tsCounter)
					}
					// Three-state per-record expiry: mostly never, some future,
					// some already past (relative to the shared clock).
					switch rng.Intn(6) {
					case 0:
						r.ExpireAt = ts(nowSec - int64(1+rng.Intn(500))) // already expired
					case 1, 2:
						r.ExpireAt = ts(nowSec + int64(1+rng.Intn(500))) // future
					default:
						// zero: never expires (majority)
					}
					recs[i] = r
				}
				return recs
			}

			for op := range 40 {
				// Occasionally advance the shared clock so future-expiring rows
				// flip to expired mid-run (read-time visibility, no compaction).
				if rng.Intn(5) == 0 {
					nowSec += int64(50 + rng.Intn(300))
				}
				switch rng.Intn(10) {
				case 0, 1, 2, 3, 4, 5: // incremental batch
					b := randBatch(1 + rng.Intn(20))
					if err := s.Apply(b); err != nil {
						t.Fatal(err)
					}
					rt.apply(b)
				case 6, 7, 8: // compact
					if err := s.Compact(); err != nil {
						t.Fatal(err)
					}
					rt.reclaimExpired(clock()) // merges reclaim expired rows on both sides
				default: // full reconcile with a random subset removed
					all := randBatch(30 + rng.Intn(50))
					if err := s.ReplaceAll(all); err != nil {
						t.Fatal(err)
					}
					rt.replaceAll(all)
					rt.reclaimExpired(clock()) // the full build drops expired survivors
				}
				check(fmt.Sprintf("op %d", op))
				// Compare visible rows: physical reclaim may drop expired rows
				// from the store that the oracle still holds (filtered at query).
				if s.Rows()+s.DeltaRows() < rt.visibleRows() {
					t.Fatalf("op %d: store has fewer visible rows than oracle (store=%d, delta=%d, oracle-visible=%d)", op, s.Rows(), s.DeltaRows(), rt.visibleRows())
				}
			}
		})
	}
}
