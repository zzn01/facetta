package facetta

import (
	"cmp"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"sort"
)

// Cond is one condition on a dimension: either equality (Value) or set
// membership (In).
type Cond struct {
	Dim, Value string
	// In, when non-empty, matches rows whose Dim equals ANY listed value;
	// Value must be empty then. An IN on a leading index dim joins
	// index-prefix planning: the planner expands it into one key interval
	// per candidate value (and per combination when several index dims carry
	// INs), so multi-value filters stay indexed instead of degrading to a
	// full scan. Expansion is budgeted — a set so large that probing every
	// combination would cost more than scanning the table falls back to the
	// scan (guarded by Config.MaxScanRows) — and INs on non-index dims stay
	// pure row filters. In has no count limit; values absent from the table
	// are simply dropped from the set.
	In []string
	// Range, when non-nil, matches rows whose Dim value (an int64 — see
	// DimInt: identity on such dims IS the integer) falls within
	// [Min, Max] (inclusive; use math.MinInt64/MaxInt64 for open ends).
	// Mutually exclusive with Value and In. Like In, Range filters rows but
	// does not join index-prefix planning. Range has no count limit either;
	// queries are guarded only by Config.MaxScanRows.
	Range *Range
}

// Range is a closed integer interval for Cond.Range.
type Range struct{ Min, Max int64 }

var (
	errBadGroupCount     = errors.New("facetta: need at least one filter group")
	errCondValueAndIn    = errors.New("facetta: Cond.Value and Cond.In are mutually exclusive")
	errCondRangeConflict = errors.New("facetta: Cond.Range is mutually exclusive with Value and In")
)

// groupPlan is one OR-group's resolved plan. condOff/condN, insOff/insN and
// rngOff/rngN are offset+count pairs into the query's queryScratch pools
// (condDims/condIDs, inWins, rWins) — NOT materialized sub-slices. See the
// CAUTION on queryScratch for why: storing an actual []int32/[]inWindow/
// []rangeWindow into a groupPlan held in qs.plans (an indexed slice) forces
// the whole per-query scratch onto the heap, unconditionally, regardless of
// inlining or of using an array instead of a slice for qs.plans. groupPlan
// itself holds no backing storage.
type groupPlan struct {
	condOff, condN int32 // equality conds: offset+count into qs.condDims/condIDs
	insOff, insN   int32 // offset+count into qs.inWins
	rngOff, rngN   int32 // offset+count into qs.rWins
	lo, hi         int   // base candidate row interval
	scan           bool  // full base scan
	dead           bool  // matches nothing anywhere
	basePossible   bool  // some base row could satisfy every cond
}

// plan resolves one group against v: dict-encodes conditions, finds the
// longest index-dim prefix it can cover (equality conds pin a dim, IN conds
// expand into candidate ids) and emits the resulting base candidate key
// intervals straight into qs.ivs — a plan contributes one interval per
// expanded combination, so planGroups no longer collects them.
//
// reserve is the number of groups still to be planned after this one. Every
// plan can emit at least one interval, so expansion here may only consume
// the qs.ivs slots beyond that reservation; qs.ivs is pre-sized by the
// caller's routing and must never grow (see the CAUTION on queryScratch).
//
// All variable-length outputs live in qs pools; p holds offset+count views
// (see the comment on groupPlan for why not sub-slices).
func (v *view) plan(sc *Schema, g []Cond, p *groupPlan, qs *queryScratch, reserve int) error {
	*p = groupPlan{basePossible: true}
	if len(g) == 0 {
		// empty group matches every row
		p.scan = true
		if n := v.base.rows(); n > 0 {
			qs.ivs = qs.ivs[:len(qs.ivs)+1] // reserve guarantees one free slot
			qs.ivs[len(qs.ivs)-1] = iv{0, n}
		}
		return nil
	}
	condOff := len(qs.condDims)
	inOff := len(qs.inWins)
	rOff := len(qs.rWins)
	for _, c := range g {
		di := sc.dimIndex(c.Dim)
		if di < 0 {
			return fmt.Errorf("facetta: unknown dimension %q", c.Dim)
		}
		if c.Range != nil {
			if c.Value != "" || len(c.In) > 0 {
				return errCondRangeConflict
			}
			if !sc.isInt(di) {
				return fmt.Errorf("facetta: dimension %q is not DimInt", c.Dim)
			}
			qs.rWins = qs.rWins[:len(qs.rWins)+1]
			qs.rWins[len(qs.rWins)-1] = rangeWindow{dim: int32(di), min: c.Range.Min, max: c.Range.Max}
			continue
		}
		if len(c.In) > 0 {
			if c.Value != "" {
				return errCondValueAndIn
			}
			off := len(qs.inPool)
			anyBase := false
			for _, val := range c.In {
				var id uint32
				var ok bool
				if sc.isInt(di) {
					var valid bool
					id, ok, valid = v.lookupNumID(di, val)
					if !valid {
						return fmt.Errorf("facetta: non-integer value %q for integer dimension %q", val, c.Dim)
					}
				} else {
					id, ok = v.lookupID(di, val)
				}
				if !ok {
					continue // value nowhere in the table: drop from the set
				}
				qs.inPool = qs.inPool[:len(qs.inPool)+1]
				qs.inPool[len(qs.inPool)-1] = id
				if int(id) < v.base.dicts[di].len() {
					anyBase = true
				}
			}
			if len(qs.inPool) == off {
				p.dead = true // no listed value exists anywhere
				return nil
			}
			// Sort this window's segment in place: matchIns binary-searches it
			// for large windows, and Task 5's IN-index expansion needs it
			// sorted too. anyBase above is already fully computed per-id
			// during the append loop, so sorting the segment afterward can't
			// affect it. Duplicate ids (from duplicate input values) stay;
			// harmless for both linear and binary matching.
			slices.Sort(qs.inPool[off:len(qs.inPool)])
			qs.inWins = qs.inWins[:len(qs.inWins)+1]
			qs.inWins[len(qs.inWins)-1] = inWindow{dim: int32(di), off: int32(off), n: int32(len(qs.inPool) - off)}
			if !anyBase {
				p.basePossible = false // every candidate id is delta-only
			}
			continue
		}
		var id uint32
		var ok bool
		if sc.isInt(di) {
			var valid bool
			id, ok, valid = v.lookupNumID(di, c.Value)
			if !valid {
				return fmt.Errorf("facetta: non-integer value %q for integer dimension %q", c.Value, c.Dim)
			}
		} else {
			id, ok = v.lookupID(di, c.Value)
		}
		if !ok {
			p.dead = true // value nowhere in the table
			return nil
		}
		qs.condDims = qs.condDims[:len(qs.condDims)+1]
		qs.condDims[len(qs.condDims)-1] = int32(di)
		qs.condIDs = qs.condIDs[:len(qs.condIDs)+1]
		qs.condIDs[len(qs.condIDs)-1] = id
	}
	// Offset+count captured now, after this group's conds are fully appended
	// above: see the CAUTION on queryScratch for why that ordering matters.
	p.condOff, p.condN = int32(condOff), int32(len(qs.condDims)-condOff)
	p.insOff, p.insN = int32(inOff), int32(len(qs.inWins)-inOff)
	p.rngOff, p.rngN = int32(rOff), int32(len(qs.rWins)-rOff)
	base := v.base
	for i := int32(0); i < p.condN; i++ {
		d := qs.condDims[p.condOff+i]
		if int(qs.condIDs[p.condOff+i]) >= base.dicts[d].len() {
			p.basePossible = false // delta-only value: no base row can match
		}
	}
	if !p.basePossible {
		return nil // lo == hi == 0: skip base, delta scan still applies
	}
	if p.insN > 0 {
		// An IN cond may cover an index-prefix dim, which turns planning
		// into a candidate expansion. That lives in its own frame
		// (planExpand) so this function — the path every equality-only
		// query takes — pays neither its scratch arrays nor its odometer;
		// keeping the two apart is worth ~4% on multi-group queries
		// (measured, see the task report).
		return v.planExpand(sc, p, qs, reserve)
	}
	var pref [maxDims]uint32
	var has [maxDims]bool
	for i := int32(0); i < p.condN; i++ {
		d := qs.condDims[p.condOff+i]
		if int(d) < sc.IndexDims {
			has[d], pref[d] = true, qs.condIDs[p.condOff+i]
		}
	}
	k := 0
	for k < sc.IndexDims && has[k] {
		k++
	}
	if k == 0 {
		p.scan = true // no usable prefix: degraded full scan
		if n := base.rows(); n > 0 {
			qs.ivs = qs.ivs[:len(qs.ivs)+1] // reserve guarantees one free slot
			qs.ivs[len(qs.ivs)-1] = iv{0, n}
		}
		return nil
	}
	var lo64 uint64
	for i := 0; i < k; i++ {
		lo64 |= uint64(pref[i]) << base.shifts[i]
	}
	hi64 := lo64 | ((uint64(1) << base.shifts[k-1]) - 1)
	p.lo = sort.Search(len(base.keys), func(i int) bool { return base.keys[i] >= lo64 })
	p.hi = sort.Search(len(base.keys), func(i int) bool { return base.keys[i] > hi64 })
	if p.lo < p.hi {
		qs.ivs = qs.ivs[:len(qs.ivs)+1] // reserve guarantees one free slot
		qs.ivs[len(qs.ivs)-1] = iv{p.lo, p.hi}
	}
	return nil
}

// planExpand is plan()'s tail for a group carrying at least one IN condition:
// index-prefix dims may be covered by candidate id SETS, so the plan can emit
// one key interval per candidate combination instead of a single one. Split
// out of plan() to keep its scratch arrays and odometer off the equality-only
// hot path (see the call site). Preconditions are plan()'s: the group's conds
// are resolved into qs, p's offset+count views are set, p is neither dead nor
// base-impossible, and reserve interval slots are owed to later groups.
//
//go:noinline
func (v *view) planExpand(sc *Schema, p *groupPlan, qs *queryScratch, reserve int) error {
	base := v.base
	// Index-prefix coverage: a dim joins the prefix when an equality cond
	// pins it, or when an IN cond supplies a candidate id set to expand over.
	// Range conds never join. A dim carrying BOTH an equality and an IN is
	// covered by the equality alone and the IN stays a pure row filter: the
	// equality is never the wider of the two, and matchIns still applies the
	// set to every visited row, so the AND is exact either way.
	var eqID [maxDims]uint32
	var hasEq [maxDims]bool
	for i := int32(0); i < p.condN; i++ {
		d := qs.condDims[p.condOff+i]
		if int(d) < sc.IndexDims {
			hasEq[d], eqID[d] = true, qs.condIDs[p.condOff+i]
		}
	}
	// Per index dim, that dim's IN window narrowed to its BASE-RESIDENT
	// candidate ids, as an offset+count into qs.inPool (offsets rather than
	// []uint32 locals: see the CAUTION on queryScratch). Segments are sorted
	// ascending, so the base-resident ones are exactly the prefix below the
	// base dictionary length — delta-only candidates cannot name a base row
	// and would only produce empty intervals.
	var candOff, candN [maxDims]int32
	for i := int32(0); i < p.insN; i++ {
		w := qs.inWins[p.insOff+i]
		// A second IN on an already-covered dim is skipped: the first
		// window's candidates are a superset of the two sets' AND (so the
		// intervals stay complete), and matchIns applies both windows to
		// every visited row anyway.
		if int(w.dim) >= sc.IndexDims || hasEq[w.dim] || candN[w.dim] != 0 {
			continue
		}
		n, _ := slices.BinarySearch(qs.inPool[w.off:w.off+w.n], uint32(base.dicts[w.dim].len()))
		// n > 0 always: a window with no base-resident candidate at all
		// cleared p.basePossible in plan(), which returned before calling us.
		candOff[w.dim], candN[w.dim] = w.off, int32(n)
	}
	// Greedy cost crossover, parameter-free: each emitted combination costs
	// two binary searches over the key column (~ceil(log2 N) probes each), so
	// extending the prefix by an IN dim pays only while the resulting
	// combination count P times log2 N stays under a full scan's N rows. The
	// second cap is the free room in the shared interval sink minus reserve
	// (see plan's doc comment): qs.ivs must never grow, so expansion stops
	// instead of overflowing it. room is >= 1 by that reservation, so the
	// single-interval paths below always fit.
	nRows := base.rows()
	logN := bits.Len(uint(nRows))
	room := cap(qs.ivs) - len(qs.ivs) - reserve
	combos, k := 1, 0
	for k < sc.IndexDims {
		if hasEq[k] {
			k++
			continue
		}
		c := int(candN[k])
		if c == 0 {
			break // dim uncovered: the prefix ends here
		}
		n := combos * c
		if n*logN > nRows || n > room {
			break // expansion stops paying for itself, or would not fit
		}
		combos, k = n, k+1
	}
	if k == 0 {
		p.scan = true // no affordable prefix: degraded full scan
		if nRows > 0 {
			qs.ivs = qs.ivs[:len(qs.ivs)+1] // room >= 1
			qs.ivs[len(qs.ivs)-1] = iv{0, nRows}
		}
		return nil
	}
	// Odometer over the k-dim prefix: the least-significant covered dim
	// advances fastest and carries left, standard mixed-radix counting. Each
	// dim's candidate ids are sorted ascending (see plan()), but IN windows
	// are not deduplicated, so a repeated id in a more-significant dim can
	// make the packed-key sequence dip back down right after a carry (the
	// duplicate re-emits an already-seen combination's key, not a new higher
	// one). The emitted intervals are therefore NOT guaranteed ascending or
	// pairwise disjoint in general — only free of duplicates entirely does
	// that hold. That's harmless downstream: planGroups sorts every interval
	// by lo before the sweep, and the sweep's high-water mark dedups overlap,
	// so a re-emitted interval is simply skipped the second time.
	//
	// p.lo/p.hi are therefore tracked as an explicit min/max hull over every
	// emitted interval (0,0 when none), not inferred from emission order.
	// matchBase's `r < p.lo || r >= p.hi` prefilter stays exact under it: a
	// base row whose prefix ids match some candidate combination carries that
	// combination's packed key, hence by key sort order lies INSIDE that
	// combination's interval, hence inside [p.lo, p.hi). A row sitting in a
	// hull gap therefore differs from every combination on at least one
	// covered dim — on an equality-covered dim matchBase's own loop rejects
	// it, on an IN-covered one matchIns does, since base rows only ever carry
	// base-resident ids and the cutoff above hides none of those from them.
	var idx [maxDims]int32
	for {
		var lo64 uint64
		for d := 0; d < k; d++ {
			id := eqID[d]
			if !hasEq[d] {
				id = qs.inPool[candOff[d]+idx[d]]
			}
			lo64 |= uint64(id) << base.shifts[d]
		}
		hi64 := lo64 | ((uint64(1) << base.shifts[k-1]) - 1)
		lo := sort.Search(len(base.keys), func(i int) bool { return base.keys[i] >= lo64 })
		hi := sort.Search(len(base.keys), func(i int) bool { return base.keys[i] > hi64 })
		if lo < hi {
			qs.ivs = qs.ivs[:len(qs.ivs)+1] // at most combos <= room writes
			qs.ivs[len(qs.ivs)-1] = iv{lo, hi}
			// Explicit hull min/max — see the comment above the loop for why
			// emission order can't be trusted to be ascending under
			// duplicate IN ids. p.hi == 0 doubles as "no interval yet",
			// safe because plan() zeroed *p and every emitted hi is >= 1
			// (lo < hi and lo >= 0).
			if p.hi == 0 || lo < p.lo {
				p.lo = lo
			}
			if hi > p.hi {
				p.hi = hi
			}
		}
		d := k - 1
		for ; d >= 0; d-- {
			if hasEq[d] {
				continue // pinned dim: nothing to advance
			}
			idx[d]++
			if idx[d] < candN[d] {
				break
			}
			idx[d] = 0 // carry into the next dim to the left
		}
		if d < 0 {
			break // odometer wrapped: every combination emitted
		}
	}
	return nil
}

// inLinearMax is matchIns's linear-scan/binary-search threshold: windows with
// at most this many candidates are probed linearly (a branchy linear scan
// beats a binary search's pointer-chasing at this size on real hardware);
// larger windows binary search instead. Deliberately not tied to
// fastInConds/fastInIDs (scratch.go) — those bound total query-shape size for
// stack-vs-pool routing, a different concern; this value is coincidentally
// also 16.
const inLinearMax = 16

// matchIns checks a plan's IN conditions against one row's dim ids. ins is a
// freshly-sliced view built by the caller (qs.inWins[p.insOff:p.insOff+p.insN])
// and pool is qs.inPool, needed to resolve each window's off/n into actual
// candidate ids (see the comment on groupPlan for why inWindow doesn't hold
// its own []uint32). It is deliberately NOT folded into matchBase/matchDelta:
// their bodies sit under the compiler's inline budget and the per-row sweeps
// rely on that; adding the IN logic (or even a call to it) pushes them over
// and costs ~20% on the indexed hot path (measured). Call sites short-circuit
// on p.insN == 0.
//
// Every window's pool segment (pool[w.off:w.off+w.n]) is sorted ascending —
// plan() sorts it once, in place, right after appending it. Per-window
// matching (both the linear and binary-search paths) is split out into
// matchOneIn (below), leaving matchIns itself just a loop-and-call: matchIns
// is inlined at its own queryGroups/QueryAggs/QueryGroupBy call sites (a
// separate, smaller inlining decision from the matchBase/matchDelta one
// above), and this split is required to keep it inlinable, not just tidier.
// Measured with `go build -gcflags='-m -m'`:
//   - original (pre-binary-search) matchIns: cost 50, under the 80 budget.
//   - binary-search branch written inline in matchIns directly: cost 134.
//   - only the large-window (binary) branch extracted to its own
//     go:noinline function, small-window linear path left inline in
//     matchIns: cost 126 — still over budget. The real cause is
//     inlineExtraCallCost (cmd/compile/internal/inline/inl.go): any call to
//     a function the compiler won't also inline costs 57 against the
//     budget, REGARDLESS of the callee's own size, precisely to bias
//     against leaving real (non-inlined) calls in hot inlined code. matchIns
//     already cost 50 on its own before adding any call, so 50+57=107 alone
//     exceeds 80 — no amount of shrinking the surrounding branch logic can
//     claw back 27+ points once a real call is present in matchIns's body.
//   - both paths (linear AND binary) moved into one go:noinline matchOneIn,
//     matchIns reduced to a bare "for range ins { call; check }" loop: cost
//     79, back under budget — the single remaining call still costs its
//     fixed 57, but the surrounding loop skeleton is cheap enough to fit the
//     rest of the budget under it. (Getting here also meant passing w by
//     value and letting matchOneIn do its own off+n slicing internally,
//     rather than slicing pool in matchIns's own call expression — every
//     extra node in matchIns's body counts, right down to the last couple
//     of points.)
func matchIns(ins []inWindow, pool []uint32, dims [][]uint32, r int) bool {
	for _, w := range ins {
		if !matchOneIn(w, pool, dims[w.dim][r]) {
			return false
		}
	}
	return true
}

// matchOneIn checks id against one IN window (w.off/w.n index into pool,
// sorted ascending): a linear probe for windows no longer than inLinearMax (a
// branchy linear scan beats a binary search's pointer-chasing at this size
// on real hardware), a binary search above it (turning an O(n) per-row cost
// into O(log n)). Deliberately NOT inlined into matchIns — see the comment
// there for why (the fixed per-call inlining cost means matchIns can afford
// exactly one such call, not this logic — or even the off+n slice
// arithmetic — written out in place).
//
//go:noinline
func matchOneIn(w inWindow, pool []uint32, id uint32) bool {
	seg := pool[w.off : w.off+w.n]
	if len(seg) <= inLinearMax {
		for _, c := range seg {
			if id == c {
				return true
			}
		}
		return false
	}
	_, ok := slices.BinarySearch(seg, id)
	return ok
}

// matchRanges checks a plan's range conditions against one row's dim ids,
// resolving values through the view's combined id space. rngs is a
// freshly-sliced view built by the caller (qs.rWins[p.rngOff:p.rngOff+p.rngN]).
// Kept out of matchBase/matchDelta for the same inline-budget reason as
// matchIns.
func matchRanges(rngs []rangeWindow, v *view, dims [][]uint32, r int) bool {
	for _, w := range rngs {
		d := int(w.dim)
		id := dims[d][r]
		var val int64
		if n := uint32(v.base.dicts[d].len()); id >= n {
			val = v.extras[d].vals[id-n]
		} else {
			val = v.base.dicts[d].vals[id]
		}
		if val < w.min || val > w.max {
			return false
		}
	}
	return true
}

// matchBase checks equality conditions only; IN and range conditions are
// checked by the caller via matchIns/matchRanges (see their comments for why).
// qs supplies the condDims/condIDs pools p's condOff/condN index into.
func (p *groupPlan) matchBase(qs *queryScratch, v *view, r int) bool {
	if p.dead || !p.basePossible {
		return false
	}
	if !p.scan && (r < p.lo || r >= p.hi) {
		return false
	}
	for i := int32(0); i < p.condN; i++ {
		d := qs.condDims[p.condOff+i]
		if v.base.dims[d][r] != qs.condIDs[p.condOff+i] {
			return false
		}
	}
	return true
}

// matchDelta checks equality conditions only; IN and range conditions are
// checked by the caller via matchIns/matchRanges (see their comments for why).
func (p *groupPlan) matchDelta(qs *queryScratch, d *delta, r int) bool {
	if p.dead {
		return false
	}
	for i := int32(0); i < p.condN; i++ {
		dd := qs.condDims[p.condOff+i]
		if d.dims[dd][r] != qs.condIDs[p.condOff+i] {
			return false
		}
	}
	return true
}

// iv is one base candidate row interval [lo, hi).
type iv struct{ lo, hi int }

// ivInsertionSortMax is the interval count up to which planGroups sorts by
// insertion rather than slices.SortFunc: measured cheaper up here (Task 2),
// where SortFunc's generic pdqsort machinery and closure call overhead
// dominate. Above it the O(n^2) tail is the bigger risk, because IN index
// expansion makes the interval count unbounded by the group count.
const ivInsertionSortMax = 32

// planGroups plans every group against v and enforces the scan budget. Plans
// land in qs.plans and their base candidate intervals in qs.ivs, emitted by
// plan() itself (both pools sized/reset by the caller's routing before this is
// called — see the comment on scratchBack), so every query entry point
// (QueryGroups, QueryAggs, QueryGroupBy) shares one planner. Returns the
// interval count, sorted by lo for the union sweep.
func (s *Store) planGroups(v *view, groups [][]Cond, qs *queryScratch) (int, error) {
	if len(groups) == 0 {
		return 0, errBadGroupCount
	}
	sc := &s.sc
	qs.plans = qs.plans[:len(groups)]
	for gi := range groups {
		// Reserve one interval slot per group still unplanned, so IN
		// expansion here can never eat the room a later plan needs.
		if err := v.plan(sc, groups[gi], &qs.plans[gi], qs, len(groups)-gi-1); err != nil {
			return 0, err
		}
		if qs.plans[gi].scan {
			s.st.fullScans.Add(1)
		}
	}
	// Intervals arrive grouped by plan (a plan emits one per expanded IN
	// combination), so they are only locally ordered: sort by lo, which is
	// what the sweep's high-water dedup below and in every entry point needs.
	n := len(qs.ivs)
	if n <= ivInsertionSortMax {
		for i := 1; i < n; i++ {
			for j := i; j > 0 && qs.ivs[j].lo < qs.ivs[j-1].lo; j-- {
				qs.ivs[j], qs.ivs[j-1] = qs.ivs[j-1], qs.ivs[j]
			}
		}
	} else {
		slices.SortFunc(qs.ivs, func(a, b iv) int { return cmp.Compare(a.lo, b.lo) })
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
			lo, hi := qs.ivs[i].lo, qs.ivs[i].hi
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

// QueryGroups sums all metrics over rows matching ANY group. dst is reused
// when cap(dst) >= len(Metrics); the call performs zero heap allocations for
// query shapes that fit the stack scratch (see queryScratch), and larger
// shapes borrow a pooled scratch instead, amortized zero-alloc in steady
// state.
func (s *Store) QueryGroups(dst []float64, groups [][]Cond) ([]float64, error) {
	// Routing is inlined here rather than behind a shared helper: see the
	// comment on scratchBack for why (a shared version isn't inlinable and
	// costs a real function-call boundary on every query).
	sh := measureShape(groups, 0)
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
	out, err := s.queryGroups(dst, groups, qs)
	// release only the pooled pointer, never &local (equivalently, never qs
	// itself): calling release on a value that might be the stack-backed
	// local forces local (and back) onto the heap, because release's body
	// can reach sync.Pool.Put — see the CAUTION on queryScratch. release is
	// a no-op for stack scratches anyway, so skipping it here changes
	// nothing at runtime.
	if pooled != nil {
		pooled.release()
	}
	return out, err
}

func (s *Store) queryGroups(dst []float64, groups [][]Cond, qs *queryScratch) ([]float64, error) {
	v := s.view.Load()
	n, err := s.planGroups(v, groups, qs)
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
		lo, hi := qs.ivs[i].lo, qs.ivs[i].hi
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
			for gi := range qs.plans {
				p := &qs.plans[gi]
				if p.matchBase(qs, v, r) &&
					(p.insN == 0 || matchIns(qs.inWins[p.insOff:p.insOff+p.insN], qs.inPool, v.base.dims, r)) &&
					(p.rngN == 0 || matchRanges(qs.rWins[p.rngOff:p.rngOff+p.rngN], v, v.base.dims, r)) {
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
		for gi := range qs.plans {
			p := &qs.plans[gi]
			if p.matchDelta(qs, d, r) &&
				(p.insN == 0 || matchIns(qs.inWins[p.insOff:p.insOff+p.insN], qs.inPool, d.dims, r)) &&
				(p.rngN == 0 || matchRanges(qs.rWins[p.rngOff:p.rngOff+p.rngN], v, d.dims, r)) {
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
// performs zero heap allocations for query shapes that fit the stack scratch
// (see queryScratch), and larger shapes borrow a pooled scratch instead,
// amortized zero-alloc in steady state.
func (s *Store) Query(dst []float64, groups ...[]Cond) ([]float64, error) {
	return s.QueryGroups(dst, groups)
}
