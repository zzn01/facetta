package facetta

import (
	"sync"
	"unsafe"
)

// Stack fast-path capacities: a query whose shape fits ALL of these runs on
// a stack-backed scratch with zero heap allocations (invariant guarded by
// TestQueryZeroAlloc). Anything larger borrows a pooled scratch.
const (
	fastGroups = 16 // OR groups
	fastConds  = 32 // total equality conditions across all groups
	// fastIvs == fastGroups: planGroups emits at most one interval per plan
	// and len(qs.plans) <= fastGroups in this task's scope (no per-group
	// interval expansion yet). Task 5's IN-index expansion will need to
	// revisit this — the pooled side already sizes ivs by shape
	// (getPooledScratch), so only this stack-fast bound is at risk.
	fastIvs     = fastGroups
	fastInConds = 16  // total IN conditions
	fastInIDs   = 128 // total IN input values
	fastRanges  = 16  // total range conditions
	fastAggs    = 16  // aggregate output columns

	// A pooled scratch whose retained capacity exceeds this is dropped on
	// release instead of returned to the pool, so one pathological query
	// cannot pin a huge block for every later query.
	maxPooledScratchBytes = 1 << 20
)

// inWindow is one resolved IN condition: candidate dictionary ids live at
// queryScratch.inPool[off : off+n] (which therefore must never reallocate
// mid-plan), sorted ascending by plan() right after the segment is filled —
// matchIns binary-searches windows above its linear-scan threshold, and this
// is also the precondition for IN index expansion. off/n are a pool
// offset+count rather than a materialized []uint32 sub-slice deliberately:
// see the CAUTION on queryScratch for why.
type inWindow struct {
	dim    int32
	off, n int32
}

// rangeWindow is one resolved range condition.
type rangeWindow struct {
	dim      int32
	min, max int64
}

// queryShape is the O(input) measure of a query used to route it to the
// stack or pooled scratch. inVals counts INPUT values (resolved <= input),
// so routing is deterministic regardless of table contents.
type queryShape struct {
	groups, conds, inConds, inVals, ranges, aggs int
}

func measureShape(groups [][]Cond, nAggs int) queryShape {
	sh := queryShape{groups: len(groups), aggs: nAggs}
	for _, g := range groups {
		for _, c := range g {
			switch {
			case c.Range != nil:
				sh.ranges++
			case len(c.In) > 0:
				sh.inConds++
				sh.inVals += len(c.In)
			default:
				sh.conds++
			}
		}
	}
	return sh
}

func (sh queryShape) fits() bool {
	return sh.groups <= fastGroups && sh.conds <= fastConds &&
		sh.inConds <= fastInConds && sh.inVals <= fastInIDs &&
		sh.ranges <= fastRanges && sh.aggs <= fastAggs
}

// queryScratch is the per-query workspace shared by planner, matcher and
// sweep. All fields are slices; the backing storage is either a caller's
// stack scratchBack (small queries) or pooled heap slices (large queries).
// Pool slices are length-managed and written before read: there is no
// defensive zeroing anywhere.
//
// CAUTION (load-bearing, verified empirically — do not "simplify" this away):
// groupPlan.condOff/condN, insOff/insN and rngOff/rngN are offset+count pairs
// into condDims/condIDs, inWins and rWins below, NOT materialized sub-slices,
// and inWindow.off/n is likewise an offset+count into inPool, not a []uint32
// field. This is required for TestQueryZeroAlloc, not just style: groupPlan
// lives in qs.plans, an indexed slice/array. Writing a SLICE-typed value
// (a pointer) into an indexed element's field, via any function that takes
// *queryScratch as a parameter, makes the Go compiler's escape analysis mark
// that parameter's content as escaping to the heap — UNCONDITIONALLY, with
// no way to prove otherwise, REGARDLESS of inlining, of using an array
// instead of a slice for qs.plans, of splitting the write into its own
// tiny function, or of the value being nothing but plain re-sliced pool
// data. That heap mark propagates back through scratchBack.fast to the
// caller's own stack-declared scratchBack, forcing it (and hence the whole
// per-query workspace) onto the heap on every call — exactly the allocation
// TestQueryZeroAlloc exists to catch. Storing a plain offset+count (int32,
// no pointer) into the same indexed element does NOT trigger this: only
// pointer-shaped values do. Concretely this means:
//   - plan() computes each group's condOff/condN etc. by indexing the pools
//     directly (qs.condDims[p.condOff+i]), never by slicing them into a
//     []int32/[]inWindow/[]rangeWindow field of groupPlan.
//   - matchIns/matchRanges still take slice parameters ([]inWindow,
//     []rangeWindow) — that's fine, since a freshly-sliced value passed as a
//     function ARGUMENT (never stored into an indexed element) doesn't
//     trigger the same escape. Callers build that argument at the call site:
//     qs.inWins[p.insOff : p.insOff+p.insN].
//   - matchIns additionally takes the inPool slice, to resolve each
//     inWindow's off/n into actual candidate ids at match time.
//   - matchBase/matchDelta take qs (or at least its condDims/condIDs pools)
//     as a parameter for the same reason: they no longer have a materialized
//     sub-slice to range over.
//
// Appends to condDims/condIDs/inWins/inPool/rWins are written as manual
// "reslice then set" (`s = s[:len(s)+1]; s[len(s)-1] = v`) rather than
// `append`, again for TestQueryZeroAlloc: appending to a slice-typed FIELD
// through a *queryScratch parameter has the same "always escapes" effect as
// above, regardless of whether the append could ever actually reallocate.
// The manual form is the one shape the compiler recognizes as a provably
// non-escaping self-assignment. Every pool involved is pre-sized exactly by
// the pooled path (see getPooledScratch) and bounded by the stack path's
// queryShape.fits(), so these appends-in-fact never need to grow; do not
// add capacity checks or defensive copying here.
type queryScratch struct {
	pooled bool

	plans []groupPlan
	ivs   []iv

	condDims []int32 // equality-condition pools; plans hold offset+count views
	condIDs  []uint32
	inWins   []inWindow
	inPool   []uint32 // indexed by inWindow.off/n: pre-sized, never grown
	rWins    []rangeWindow

	mets  []int // aggregate scratch (QueryAggs / QueryGroupBy)
	ddims []int
	acc   []float64
	bms   [][]uint64
}

// scratchBack is the stack backing for small queries: just the arrays, with
// NO embedded queryScratch field. Declared as a local in each query entry
// point, which routes to it or to the pool ITSELF (there is deliberately no
// single "pick" method — see below).
//
// fast() builds the stack-fast queryScratch value BY VALUE, not as a
// *queryScratch: a function that returns a pointer aliasing ITS RECEIVER's
// arrays forces the receiver onto the heap even when that pointer never
// escapes the caller — confirmed empirically, and not fixable by inlining,
// by using an array instead of a slice, or by any restructuring that still
// ends in "return &something-derived-from-b". Returning the value lets the
// caller copy it into its OWN local and take that local's address itself,
// in its own stack frame, where escape analysis can actually see the
// address never leaves.
//
// Routing (measureShape + fits + calling fast() or getPooledScratch) lives
// at each of the three call sites instead of behind one shared "pick"
// method for a second, purely-a-perf reason: a shared pick() big enough to
// both measure the shape AND build the value comes out well over the
// compiler's inline budget (measured cost ~230 vs the ~80 budget), so it's
// never inlined — every call pays a real function-call boundary (measured
// ~90ns) on top of the ~50ns the work actually costs. fast() alone (just
// the value construction) fits under budget and inlines cleanly, so calling
// it directly from each entry point removes that boundary. Callers must use
// exactly this shape:
//
//	sh := measureShape(groups, nAggs)
//	var qs *queryScratch
//	var back scratchBack
//	var local queryScratch
//	if sh.fits() {
//		local = back.fast()
//		qs = &local
//	} else {
//		qs = getPooledScratch(sh)
//	}
//	... use qs ...
//	if qs.pooled {
//		qs.release() // never call release on &local, see QueryGroups
//	}
type scratchBack struct {
	plans    [fastGroups]groupPlan
	ivs      [fastIvs]iv
	condDims [fastConds]int32
	condIDs  [fastConds]uint32
	inWins   [fastInConds]inWindow
	inPool   [fastInIDs]uint32
	rWins    [fastRanges]rangeWindow
	mets     [fastAggs]int
	ddims    [fastAggs]int
	acc      [fastAggs]float64
	bms      [fastAggs][]uint64
}

// fast builds the stack-backed queryScratch value for a shape that already
// passed queryShape.fits(). Kept to a single composite-literal return (no
// separate field-assignment statements) specifically to stay under the
// inliner's cost budget — see the comment on scratchBack.
func (b *scratchBack) fast() queryScratch {
	return queryScratch{
		plans:    b.plans[:0],
		ivs:      b.ivs[:0],
		condDims: b.condDims[:0],
		condIDs:  b.condIDs[:0],
		inWins:   b.inWins[:0],
		inPool:   b.inPool[:0],
		rWins:    b.rWins[:0],
		mets:     b.mets[:0],
		ddims:    b.ddims[:0],
		acc:      b.acc[:0],
		bms:      b.bms[:0],
	}
}

var scratchPool = sync.Pool{New: func() any { return &queryScratch{pooled: true} }}

func getPooledScratch(sh queryShape) *queryScratch {
	q := scratchPool.Get().(*queryScratch)
	q.plans = grow(q.plans, sh.groups)
	// One base candidate interval per group, at most (no per-group interval
	// expansion until Task 5's IN-index expansion lands, at which point this
	// sizing needs revisiting alongside the offset+count scheme above).
	q.ivs = grow(q.ivs, sh.groups)
	q.condDims = grow(q.condDims, sh.conds)
	q.condIDs = grow(q.condIDs, sh.conds)
	q.inWins = grow(q.inWins, sh.inConds)
	q.inPool = grow(q.inPool, sh.inVals) // exact: appends never reallocate
	q.rWins = grow(q.rWins, sh.ranges)
	q.mets = grow(q.mets, sh.aggs)
	q.ddims = grow(q.ddims, sh.aggs)
	q.acc = grow(q.acc, sh.aggs)
	q.bms = grow(q.bms, sh.aggs)
	return q
}

// grow returns s with length 0 and capacity >= n.
func grow[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, 0, n)
	}
	return s[:0]
}

func (q *queryScratch) retainedBytes() int {
	return cap(q.plans)*int(unsafe.Sizeof(groupPlan{})) +
		cap(q.ivs)*int(unsafe.Sizeof(iv{})) +
		cap(q.condDims)*4 + cap(q.condIDs)*4 +
		cap(q.inWins)*int(unsafe.Sizeof(inWindow{})) +
		cap(q.inPool)*4 +
		cap(q.rWins)*int(unsafe.Sizeof(rangeWindow{})) +
		cap(q.mets)*8 + cap(q.ddims)*8 + cap(q.acc)*8 +
		cap(q.bms)*int(unsafe.Sizeof([]uint64{}))
}

func (q *queryScratch) oversized() bool { return q.retainedBytes() > maxPooledScratchBytes }

// release returns a pooled scratch for reuse; stack-backed scratches and
// oversized pooled ones (retention guard) are simply dropped. bms entries are
// nilled first so a pooled scratch never pins the last query's distinct-count
// bitmaps (they'd otherwise count toward retainedBytes forever without ever
// tripping the retention guard, since bitmap backing arrays aren't part of
// this struct's own slice capacities).
func (q *queryScratch) release() {
	if !q.pooled || q.oversized() {
		return
	}
	for i := range q.bms {
		q.bms[i] = nil
	}
	scratchPool.Put(q)
}
