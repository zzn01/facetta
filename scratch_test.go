// scratch_test.go
package facetta

import "testing"

func TestMeasureShapeAndFits(t *testing.T) {
	small := [][]Cond{{{Dim: "a", Value: "x"}, {Dim: "b", In: []string{"1", "2"}}}}
	sh := measureShape(small, 2)
	if sh.groups != 1 || sh.conds != 1 || sh.inConds != 1 || sh.inVals != 2 || sh.aggs != 2 {
		t.Fatalf("bad shape: %+v", sh)
	}
	if !sh.fits() {
		t.Fatal("small shape must fit the stack fast path")
	}
	big := make([][]Cond, fastGroups+1)
	for i := range big {
		big[i] = []Cond{{Dim: "a", Value: "x"}}
	}
	if measureShape(big, 0).fits() {
		t.Fatal("17 groups must not fit")
	}
	// inVals is counted from INPUT length (resolved <= input), so fits()
	// is deterministic w.r.t. table contents.
	manyVals := [][]Cond{{{Dim: "a", In: make([]string, fastInIDs+1)}}}
	if measureShape(manyVals, 0).fits() {
		t.Fatal("129 input IN values must not fit")
	}
}

// TestScratchFastAndPooled exercises the routing shape every query entry
// point uses (measureShape + fits() + scratchBack.fast() or
// getPooledScratch): see the comment on scratchBack for why there is no
// single shared "pick" method to call instead.
func TestScratchFastAndPooled(t *testing.T) {
	small := [][]Cond{{{Dim: "a", Value: "x"}}}
	var b scratchBack
	qs := &queryScratch{}
	if measureShape(small, 1).fits() {
		local := b.fast()
		qs = &local
	} else {
		t.Fatal("small shape must fit the stack fast path")
	}
	if qs.pooled {
		t.Fatal("small shape must use stack backing")
	}
	if cap(qs.condDims) != fastConds || cap(qs.inPool) != fastInIDs {
		t.Fatal("stack backing not wired to fast arrays")
	}
	qs.release() // must be a no-op for stack backing

	big := make([][]Cond, fastGroups+1)
	for i := range big {
		big[i] = []Cond{{Dim: "a", Value: "x"}}
	}
	shBig := measureShape(big, 0)
	if shBig.fits() {
		t.Fatal("17 groups must not fit the stack fast path")
	}
	qp := getPooledScratch(shBig)
	if !qp.pooled {
		t.Fatal("oversized shape must use pooled scratch")
	}
	if cap(qp.plans) < len(big) {
		t.Fatal("pooled scratch not pre-sized for the shape")
	}
	// inPool must be pre-sized exactly: later appends must NEVER reallocate,
	// because inWindow.off/n index into it.
	shaped := [][]Cond{{{Dim: "a", In: make([]string, 500)}}}
	shShaped := measureShape(shaped, 0)
	qi := getPooledScratch(shShaped)
	if cap(qi.inPool) < 500 {
		t.Fatal("pooled inPool must be pre-sized to input IN value count")
	}
	qp.release()
	qi.release()
}

func TestScratchPoolRetention(t *testing.T) {
	qs := &queryScratch{pooled: true}
	qs.inPool = make([]uint32, 0, (maxPooledScratchBytes/4)+1) // >1MB alone
	if qs.retainedBytes() <= maxPooledScratchBytes {
		t.Fatal("retainedBytes must count slice capacities")
	}
	// release() of an oversized scratch must drop it: we can only test the
	// predicate directly (sync.Pool contents are not observable).
	if !qs.oversized() {
		t.Fatal("oversized() must be true above maxPooledScratchBytes")
	}
}
