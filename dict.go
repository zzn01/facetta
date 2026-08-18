package facetta

import (
	"maps"
	"slices"
	"strconv"
)

// dict maps dimension strings to dense uint32 ids. Immutable once published
// in a view; writers clone before extending. Dictionaries of dims declared
// DimInt hold canonical integer spellings only (enforced at the write/load
// boundaries) and carry vals, a parallel array of each entry's parsed int64,
// so range conditions cost two integer comparisons per row at query time:
// parsing happens once per dictionary entry, on insertion, never on the
// query path.
type dict struct {
	ids     map[string]uint32
	strs    []string
	numeric bool
	vals    []int64 // [id] parsed value; only when numeric
}

func newDict(numeric bool) *dict {
	return &dict{ids: map[string]uint32{}, numeric: numeric}
}

func (d *dict) getOrAdd(s string) uint32 {
	if id, ok := d.ids[s]; ok {
		return id
	}
	id := uint32(len(d.strs))
	d.ids[s] = id
	d.strs = append(d.strs, s)
	if d.numeric {
		// canonical entries always parse; err is unreachable by construction
		v, _ := strconv.ParseInt(s, 10, 64)
		d.vals = append(d.vals, v)
	}
	return id
}

func (d *dict) lookup(s string) (uint32, bool) {
	id, ok := d.ids[s]
	return id, ok
}

// lookupB looks up a key rendered into a byte buffer without allocating
// (the string conversion inside a map index is free).
func (d *dict) lookupB(b []byte) (uint32, bool) {
	id, ok := d.ids[string(b)]
	return id, ok
}

func (d *dict) len() int { return len(d.strs) }

func (d *dict) clone() *dict {
	return &dict{ids: maps.Clone(d.ids), strs: slices.Clone(d.strs), numeric: d.numeric, vals: slices.Clone(d.vals)}
}
