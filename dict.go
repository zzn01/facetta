package facetta

import (
	"maps"
	"math"
	"slices"
	"strconv"
)

// dict maps dimension strings to dense uint32 ids. Immutable once published
// in a view; writers clone before extending. Dictionaries of dims declared with
// DimNumeric type hold canonical numeric spellings only (enforced at the
// write/load boundaries) and carry vals, a parallel array of each entry's
// parsed float64, so range conditions cost two float comparisons per row at
// query time: parsing happens once per dictionary entry, on insertion, never
// on the query path. (The NaN fallback below is defensive; canonical entries
// always parse.)
type dict struct {
	ids     map[string]uint32
	strs    []string
	numeric bool
	vals    []float64 // [id] parsed value; only when numeric
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
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			v = math.NaN() // unparseable: matches no range, still matchable by equality
		}
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
