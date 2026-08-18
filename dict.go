package facetta

import (
	"maps"
	"slices"
	"strconv"
)

// dict maps dimension values to dense uint32 ids. Immutable once published
// in a view; writers clone before extending. Two variants share the struct:
//
//   - String dims key by spelling and keep the strings (they are the
//     interchange format: persistence, group-by keys alias them).
//   - Integer dims (DimInt) key by the int64 VALUE and store no strings at
//     all — identity is the integer, enforced by the map key itself, so the
//     write and query paths never render a spelling. Spellings exist only at
//     output boundaries (group-by keys, snapshot save), formatted on demand
//     by str().
//
// A range check at query time reads vals directly: two integer comparisons
// per row, parsing confined to the boundaries.
type dict struct {
	numeric bool
	// string dims
	ids  map[string]uint32
	strs []string
	// integer dims
	idsN map[int64]uint32
	vals []int64 // [id] value; doubles as the range-check column
}

func newDict(numeric bool) *dict {
	if numeric {
		return &dict{numeric: true, idsN: map[int64]uint32{}}
	}
	return &dict{ids: map[string]uint32{}}
}

// getOrAdd interns a string-dim value. String dicts only.
func (d *dict) getOrAdd(s string) uint32 {
	if id, ok := d.ids[s]; ok {
		return id
	}
	id := uint32(len(d.strs))
	d.ids[s] = id
	d.strs = append(d.strs, s)
	return id
}

// getOrAddN interns an integer-dim value. Integer dicts only.
func (d *dict) getOrAddN(v int64) uint32 {
	if id, ok := d.idsN[v]; ok {
		return id
	}
	id := uint32(len(d.vals))
	d.idsN[v] = id
	d.vals = append(d.vals, v)
	return id
}

// addFrom copies src's entry id into d (both dicts of the same variant),
// returning its id in d. Lets merge and dictionary compaction stay
// variant-agnostic.
func (d *dict) addFrom(src *dict, id uint32) uint32 {
	if d.numeric {
		return d.getOrAddN(src.vals[id])
	}
	return d.getOrAdd(src.strs[id])
}

func (d *dict) lookup(s string) (uint32, bool) {
	id, ok := d.ids[s]
	return id, ok
}

func (d *dict) lookupN(v int64) (uint32, bool) {
	id, ok := d.idsN[v]
	return id, ok
}

// str renders entry id's spelling: aliased for string dims, formatted on
// demand for integer dims (allocates — output boundaries only).
func (d *dict) str(id uint32) string {
	if d.numeric {
		return strconv.FormatInt(d.vals[id], 10)
	}
	return d.strs[id]
}

func (d *dict) len() int {
	if d.numeric {
		return len(d.vals)
	}
	return len(d.strs)
}

func (d *dict) clone() *dict {
	return &dict{
		numeric: d.numeric,
		ids:     maps.Clone(d.ids), strs: slices.Clone(d.strs),
		idsN: maps.Clone(d.idsN), vals: slices.Clone(d.vals),
	}
}
