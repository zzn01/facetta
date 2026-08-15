package facetta

import (
	"maps"
	"slices"
)

// dict maps dimension strings to dense uint32 ids. Immutable once published
// in a view; writers clone before extending.
type dict struct {
	ids  map[string]uint32
	strs []string
}

func newDict() *dict {
	return &dict{ids: map[string]uint32{}}
}

func (d *dict) getOrAdd(s string) uint32 {
	if id, ok := d.ids[s]; ok {
		return id
	}
	id := uint32(len(d.strs))
	d.ids[s] = id
	d.strs = append(d.strs, s)
	return id
}

func (d *dict) lookup(s string) (uint32, bool) {
	id, ok := d.ids[s]
	return id, ok
}

func (d *dict) len() int { return len(d.strs) }

func (d *dict) clone() *dict {
	return &dict{ids: maps.Clone(d.ids), strs: slices.Clone(d.strs)}
}
