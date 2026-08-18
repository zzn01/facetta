package facetta

import "testing"

func TestDict(t *testing.T) {
	d := newDict(false)
	if d.len() != 0 {
		t.Fatal("new dict not empty")
	}
	a := d.getOrAdd("US")
	b := d.getOrAdd("DE")
	if a != 0 || b != 1 {
		t.Fatalf("ids = %d,%d, want 0,1", a, b)
	}
	if d.getOrAdd("US") != a {
		t.Fatal("getOrAdd not idempotent")
	}
	if id, ok := d.lookup("DE"); !ok || id != 1 {
		t.Fatalf("lookup(DE) = %d,%v", id, ok)
	}
	if _, ok := d.lookup("FR"); ok {
		t.Fatal("lookup of absent string succeeded")
	}
	if d.strs[0] != "US" || d.strs[1] != "DE" {
		t.Fatal("reverse mapping broken")
	}
	c := d.clone()
	c.getOrAdd("FR")
	if d.len() != 2 || c.len() != 3 {
		t.Fatal("clone must not share state")
	}
}

func TestIntDict(t *testing.T) {
	d := newDict(true)
	if id := d.getOrAddN(42); id != 0 {
		t.Fatalf("first id = %d, want 0", id)
	}
	if id := d.getOrAddN(-7); id != 1 {
		t.Fatalf("second id = %d, want 1", id)
	}
	if id := d.getOrAddN(42); id != 0 {
		t.Fatalf("re-add id = %d, want 0", id)
	}
	if id, ok := d.lookupN(-7); !ok || id != 1 {
		t.Fatalf("lookupN(-7) = %d,%v", id, ok)
	}
	if _, ok := d.lookupN(99); ok {
		t.Fatal("lookupN(99) found")
	}
	if d.len() != 2 {
		t.Fatalf("len = %d, want 2", d.len())
	}
	// spellings are rendered, not stored
	if s := d.str(0); s != "42" {
		t.Fatalf("str(0) = %q", s)
	}
	if s := d.str(1); s != "-7" {
		t.Fatalf("str(1) = %q", s)
	}
	if len(d.strs) != 0 || d.ids != nil {
		t.Fatal("integer dict must not hold string storage")
	}
	// clone and addFrom preserve the variant
	c := d.clone()
	c.getOrAddN(8)
	if d.len() != 2 || c.len() != 3 {
		t.Fatalf("clone isolation broken: %d/%d", d.len(), c.len())
	}
	e := newDict(true)
	if id := e.addFrom(c, 2); id != 0 || e.str(0) != "8" {
		t.Fatalf("addFrom = %d, %q", id, e.str(0))
	}
}
