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
