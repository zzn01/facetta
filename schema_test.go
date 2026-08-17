package facetta

import (
	"testing"
	"time"
)

func testSchema() Schema {
	return Schema{
		Dims: []Dim{
			{Name: "source"}, {Name: "account"}, {Name: "publisher"},
			{Name: "country"}, {Name: "os"},
		},
		IndexDims: 3,
		Metrics:   []string{"visits", "revenue"},
	}
}

func TestSchemaValidate(t *testing.T) {
	sc := testSchema()
	if err := sc.validate(); err != nil {
		t.Fatalf("valid schema rejected: %v", err)
	}
	bad := []Schema{
		{Dims: nil, IndexDims: 0, Metrics: []string{"m"}},
		{Dims: []Dim{{Name: "a"}}, IndexDims: 0, Metrics: []string{"m"}},
		{Dims: []Dim{{Name: "a"}}, IndexDims: 2, Metrics: []string{"m"}},
		{Dims: []Dim{{Name: "a"}, {Name: "a"}}, IndexDims: 1, Metrics: []string{"m"}},
		{Dims: []Dim{{Name: ""}}, IndexDims: 1, Metrics: []string{"m"}},
		{Dims: []Dim{{Name: "a"}}, IndexDims: 1, Metrics: nil},
		{Dims: []Dim{{Name: "a"}}, IndexDims: 1, Metrics: []string{"m", "m"}},
		{Dims: []Dim{{Name: "a", Type: 99}}, IndexDims: 1, Metrics: []string{"m"}},
	}
	for i, s := range bad {
		if err := s.validate(); err == nil {
			t.Errorf("case %d: invalid schema accepted", i)
		}
	}
}

func TestSchemaFingerprint(t *testing.T) {
	a, b := testSchema(), testSchema()
	if a.fingerprint() != b.fingerprint() {
		t.Fatal("identical schemas must have equal fingerprints")
	}
	b.Metrics = []string{"visits"}
	if a.fingerprint() == b.fingerprint() {
		t.Fatal("different schemas must have different fingerprints")
	}
	c := testSchema()
	c.IndexDims = 2
	if a.fingerprint() == c.fingerprint() {
		t.Fatal("IndexDims must affect fingerprint")
	}
}

func TestDimIndex(t *testing.T) {
	sc := testSchema()
	if got := sc.dimIndex("country"); got != 3 {
		t.Fatalf("dimIndex(country) = %d, want 3", got)
	}
	if got := sc.dimIndex("nope"); got != -1 {
		t.Fatalf("dimIndex(nope) = %d, want -1", got)
	}
}

func rec(updated time.Time, metrics []float64, dims ...string) Record {
	return Record{Dims: dims, Metrics: metrics, UpdatedAt: updated}
}
