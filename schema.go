// Package facetta provides an embedded, read-optimized, in-memory aggregation
// table: dictionary-encoded columnar snapshots with an atomic-swap delta
// overlay, synced from an external source of truth.
package facetta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"time"
)

const (
	maxDims   = 16
	maxGroups = 16
	maxConds  = 16
	maxInVals = 16
)

var (
	// ErrRowLimit is returned when a compaction would exceed Config.MaxRows;
	// the store keeps serving the previous snapshot.
	ErrRowLimit = errors.New("facetta: row limit exceeded, compaction refused")
	// ErrKeyOverflow is returned when the packed index key for the leading
	// IndexDims dimensions no longer fits in 64 bits.
	ErrKeyOverflow = errors.New("facetta: index dimensions exceed 64-bit packed key")
	// ErrScanBudget is returned when a query's known scan work (deduped base
	// candidate rows + delta rows) exceeds Config.MaxScanRows. It is returned
	// bare (never wrapped) so the refusal path stays allocation-free; the row
	// counts and budget are observable via Stats().ScanBudgetRefusals and the
	// caller's own config. It means "refused fast, fall back".
	ErrScanBudget = errors.New("facetta: query exceeds scan budget")
)

// Schema defines the table shape. The first IndexDims dimensions form the
// sorted index prefix used for fast equality filtering; row identity is the
// combination of ALL dimensions. Choose selective leading dims: both query
// planning and upsert row matching narrow by the packed index prefix, and a
// low-cardinality prefix over a high-cardinality table degrades them toward
// linear scans.
// DimType selects a dimension's value semantics.
type DimType uint8

const (
	// DimString treats values as opaque labels; equality and IN only.
	// The zero value, so plain Dim{Name: "..."} declares a string dim.
	DimString DimType = iota
	// DimNumeric declares values to be numbers (encode times as unix
	// timestamps), enabling Cond.Range. On these dims IDENTITY IS THE
	// NUMBER: every value is canonicalized at the write and query
	// boundaries ("1.0"/"01"/"1e0" are one row, one dictionary entry, one
	// group-by key "1"; integers exact up to 2^53). Non-numeric or
	// non-finite values are rejected — Apply/ReplaceAll and conditions
	// error instead of silently mismatching. The snapshot format and the
	// schema fingerprint are unaffected by the type; snapshots holding
	// non-canonical values for a numeric dim are refused at load
	// (full-pull fallback).
	DimNumeric
)

// Dim declares one dimension: its name and value semantics. Future per-dim
// attributes are added here, so the declaration grows without breaking.
type Dim struct {
	Name string
	Type DimType
}

type Schema struct {
	Dims      []Dim
	IndexDims int
	Metrics   []string // metric names; values are float64 in Record.Metrics
}

func (s *Schema) validate() error {
	if len(s.Dims) == 0 || len(s.Dims) > maxDims {
		return fmt.Errorf("facetta: need 1..%d dimensions, got %d", maxDims, len(s.Dims))
	}
	if s.IndexDims < 1 || s.IndexDims > len(s.Dims) {
		return fmt.Errorf("facetta: IndexDims must be in 1..%d, got %d", len(s.Dims), s.IndexDims)
	}
	if len(s.Metrics) == 0 {
		return errors.New("facetta: need at least one metric")
	}
	seen := map[string]bool{}
	for _, d := range s.Dims {
		if d.Name == "" || seen[d.Name] {
			return fmt.Errorf("facetta: empty or duplicate dimension %q", d.Name)
		}
		if d.Type > DimNumeric {
			return fmt.Errorf("facetta: dimension %q has unknown type %d", d.Name, d.Type)
		}
		seen[d.Name] = true
	}
	seenM := map[string]bool{}
	for _, m := range s.Metrics {
		if m == "" || seenM[m] {
			return fmt.Errorf("facetta: empty or duplicate metric %q", m)
		}
		seenM[m] = true
	}
	return nil
}

// fingerprint identifies the schema in persisted snapshot headers.
func (s *Schema) fingerprint() uint64 {
	h := fnv.New64a()
	var n [8]byte
	binary.LittleEndian.PutUint64(n[:], uint64(s.IndexDims))
	h.Write(n[:])
	for _, d := range s.Dims {
		h.Write([]byte(d.Name)) // names only: DimType is a capability, not data shape
		h.Write([]byte{0})
	}
	h.Write([]byte{1})
	for _, m := range s.Metrics {
		h.Write([]byte(m))
		h.Write([]byte{0})
	}
	return h.Sum64()
}

// canonNum returns the canonical spelling of a numeric dim value: the
// shortest round-trip float64 formatting ("1.0"/"01"/"1e0" -> "1"). This IS
// the identity form for numeric dims — every write and query boundary
// rewrites values through it, so equality, IN, Range, dedup and group-by
// keys all agree on the number, not the spelling. Non-finite and
// unparseable values are rejected (ok == false).
func canonNum(s string) (string, bool) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return "", false
	}
	return strconv.FormatFloat(v, 'g', -1, 64), true
}

// isNumeric reports whether dim i was declared DimNumeric.
func (s *Schema) isNumeric(i int) bool {
	return s.Dims[i].Type == DimNumeric
}

func (s *Schema) dimIndex(name string) int {
	for i, d := range s.Dims {
		if d.Name == name {
			return i
		}
	}
	return -1
}

// Record is one upsert row from the source of truth.
type Record struct {
	Dims      []string  // len == len(Schema.Dims)
	Metrics   []float64 // len == len(Schema.Metrics)
	UpdatedAt time.Time
	// ExpireAt is the absolute per-row expiry. Zero means never expires.
	// A row is invisible to queries once now >= ExpireAt (read-time skip),
	// and is physically reclaimed on the next Compact/ReplaceAll. Writing a
	// record with ExpireAt in the past acts as a tombstone for its tuple.
	ExpireAt time.Time
}

// Config holds store-level self-protection settings.
type Config struct {
	// TTL drops rows whose UpdatedAt is older than now-TTL at compaction time.
	// Zero disables TTL eviction.
	TTL time.Duration
	// MaxRows refuses compactions that would exceed this row count.
	// Zero disables the limit.
	MaxRows int
	// MaxScanRows refuses a query whose known scan work (deduped base candidate
	// rows + delta rows, computed after planning and before any row is touched)
	// exceeds this count, returning ErrScanBudget. It converts the worst-case
	// O(N) full scan into a fail-fast refusal for deadline-sensitive callers.
	// Zero disables the budget (no behavior change).
	MaxScanRows int
	// DictCompactInterval rate-limits dictionary compaction (reclaiming
	// garbage entries, renumbering ids, shrinking packed-key widths) to at
	// most one merge per interval; merges inside the window keep ids stable
	// and skip the mark/remap passes, which cost ~40-60% of merge wall time.
	// Zero compacts dictionaries on every merge. A merge refused with
	// ErrKeyOverflow retries once with dictionary compaction regardless of
	// the gate, so overflow recovery is never delayed.
	DictCompactInterval time.Duration
}
