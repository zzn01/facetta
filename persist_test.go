package facetta

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")
	recs := baseRecords()
	s := seededStore(t, recs)
	if err := s.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	s2, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.LoadSnapshot(path); err != nil {
		t.Fatal(err)
	}
	if s2.Rows() != s.Rows() {
		t.Fatalf("rows = %d, want %d", s2.Rows(), s.Rows())
	}
	if !s2.SyncPosition().Equal(s.SyncPosition()) {
		t.Fatalf("position = %v, want %v", s2.SyncPosition(), s.SyncPosition())
	}
	rt := newRefTable(testSchema())
	rt.apply(recs)
	for _, groups := range [][][]Cond{
		{{{Dim: "source", Value: "s1"}}},
		{{{Dim: "os", Value: "ios"}}},
		{{}},
	} {
		got, err := s2.QueryGroups(nil, groups)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, got, rt.query(groups))
	}
}

// TestSnapshotExpireRoundtrip covers acceptance §4 (persist path preserves the
// expire column): a future-expiring row survives save/load and stays queryable,
// with store/oracle agreement after load.
func TestSnapshotExpireRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")
	now := time.Unix(10000, 0).UTC()
	future := now.Add(time.Hour)
	recs := []Record{
		rec(now, []float64{10, 1}, "s1", "a1", "p1", "US", "ios"),
		recExp(now, future, []float64{20, 2}, "s2", "a2", "p2", "DE", "web"),
	}
	s, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return now }
	if err := s.seed(recs); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	s2, err := New(testSchema(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	s2.now = func() time.Time { return now }
	if err := s2.LoadSnapshot(path); err != nil {
		t.Fatal(err)
	}
	rt := newRefTable(testSchema())
	rt.now = func() time.Time { return now }
	rt.replaceAll(recs)
	for _, g := range [][][]Cond{
		{{}},
		{{{Dim: "source", Value: "s2"}}},
	} {
		got, err := s2.QueryGroups(nil, g)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, got, rt.query(g))
	}
	// the future row must physically survive the load (expire column intact).
	if s2.Rows() != 2 {
		t.Fatalf("rows after load = %d, want 2", s2.Rows())
	}
	// minExpire must be recomputed on load so idle-reclaim still triggers.
	if v := s2.view.Load().base.minExpire; v != future.UnixMilli() {
		t.Fatalf("minExpire after load = %d, want %d", v, future.UnixMilli())
	}
}

// TestSnapshotV1Rejected covers acceptance §4: a v1-format file is rejected so
// the caller falls back to a full pull. We craft a v1 header by patching the
// version to 1 and recomputing the CRC over the mutated body.
func TestSnapshotV1Rejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")
	s := seededStore(t, baseRecords())
	if err := s.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := data[:len(data)-4]
	binary.LittleEndian.PutUint32(body[8:12], 1) // downgrade version field to v1
	binary.LittleEndian.PutUint32(data[len(data)-4:], crc32.Checksum(body, castagnoli))
	p2 := filepath.Join(dir, "v1.bin")
	if err := os.WriteFile(p2, data, 0o644); err != nil {
		t.Fatal(err)
	}
	s2, _ := New(testSchema(), Config{})
	if err := s2.LoadSnapshot(p2); err == nil {
		t.Fatal("v1 snapshot accepted; want rejection so caller falls back")
	}
}

func TestSnapshotLoadFailures(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")
	s := seededStore(t, baseRecords())
	if err := s.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}

	load := func(t *testing.T, sc Schema, cfg Config, mutate func([]byte) []byte) error {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if mutate != nil {
			data = mutate(data)
		}
		p2 := filepath.Join(t.TempDir(), "m.bin")
		if err := os.WriteFile(p2, data, 0o644); err != nil {
			t.Fatal(err)
		}
		s2, err := New(sc, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return s2.LoadSnapshot(p2)
	}

	t.Run("missing file", func(t *testing.T) {
		s2, _ := New(testSchema(), Config{})
		if err := s2.LoadSnapshot(filepath.Join(dir, "absent.bin")); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("corrupt byte", func(t *testing.T) {
		if err := load(t, testSchema(), Config{}, func(b []byte) []byte {
			b[len(b)/2] ^= 0xFF
			return b
		}); err == nil {
			t.Fatal("corrupt snapshot accepted")
		}
	})
	t.Run("truncated", func(t *testing.T) {
		if err := load(t, testSchema(), Config{}, func(b []byte) []byte {
			return b[:len(b)/2]
		}); err == nil {
			t.Fatal("truncated snapshot accepted")
		}
	})
	t.Run("version mismatch", func(t *testing.T) {
		if err := load(t, testSchema(), Config{}, func(b []byte) []byte {
			// Patch the version field (uint32 LE right after the 8-byte magic)
			// AND recompute the trailing CRC over the new body, so the load
			// reaches the version check rather than failing on CRC first.
			body := b[:len(b)-4]
			binary.LittleEndian.PutUint32(body[8:12], snapVersion+1)
			binary.LittleEndian.PutUint32(b[len(b)-4:], crc32.Checksum(body, castagnoli))
			return b
		}); err == nil {
			t.Fatal("wrong version accepted")
		}
	})
	t.Run("schema fingerprint mismatch", func(t *testing.T) {
		other := testSchema()
		other.Metrics = []string{"visits"}
		if err := load(t, other, Config{}, nil); err == nil {
			t.Fatal("wrong schema accepted")
		}
	})
	t.Run("stale position", func(t *testing.T) {
		// snapshot data is at ts(100); with TTL 1h and current wall clock,
		// maxUpdated is far older than now-TTL -> stale
		if err := load(t, testSchema(), Config{TTL: time.Hour}, nil); err == nil {
			t.Fatal("stale snapshot accepted")
		}
	})
	t.Run("store untouched after failed load", func(t *testing.T) {
		s2, _ := New(testSchema(), Config{})
		_ = s2.seed(baseRecords())
		before := s2.Rows()
		_ = s2.LoadSnapshot(filepath.Join(dir, "absent.bin"))
		if s2.Rows() != before {
			t.Fatal("failed load must not modify the store")
		}
	})
}

func TestSaveIsAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")
	s := seededStore(t, baseRecords())
	if err := s.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "snap.bin" {
		t.Fatalf("leftover temp files: %v", entries)
	}
}
