package facetta

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// waitFor polls cond until it holds or the deadline expires.
func waitFor(t *testing.T, msg string, cond func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for %s", msg)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestCompactorRatioTrigger: a single new delta row over a small base with a
// small DeltaRatio drives a compaction via the check tick.
func TestCompactorRatioTrigger(t *testing.T) {
	s := seededStore(t, baseRecords())
	if err := s.Apply([]Record{rec(ts(500), []float64{5, 5}, "s9", "a9", "p9", "JP", "web")}); err != nil {
		t.Fatal(err)
	}
	c := NewCompactor(s, CompactorConfig{
		CheckInterval:   2 * time.Millisecond,
		CompactInterval: time.Hour, // isolate the ratio path
		DeltaRatio:      0.1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	waitFor(t, "ratio compaction", func() bool {
		return s.DeltaRows() == 0 && s.Stats().Compactions >= 1
	})
}

// TestCompactorPeriodicExpiry: no delta, but a future-expiring base row ages
// out under an injected clock; the compact tick reclaims it without any Apply.
func TestCompactorPeriodicExpiry(t *testing.T) {
	s, clk := newClockStore(t, Config{})
	now := *clk
	recs := []Record{
		rec(now, []float64{10, 1}, "s1", "a1", "p1", "US", "ios"), // never expires
		recExp(now, now.Add(time.Hour), []float64{20, 2}, "s2", "a2", "p2", "DE", "web"),
	}
	if err := s.seed(recs); err != nil {
		t.Fatal(err)
	}
	if s.Rows() != 2 || s.DeltaRows() != 0 {
		t.Fatalf("seed rows=%d delta=%d, want 2+0", s.Rows(), s.DeltaRows())
	}
	*clk = now.Add(time.Hour + time.Millisecond) // s2 now expired

	c := NewCompactor(s, CompactorConfig{
		CheckInterval:   time.Hour, // isolate the periodic tidy path
		CompactInterval: 2 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	waitFor(t, "periodic expiry reclaim", func() bool {
		return s.Rows() == 1
	})
}

// TestCompactorCapBlocked: while cap-blocked the ratio path stays quiet (no
// compaction storm), yet the CompactInterval tick keeps attempting and the
// failures accrue.
func TestCompactorCapBlocked(t *testing.T) {
	s, err := New(testSchema(), Config{MaxRows: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.seed(baseRecords()); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply([]Record{rec(ts(500), []float64{1, 1}, "s9", "a9", "p9", "JP", "web")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); !errors.Is(err, ErrRowLimit) {
		t.Fatalf("Compact err = %v, want ErrRowLimit", err)
	}
	if !s.isCapBlocked() {
		t.Fatal("cap not blocked")
	}
	failBefore := s.Stats().CompactionFailures

	c := NewCompactor(s, CompactorConfig{
		CheckInterval:   2 * time.Millisecond,
		CompactInterval: 2 * time.Millisecond,
		DeltaRatio:      0.1, // ratio would fire if not gated by isCapBlocked
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	// The compact tick keeps trying and failing (cap still exceeded).
	waitFor(t, "compact-tick retries while blocked", func() bool {
		return s.Stats().CompactionFailures > failBefore
	})
	// No successful compaction can happen while blocked: the ratio path must
	// not have snuck one through.
	if got := s.Stats().Compactions; got != 0 {
		t.Fatalf("Compactions = %d while cap-blocked, want 0 (no storm)", got)
	}
	if s.DeltaRows() != 1 {
		t.Fatalf("delta drained while blocked: %d, want 1", s.DeltaRows())
	}
}

// TestCompactorDeltaCapTrigger: with the ratio set impossibly high (so the
// relative trigger never fires), a small absolute MaxDeltaRows still drives a
// compaction once the delta crosses the cap.
func TestCompactorDeltaCapTrigger(t *testing.T) {
	s := seededStore(t, baseRecords())
	// 3 delta rows; ratio 100.0 would need 400 rows over a 4-row base, so only
	// the absolute cap (MaxDeltaRows=3) can trigger here.
	if err := s.Apply([]Record{
		rec(ts(500), []float64{1, 1}, "s9", "a9", "p9", "JP", "web"),
		rec(ts(500), []float64{1, 1}, "s9", "a9", "p8", "JP", "web"),
		rec(ts(500), []float64{1, 1}, "s9", "a9", "p7", "JP", "web"),
	}); err != nil {
		t.Fatal(err)
	}
	c := NewCompactor(s, CompactorConfig{
		CheckInterval:   2 * time.Millisecond,
		CompactInterval: time.Hour, // isolate the check path
		DeltaRatio:      100.0,     // relative trigger never fires
		MaxDeltaRows:    3,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	waitFor(t, "delta-cap compaction", func() bool {
		return s.DeltaRows() == 0 && s.Stats().Compactions >= 1
	})
}

// TestCompactorSaveAndCancel: SnapshotPath set -> a file appears after the
// compaction; cancelling the context makes Run return context.Canceled.
func TestCompactorSaveAndCancel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.bin")
	s := seededStore(t, baseRecords())
	if err := s.Apply([]Record{rec(ts(500), []float64{5, 5}, "s9", "a9", "p9", "JP", "web")}); err != nil {
		t.Fatal(err)
	}
	c := NewCompactor(s, CompactorConfig{
		CheckInterval:   2 * time.Millisecond,
		CompactInterval: time.Hour,
		DeltaRatio:      0.1,
		SnapshotPath:    path,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	waitFor(t, "snapshot file after compaction", func() bool {
		_, err := os.Stat(path)
		return err == nil && s.Stats().SnapshotSaves >= 1
	})

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}
