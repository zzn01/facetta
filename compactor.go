package facetta

import (
	"context"
	"time"
)

// CompactorConfig configures the background compaction policy.
type CompactorConfig struct {
	CheckInterval   time.Duration // ratio/expiry poll cadence, default 10s
	CompactInterval time.Duration // periodic tidy cadence, default 5m
	DeltaRatio      float64       // compact when delta/base exceeds this, default 0.1
	MaxDeltaRows    int           // absolute delta cap: compact when DeltaRows >= this, 0 disables
	SnapshotPath    string        // save after successful compaction; "" disables
}

func (c CompactorConfig) withDefaults() CompactorConfig {
	if c.CheckInterval <= 0 {
		c.CheckInterval = 10 * time.Second
	}
	if c.CompactInterval <= 0 {
		c.CompactInterval = 5 * time.Minute
	}
	if c.DeltaRatio <= 0 {
		c.DeltaRatio = 0.1
	}
	return c
}

// Compactor drives Store.Compact in the background. It knows nothing about
// where data comes from; the host owns ingestion (Apply/ReplaceAll).
type Compactor struct {
	store *Store
	cfg   CompactorConfig
}

func NewCompactor(s *Store, cfg CompactorConfig) *Compactor {
	return &Compactor{store: s, cfg: cfg.withDefaults()}
}

// Run blocks until ctx is done. Typically `go c.Run(ctx)`.
func (c *Compactor) Run(ctx context.Context) error {
	check := time.NewTicker(c.cfg.CheckInterval)
	defer check.Stop()
	compact := time.NewTicker(c.cfg.CompactInterval)
	defer compact.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-check.C:
			// Ratio-triggered compaction, skipped while cap-blocked: it would
			// just re-fail the cap. The CompactInterval tick still retries so
			// recovery (TTL shrinkage, upstream deletes) lands.
			if c.store.isCapBlocked() {
				continue
			}
			// Two triggers, both gated by cap-block above. The ratio bounds the
			// delta's RELATIVE size; MaxDeltaRows is an ABSOLUTE cap that bounds
			// the delta's per-query scan cost (each delta row is a linear-scan
			// visit). MaxDeltaRows lives here, in Compactor policy, and NOT in
			// NeedsCompaction: NeedsCompaction answers "would a compact change
			// the view", a state predicate, whereas the absolute cap is a
			// latency policy knob.
			d := c.store.DeltaRows()
			base := max(c.store.Rows(), 1)
			ratioHit := d > 0 && float64(d) >= c.cfg.DeltaRatio*float64(base)
			capHit := c.cfg.MaxDeltaRows > 0 && d >= c.cfg.MaxDeltaRows
			if ratioHit || capHit {
				c.compactAndSave()
			}
		case <-compact.C:
			// Periodic tidy: attempts even while cap-blocked (recovery discovery).
			if c.store.NeedsCompaction() {
				c.compactAndSave()
			}
		}
	}
}

func (c *Compactor) compactAndSave() {
	if err := c.store.Compact(); err != nil {
		return // counted in stats; old snapshot keeps serving
	}
	if c.cfg.SnapshotPath == "" {
		return
	}
	_ = c.store.SaveSnapshot(c.cfg.SnapshotPath) // failures counted in stats
}
