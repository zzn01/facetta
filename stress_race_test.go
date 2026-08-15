//go:build race

package facetta

// raceEnabled is true when the test binary is built with -race. Under the race
// detector, compaction allocation and GC stop-the-world pauses are amplified
// ~10x, so the absolute per-query latency ceiling is relaxed; the relative
// bound (query latency far below compaction duration) still proves reads never
// block on the compaction lock.
const raceEnabled = true
