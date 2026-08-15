//go:build !race

package facetta

// raceEnabled is false in a normal (non-race) build; the strict absolute
// latency ceiling applies.
const raceEnabled = false
