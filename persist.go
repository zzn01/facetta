package facetta

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
)

const snapMagic = "FCSNAP01"

// snapVersion is the on-disk format version. v2 appends the per-row expireAt
// column after the updated column; v1 files are rejected (the caller falls
// back to a full pull).
const snapVersion = 2

var (
	errSnapshotFormat = errors.New("facetta: snapshot file invalid or corrupt")
	errSnapshotStale  = errors.New("facetta: snapshot position older than TTL")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// SaveSnapshot atomically persists the current base snapshot (FR-5). The
// delta overlay is not persisted: its rows are newer than the stored sync
// position and will be re-fetched on restart.
func (s *Store) SaveSnapshot(path string) error {
	snap := s.view.Load().base
	if err := writeSnapshotFile(path, &s.sc, snap); err != nil {
		s.st.snapshotSaveFailures.Add(1)
		return err
	}
	s.st.snapshotSaves.Add(1)
	return nil
}

// LoadSnapshot loads a persisted snapshot. It replaces the whole view,
// discarding any existing delta and extras. On any validation error the
// store is left untouched so the caller can fall back to a full pull.
func (s *Store) LoadSnapshot(path string) error {
	snap, err := readSnapshotFile(path, &s.sc, s.ttlCutoff())
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.MaxRows > 0 && snap.rows() > s.cfg.MaxRows {
		return fmt.Errorf("%w: %d rows > max %d", ErrRowLimit, snap.rows(), s.cfg.MaxRows)
	}
	s.view.Store(newView(snap))
	s.capBlocked.Store(false)
	return nil
}

func writeSnapshotFile(path string, sc *Schema, snap *snapshot) (err error) {
	f, err := os.CreateTemp(filepath.Dir(path), ".olsnap-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()
	h := crc32.New(castagnoli)
	w := bufio.NewWriterSize(io.MultiWriter(f, h), 1<<20)
	le := binary.LittleEndian
	var b8 [8]byte
	wU32 := func(v uint32) { le.PutUint32(b8[:4], v); w.Write(b8[:4]) }
	wU64 := func(v uint64) { le.PutUint64(b8[:], v); w.Write(b8[:]) }
	w.WriteString(snapMagic)
	wU32(snapVersion)
	wU64(sc.fingerprint())
	wU64(uint64(snap.maxUpdated))
	wU64(uint64(snap.rows()))
	for _, d := range snap.dicts {
		wU32(uint32(d.len()))
		for _, s := range d.strs {
			wU32(uint32(len(s)))
			w.WriteString(s)
		}
	}
	for _, col := range snap.dims {
		for _, v := range col {
			wU32(v)
		}
	}
	for _, col := range snap.mets {
		for _, v := range col {
			wU64(math.Float64bits(v))
		}
	}
	for _, u := range snap.updated {
		wU64(uint64(u))
	}
	for _, e := range snap.expire {
		wU64(uint64(e))
	}
	if err = w.Flush(); err != nil {
		return err
	}
	// trailing CRC written to the file only (not part of the hashed payload)
	le.PutUint32(b8[:4], h.Sum32())
	if _, err = f.Write(b8[:4]); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readSnapshotFile(path string, sc *Schema, ttlCutoff int64) (*snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < len(snapMagic)+4+8+8+8+4 {
		return nil, errSnapshotFormat
	}
	body, tail := data[:len(data)-4], data[len(data)-4:]
	le := binary.LittleEndian
	if crc32.Checksum(body, castagnoli) != le.Uint32(tail) {
		return nil, errSnapshotFormat
	}
	cur := 0
	rd := func(n int) ([]byte, bool) {
		if cur+n > len(body) {
			return nil, false
		}
		b := body[cur : cur+n]
		cur += n
		return b, true
	}
	rdU32 := func() (uint32, bool) {
		b, ok := rd(4)
		if !ok {
			return 0, false
		}
		return le.Uint32(b), true
	}
	rdU64 := func() (uint64, bool) {
		b, ok := rd(8)
		if !ok {
			return 0, false
		}
		return le.Uint64(b), true
	}
	if m, ok := rd(len(snapMagic)); !ok || string(m) != snapMagic {
		return nil, errSnapshotFormat
	}
	if v, ok := rdU32(); !ok || v != snapVersion {
		return nil, errSnapshotFormat
	}
	if fp, ok := rdU64(); !ok || fp != sc.fingerprint() {
		return nil, errSnapshotFormat
	}
	maxUpdatedU, ok := rdU64()
	if !ok {
		return nil, errSnapshotFormat
	}
	maxUpdated := int64(maxUpdatedU)
	if ttlCutoff > 0 && maxUpdated < ttlCutoff {
		return nil, errSnapshotStale
	}
	rowsU, ok := rdU64()
	if !ok || rowsU > uint64(len(body)) { // cheap sanity bound before allocating
		return nil, errSnapshotFormat
	}
	n := int(rowsU)
	nd, nm := len(sc.Dims), len(sc.Metrics)
	snap := &snapshot{sc: sc, maxUpdated: maxUpdated}
	snap.dicts = make([]*dict, nd)
	cards := make([]int, nd)
	for d := range nd {
		cnt, ok := rdU32()
		if !ok {
			return nil, errSnapshotFormat
		}
		snap.dicts[d] = newDict(sc.isNumeric(d))
		for range cnt {
			sl, ok := rdU32()
			if !ok {
				return nil, errSnapshotFormat
			}
			sb, ok := rd(int(sl))
			if !ok {
				return nil, errSnapshotFormat
			}
			snap.dicts[d].getOrAdd(string(sb))
		}
		if snap.dicts[d].len() != int(cnt) {
			return nil, errSnapshotFormat // duplicate strings in stored dict
		}
		if sc.isNumeric(d) {
			// numeric identity: every stored entry must already be canonical
			// (guards snapshots written before the dim was declared numeric)
			for _, str := range snap.dicts[d].strs {
				if cs, ok := canonNum(str); !ok || cs != str {
					return nil, errSnapshotFormat
				}
			}
		}
		cards[d] = int(cnt)
	}
	snap.dims = make([][]uint32, nd)
	for d := range nd {
		col := make([]uint32, n)
		for r := range n {
			v, ok := rdU32()
			if !ok || int(v) >= cards[d] {
				return nil, errSnapshotFormat
			}
			col[r] = v
		}
		snap.dims[d] = col
	}
	snap.mets = make([][]float64, nm)
	for m := range nm {
		col := make([]float64, n)
		for r := range n {
			v, ok := rdU64()
			if !ok {
				return nil, errSnapshotFormat
			}
			col[r] = math.Float64frombits(v)
		}
		snap.mets[m] = col
	}
	snap.updated = make([]int64, n)
	gotMax := int64(0)
	for r := range n {
		v, ok := rdU64()
		if !ok {
			return nil, errSnapshotFormat
		}
		snap.updated[r] = int64(v)
		if snap.updated[r] > gotMax {
			gotMax = snap.updated[r]
		}
	}
	snap.expire = make([]int64, n)
	for r := range n {
		v, ok := rdU64()
		if !ok {
			return nil, errSnapshotFormat
		}
		snap.expire[r] = int64(v)
		snap.trackMin(snap.updated[r], snap.expire[r]) // recompute minima on load
	}
	if cur != len(body) || (n > 0 && gotMax != maxUpdated) {
		return nil, errSnapshotFormat
	}
	shifts, err := computeShifts(cards, sc.IndexDims)
	if err != nil {
		return nil, errSnapshotFormat
	}
	snap.shifts = shifts
	snap.keys = make([]uint64, n)
	idBuf := make([]uint32, sc.IndexDims)
	for r := range n {
		for i := 0; i < sc.IndexDims; i++ {
			idBuf[i] = snap.dims[i][r]
		}
		snap.keys[r] = packKey(shifts, idBuf)
		if r > 0 && snap.keys[r-1] > snap.keys[r] {
			return nil, errSnapshotFormat // rows must be key-sorted
		}
	}
	return snap, nil
}
