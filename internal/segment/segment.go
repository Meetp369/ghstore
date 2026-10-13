// Package segment implements immutable columnar segments.
//
// A segment is a directory holding one file per column:
//
//	ts.col      count x int64 (little endian), sorted ascending
//	<col>.col   uvarint dict size, dict strings (uvarint len + bytes),
//	            then count x uint32 dictionary codes
//	meta.json   id, row count, min/max timestamp, source WAL id
//
// Rows are sorted by timestamp when written, so a time-range query finds
// its row range with two binary searches, and a whole segment is skipped
// when [MinTS, MaxTS] does not overlap the query range. String columns are
// dictionary-encoded, so group-by counts into a slice indexed by code
// instead of hashing every string.
package segment

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/Meetp369/ghstore/internal/event"
)

// Meta is the segment's metadata, stored in meta.json.
type Meta struct {
	ID    uint64 `json:"id"`
	Count int    `json:"count"`
	MinTS int64  `json:"min_ts"`
	MaxTS int64  `json:"max_ts"`
	WALID uint64 `json:"wal_id"`
}

// StrCol is a dictionary-encoded string column.
type StrCol struct {
	Dict  []string
	Codes []uint32
}

// Segment is an opened, read-only segment held in memory.
type Segment struct {
	Meta Meta
	Dir  string
	TS   []int64
	Cols map[string]*StrCol
}

// ErrEmpty is returned when writing a segment with no rows.
var ErrEmpty = errors.New("segment: no rows")

// Write builds a segment from evs and writes it atomically to dir: files go
// into dir+".tmp", are fsynced, and the directory is renamed into place.
// evs is not modified.
func Write(dir string, id, walID uint64, evs []event.Event) (*Segment, error) {
	if len(evs) == 0 {
		return nil, ErrEmpty
	}
	sorted := make([]event.Event, len(evs))
	copy(sorted, evs)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].TS < sorted[j].TS })

	seg := &Segment{
		Dir:  dir,
		TS:   make([]int64, len(sorted)),
		Cols: make(map[string]*StrCol, len(event.Columns)),
		Meta: Meta{ID: id, Count: len(sorted), WALID: walID,
			MinTS: sorted[0].TS, MaxTS: sorted[len(sorted)-1].TS},
	}
	for i, e := range sorted {
		seg.TS[i] = e.TS
	}
	for _, col := range event.Columns {
		seg.Cols[col] = encodeColumn(sorted, col)
	}

	tmp := dir + ".tmp"
	if err := os.RemoveAll(tmp); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, err
	}
	if err := writeFile(filepath.Join(tmp, "ts.col"), encodeTS(seg.TS)); err != nil {
		return nil, err
	}
	for _, col := range event.Columns {
		if err := writeFile(filepath.Join(tmp, col+".col"), encodeStrCol(seg.Cols[col])); err != nil {
			return nil, err
		}
	}
	mb, _ := json.Marshal(seg.Meta)
	if err := writeFile(filepath.Join(tmp, "meta.json"), mb); err != nil {
		return nil, err
	}
	if err := syncDir(tmp); err != nil {
		return nil, err
	}
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, dir); err != nil {
		return nil, err
	}
	if err := syncDir(filepath.Dir(dir)); err != nil {
		return nil, err
	}
	return seg, nil
}

// Open loads a segment directory into memory.
func Open(dir string) (*Segment, error) {
	mb, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return nil, fmt.Errorf("segment: %w", err)
	}
	seg := &Segment{Dir: dir, Cols: make(map[string]*StrCol, len(event.Columns))}
	if err := json.Unmarshal(mb, &seg.Meta); err != nil {
		return nil, fmt.Errorf("segment: bad meta in %s: %w", dir, err)
	}
	tb, err := os.ReadFile(filepath.Join(dir, "ts.col"))
	if err != nil {
		return nil, fmt.Errorf("segment: %w", err)
	}
	if len(tb) != 8*seg.Meta.Count {
		return nil, fmt.Errorf("segment: %s ts.col has %d bytes, want %d", dir, len(tb), 8*seg.Meta.Count)
	}
	seg.TS = make([]int64, seg.Meta.Count)
	for i := range seg.TS {
		seg.TS[i] = int64(binary.LittleEndian.Uint64(tb[8*i:]))
	}
	for _, col := range event.Columns {
		b, err := os.ReadFile(filepath.Join(dir, col+".col"))
		if err != nil {
			return nil, fmt.Errorf("segment: %w", err)
		}
		sc, err := decodeStrCol(b, seg.Meta.Count)
		if err != nil {
			return nil, fmt.Errorf("segment: %s/%s.col: %w", dir, col, err)
		}
		seg.Cols[col] = sc
	}
	return seg, nil
}

// Overlaps reports whether any row could fall in [from, to).
func (s *Segment) Overlaps(from, to int64) bool {
	return s.Meta.MaxTS >= from && s.Meta.MinTS < to
}

// GroupCount adds, for rows with from <= ts < to whose filterCol equals
// filterVal (no filter if filterCol is ""), the count of each value of
// groupCol into out. It returns the number of rows matched.
func (s *Segment) GroupCount(groupCol string, from, to int64, filterCol, filterVal string, out map[string]int64) (int64, error) {
	g, ok := s.Cols[groupCol]
	if !ok {
		return 0, fmt.Errorf("segment: unknown column %q", groupCol)
	}
	if !s.Overlaps(from, to) {
		return 0, nil
	}
	lo := sort.Search(len(s.TS), func(i int) bool { return s.TS[i] >= from })
	hi := sort.Search(len(s.TS), func(i int) bool { return s.TS[i] >= to })
	if lo >= hi {
		return 0, nil
	}

	var fcodes []uint32
	var fcode uint32
	if filterCol != "" {
		f, ok := s.Cols[filterCol]
		if !ok {
			return 0, fmt.Errorf("segment: unknown column %q", filterCol)
		}
		found := false
		for i, v := range f.Dict {
			if v == filterVal {
				fcode, found = uint32(i), true
				break
			}
		}
		if !found {
			return 0, nil // value never occurs in this segment
		}
		fcodes = f.Codes
	}

	counts := make([]int64, len(g.Dict))
	var matched int64
	if fcodes == nil {
		for _, c := range g.Codes[lo:hi] {
			counts[c]++
		}
		matched = int64(hi - lo)
	} else {
		for i := lo; i < hi; i++ {
			if fcodes[i] == fcode {
				counts[g.Codes[i]]++
				matched++
			}
		}
	}
	for code, n := range counts {
		if n > 0 {
			out[g.Dict[code]] += n
		}
	}
	return matched, nil
}

func encodeColumn(evs []event.Event, col string) *StrCol {
	sc := &StrCol{Codes: make([]uint32, len(evs))}
	idx := make(map[string]uint32)
	for i, e := range evs {
		v, _ := e.Field(col)
		code, ok := idx[v]
		if !ok {
			code = uint32(len(sc.Dict))
			idx[v] = code
			sc.Dict = append(sc.Dict, v)
		}
		sc.Codes[i] = code
	}
	return sc
}

func encodeTS(ts []int64) []byte {
	b := make([]byte, 8*len(ts))
	for i, v := range ts {
		binary.LittleEndian.PutUint64(b[8*i:], uint64(v))
	}
	return b
}

func encodeStrCol(sc *StrCol) []byte {
	b := binary.AppendUvarint(nil, uint64(len(sc.Dict)))
	for _, s := range sc.Dict {
		b = binary.AppendUvarint(b, uint64(len(s)))
		b = append(b, s...)
	}
	for _, c := range sc.Codes {
		b = binary.LittleEndian.AppendUint32(b, c)
	}
	return b
}

func decodeStrCol(b []byte, count int) (*StrCol, error) {
	n, m := binary.Uvarint(b)
	if m <= 0 || n > uint64(len(b)) {
		return nil, errors.New("bad dictionary size")
	}
	off := m
	sc := &StrCol{Dict: make([]string, 0, n)}
	for i := uint64(0); i < n; i++ {
		l, m := binary.Uvarint(b[off:])
		if m <= 0 || uint64(len(b)-off-m) < l {
			return nil, errors.New("truncated dictionary")
		}
		off += m
		sc.Dict = append(sc.Dict, string(b[off:off+int(l)]))
		off += int(l)
	}
	if len(b)-off != 4*count {
		return nil, fmt.Errorf("codes: %d bytes, want %d", len(b)-off, 4*count)
	}
	sc.Codes = make([]uint32, count)
	for i := range sc.Codes {
		c := binary.LittleEndian.Uint32(b[off+4*i:])
		if uint64(c) >= n {
			return nil, fmt.Errorf("code %d out of range", c)
		}
		sc.Codes[i] = c
	}
	return sc, nil
}

func writeFile(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	if _, err := w.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
