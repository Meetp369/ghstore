// Package store ties the WAL, the in-memory row buffer and the columnar
// segments together.
//
// Write path: Append writes a batch to the active WAL file, then to the
// active memtable. When the memtable reaches FlushThreshold rows it is
// rotated: it moves to the immutable list and a fresh memtable + WAL file
// take its place. Rotation is a pointer swap under a short lock, so writers
// never wait for disk compaction.
//
// Compaction: a single background goroutine takes the oldest immutable
// memtable, writes it as a columnar segment, records the segment in the
// manifest, and only then swaps the memtable for the segment and deletes its
// WAL file. Queries snapshot (segments, immutable memtables, active
// memtable) under a read lock, so each row is seen exactly once: either in
// its memtable or in its segment, never both and never neither.
//
// Recovery: on Open, segments listed in the manifest are loaded, segment
// directories not in the manifest are deleted (crash mid-flush), WAL files
// already covered by the manifest are deleted (crash after the manifest
// write but before WAL cleanup), and the remaining WAL files are replayed
// into immutable memtables and flushed.
package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Meetp369/ghstore/internal/event"
	"github.com/Meetp369/ghstore/internal/segment"
	"github.com/Meetp369/ghstore/internal/wal"
)

// Options configures a Store.
type Options struct {
	Dir string
	// FlushThreshold is the number of buffered rows that triggers a
	// rotation and background flush. Default 200,000.
	FlushThreshold int
	// SyncWAL fsyncs the WAL on every Append. Without it a machine crash
	// (not a process crash) can lose recently acknowledged batches.
	SyncWAL bool
	// QueryWorkers bounds how many segments are scanned in parallel.
	// Default 4.
	QueryWorkers int
}

// ErrClosed is returned by operations on a closed store.
var ErrClosed = errors.New("store: closed")

// Store is a concurrent, crash-safe event store.
type Store struct {
	opts Options

	// writeMu serializes appends and rotations (WAL order == append order).
	writeMu sync.Mutex
	wal     *wal.Writer
	closed  bool

	// mu guards the structure below; held only for pointer swaps and
	// snapshots, never during disk I/O.
	mu       sync.RWMutex
	cond     *sync.Cond
	active   *memtable
	immut    []*memtable
	segs     []*segment.Segment
	flushErr error

	// man is owned by the flusher goroutine after Open returns.
	man manifest

	flushCh  chan struct{}
	stop     chan struct{}
	abandon  bool // test hook: stop without flushing (simulated crash)
	wg       sync.WaitGroup
	recovery RecoveryInfo
}

// RecoveryInfo describes what Open found on disk.
type RecoveryInfo struct {
	SegmentsLoaded    int
	WALFilesReplayed  int
	RowsReplayed      int
	OrphansRemoved    int
	StaleWALsRemoved  int
	TornWALTailsFound int
}

// Open opens or creates a store in opts.Dir and recovers any unflushed data.
func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, errors.New("store: Dir is required")
	}
	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = 200_000
	}
	if opts.QueryWorkers <= 0 {
		opts.QueryWorkers = 4
	}
	for _, d := range []string{opts.Dir, walDir(opts.Dir), segDir(opts.Dir)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	man, err := loadManifest(opts.Dir)
	if err != nil {
		return nil, err
	}
	s := &Store{opts: opts, man: man, flushCh: make(chan struct{}, 1), stop: make(chan struct{})}
	s.cond = sync.NewCond(&s.mu)

	// 1. Segments: load the ones in the manifest, delete the rest.
	listed := make(map[string]bool, len(man.Segments))
	for _, id := range man.Segments {
		seg, err := segment.Open(s.segPath(id))
		if err != nil {
			return nil, fmt.Errorf("store: manifest lists segment %d: %w", id, err)
		}
		s.segs = append(s.segs, seg)
		listed[filepath.Base(s.segPath(id))] = true
	}
	entries, err := os.ReadDir(segDir(opts.Dir))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !listed[e.Name()] {
			if err := os.RemoveAll(filepath.Join(segDir(opts.Dir), e.Name())); err != nil {
				return nil, err
			}
			s.recovery.OrphansRemoved++
		}
	}
	s.recovery.SegmentsLoaded = len(s.segs)

	// 2. WAL files: drop the ones already flushed, replay the rest in order.
	ids, err := listWALs(walDir(opts.Dir))
	if err != nil {
		return nil, err
	}
	maxID := man.FlushedWAL
	for _, id := range ids {
		if id > maxID {
			maxID = id
		}
		if id <= man.FlushedWAL {
			if err := os.Remove(s.walPath(id)); err != nil {
				return nil, err
			}
			s.recovery.StaleWALsRemoved++
			continue
		}
		res, err := wal.Replay(s.walPath(id))
		if err != nil {
			return nil, err
		}
		if res.Truncated {
			s.recovery.TornWALTailsFound++
		}
		if len(res.Events) == 0 {
			if err := os.Remove(s.walPath(id)); err != nil {
				return nil, err
			}
			continue
		}
		m := newMemtable(id)
		m.append(res.Events)
		s.immut = append(s.immut, m)
		s.recovery.WALFilesReplayed++
		s.recovery.RowsReplayed += len(res.Events)
	}

	// 3. Fresh active memtable + WAL file.
	s.active = newMemtable(maxID + 1)
	w, err := wal.Create(s.walPath(s.active.walID), opts.SyncWAL)
	if err != nil {
		return nil, err
	}
	s.wal = w

	s.wg.Add(1)
	go s.flushLoop()
	if len(s.immut) > 0 {
		s.signal()
	}
	return s, nil
}

// Recovery returns what Open found and repaired on disk.
func (s *Store) Recovery() RecoveryInfo { return s.recovery }

// Append durably adds a batch of events. When it returns nil, the batch is
// in the WAL and visible to queries.
func (s *Store) Append(evs []event.Event) error {
	if len(evs) == 0 {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if err := s.wal.Append(evs); err != nil {
		return err
	}
	if s.active.append(evs) >= s.opts.FlushThreshold {
		return s.rotateLocked()
	}
	return nil
}

// rotateLocked moves the active memtable to the immutable list and starts a
// new WAL file. Caller holds writeMu.
func (s *Store) rotateLocked() error {
	if s.active.len() == 0 {
		return nil
	}
	if err := s.wal.Close(); err != nil {
		return err
	}
	next := newMemtable(s.active.walID + 1)
	w, err := wal.Create(s.walPath(next.walID), s.opts.SyncWAL)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.immut = append(s.immut, s.active)
	s.active = next
	s.mu.Unlock()
	s.wal = w
	s.signal()
	return nil
}

func (s *Store) signal() {
	select {
	case s.flushCh <- struct{}{}:
	default:
	}
}

// Flush rotates the active memtable and waits until every buffered row is in
// a segment.
func (s *Store) Flush() error {
	s.writeMu.Lock()
	if s.closed {
		s.writeMu.Unlock()
		return ErrClosed
	}
	err := s.rotateLocked()
	s.writeMu.Unlock()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.immut) > 0 && s.flushErr == nil {
		s.cond.Wait()
	}
	return s.flushErr
}

// Close flushes all buffered rows to segments and releases resources.
func (s *Store) Close() error {
	s.writeMu.Lock()
	if s.closed {
		s.writeMu.Unlock()
		return nil
	}
	s.closed = true
	werr := s.wal.Close()
	s.mu.Lock()
	if s.active.len() > 0 {
		s.immut = append(s.immut, s.active)
		s.active = newMemtable(s.active.walID + 1)
	}
	s.mu.Unlock()
	s.writeMu.Unlock()

	close(s.stop)
	s.wg.Wait()
	s.mu.RLock()
	ferr := s.flushErr
	s.mu.RUnlock()
	return errors.Join(werr, ferr)
}

// crash stops the store like a killed process would: no final flush, WAL
// files left as they are. Used by tests.
func (s *Store) crash() {
	s.writeMu.Lock()
	s.closed = true
	s.abandon = true
	_ = s.wal.Close()
	s.writeMu.Unlock()
	close(s.stop)
	s.wg.Wait()
}

func (s *Store) flushLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.flushCh:
			s.flushPending()
		case <-s.stop:
			if !s.abandon {
				s.flushPending()
			}
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
			return
		}
	}
}

// flushPending flushes immutable memtables oldest-first. On error it records
// the error and returns; the next signal retries.
func (s *Store) flushPending() {
	for {
		s.mu.RLock()
		if len(s.immut) == 0 {
			s.mu.RUnlock()
			return
		}
		m := s.immut[0]
		s.mu.RUnlock()

		if err := s.flushOne(m); err != nil {
			s.mu.Lock()
			s.flushErr = err
			s.cond.Broadcast()
			s.mu.Unlock()
			return
		}
	}
}

func (s *Store) flushOne(m *memtable) error {
	id := s.man.NextSegment
	seg, err := segment.Write(s.segPath(id), id, m.walID, m.rows())
	if err != nil {
		return fmt.Errorf("store: write segment %d: %w", id, err)
	}
	next := s.man
	next.Segments = append(append([]uint64(nil), s.man.Segments...), id)
	next.NextSegment = id + 1
	next.FlushedWAL = m.walID
	if err := saveManifest(s.opts.Dir, next); err != nil {
		return fmt.Errorf("store: save manifest: %w", err)
	}
	s.man = next

	s.mu.Lock()
	s.segs = append(s.segs, seg)
	s.immut = s.immut[1:]
	s.flushErr = nil
	s.cond.Broadcast()
	s.mu.Unlock()

	// The rows are now durable in the segment; the WAL file is obsolete.
	// If we crash before this, Open deletes it because id <= FlushedWAL.
	if err := os.Remove(s.walPath(m.walID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// snapshot returns a consistent view of all row sources.
func (s *Store) snapshot() ([]*segment.Segment, []*memtable) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	segs := append([]*segment.Segment(nil), s.segs...)
	mems := make([]*memtable, 0, len(s.immut)+1)
	mems = append(mems, s.immut...)
	mems = append(mems, s.active)
	return segs, mems
}

// Stats summarizes the store's contents.
type Stats struct {
	Segments     int    `json:"segments"`
	SegmentRows  int64  `json:"segment_rows"`
	BufferedRows int64  `json:"buffered_rows"`
	PendingFlush int    `json:"pending_flush"`
	TotalRows    int64  `json:"total_rows"`
	MinTS        int64  `json:"min_ts"`
	MaxTS        int64  `json:"max_ts"`
	LastFlushErr string `json:"last_flush_error,omitempty"`
}

// Stats returns row counts and the time range covered.
func (s *Store) Stats() Stats {
	segs, mems := s.snapshot()
	st := Stats{Segments: len(segs), PendingFlush: len(mems) - 1}
	first := true
	see := func(min, max int64) {
		if first || min < st.MinTS {
			st.MinTS = min
		}
		if first || max > st.MaxTS {
			st.MaxTS = max
		}
		first = false
	}
	for _, g := range segs {
		st.SegmentRows += int64(g.Meta.Count)
		see(g.Meta.MinTS, g.Meta.MaxTS)
	}
	for _, m := range mems {
		for _, e := range m.rows() {
			st.BufferedRows++
			see(e.TS, e.TS)
		}
	}
	st.TotalRows = st.SegmentRows + st.BufferedRows
	s.mu.RLock()
	if s.flushErr != nil {
		st.LastFlushErr = s.flushErr.Error()
	}
	s.mu.RUnlock()
	return st
}

func walDir(dir string) string { return filepath.Join(dir, "wal") }
func segDir(dir string) string { return filepath.Join(dir, "segments") }

func (s *Store) walPath(id uint64) string {
	return filepath.Join(walDir(s.opts.Dir), fmt.Sprintf("wal-%08d.log", id))
}

func (s *Store) segPath(id uint64) string {
	return filepath.Join(segDir(s.opts.Dir), fmt.Sprintf("seg-%08d", id))
}

func listWALs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log"), 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
