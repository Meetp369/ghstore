// Package wal implements an append-only write-ahead log of event batches.
//
// Each Append writes one record:
//
//	uint32 payload length | uint32 CRC32 (Castagnoli) of payload | payload
//
// where payload = uvarint event count, then each event in event.AppendBinary
// form. A batch is the unit of durability: on replay, a record that is
// truncated (torn write) or fails its checksum ends the log, so a batch is
// either fully recovered or not at all.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/Meetp369/ghstore/internal/event"
)

const headerSize = 8

// maxRecord guards against allocating huge buffers on a corrupt length.
const maxRecord = 256 << 20

var table = crc32.MakeTable(crc32.Castagnoli)

// Writer appends batches to a single WAL file.
type Writer struct {
	f    *os.File
	bw   *bufio.Writer
	sync bool
	buf  []byte
}

// Create opens path for appending, creating it if needed. If sync is true,
// every Append is fsynced before it returns.
func Create(path string, sync bool) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &Writer{f: f, bw: bufio.NewWriterSize(f, 1<<20), sync: sync}, nil
}

// Append durably writes one batch. An empty batch is a no-op.
func (w *Writer) Append(evs []event.Event) error {
	if len(evs) == 0 {
		return nil
	}
	p := w.buf[:0]
	p = append(p, make([]byte, headerSize)...)
	p = binary.AppendUvarint(p, uint64(len(evs)))
	for _, e := range evs {
		p = event.AppendBinary(p, e)
	}
	payload := p[headerSize:]
	binary.LittleEndian.PutUint32(p[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(p[4:8], crc32.Checksum(payload, table))
	w.buf = p

	if _, err := w.bw.Write(p); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if w.sync {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("wal: fsync: %w", err)
		}
	}
	return nil
}

// Close flushes, syncs and closes the file.
func (w *Writer) Close() error {
	ferr := w.bw.Flush()
	serr := w.f.Sync()
	cerr := w.f.Close()
	return errors.Join(ferr, serr, cerr)
}

// ReplayResult describes what Replay recovered.
type ReplayResult struct {
	Events  []event.Event
	Batches int
	// Truncated is true if replay stopped at a torn or corrupt record.
	Truncated bool
}

// Replay reads every intact batch from the WAL at path.
func Replay(path string) (ReplayResult, error) {
	var res ReplayResult
	f, err := os.Open(path)
	if err != nil {
		return res, fmt.Errorf("wal: open %s: %w", path, err)
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)

	hdr := make([]byte, headerSize)
	var payload []byte
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			if err == io.EOF {
				return res, nil
			}
			if err == io.ErrUnexpectedEOF {
				res.Truncated = true
				return res, nil
			}
			return res, fmt.Errorf("wal: read header: %w", err)
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		if n == 0 || n > maxRecord {
			res.Truncated = true
			return res, nil
		}
		if cap(payload) < int(n) {
			payload = make([]byte, n)
		}
		payload = payload[:n]
		if _, err := io.ReadFull(r, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				res.Truncated = true
				return res, nil
			}
			return res, fmt.Errorf("wal: read payload: %w", err)
		}
		if crc32.Checksum(payload, table) != sum {
			res.Truncated = true
			return res, nil
		}
		evs, err := decodeBatch(payload)
		if err != nil {
			res.Truncated = true
			return res, nil
		}
		res.Events = append(res.Events, evs...)
		res.Batches++
	}
}

func decodeBatch(p []byte) ([]event.Event, error) {
	count, n := binary.Uvarint(p)
	if n <= 0 || count > uint64(len(p)) {
		return nil, event.ErrCorrupt
	}
	off := n
	evs := make([]event.Event, 0, count)
	for i := uint64(0); i < count; i++ {
		e, m, err := event.DecodeBinary(p[off:])
		if err != nil {
			return nil, err
		}
		off += m
		evs = append(evs, e)
	}
	if off != len(p) {
		return nil, event.ErrCorrupt
	}
	return evs, nil
}
