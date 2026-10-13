package store

import (
	"sync"

	"github.com/Meetp369/ghstore/internal/event"
)

// memtable is the in-memory row buffer that sits in front of one WAL file.
// It is append-only: once it is rotated out of the active slot it is never
// written again, which is what lets the flusher read it without blocking
// queries.
type memtable struct {
	walID uint64

	mu  sync.RWMutex
	evs []event.Event
}

func newMemtable(walID uint64) *memtable {
	return &memtable{walID: walID}
}

func (m *memtable) append(evs []event.Event) int {
	m.mu.Lock()
	m.evs = append(m.evs, evs...)
	n := len(m.evs)
	m.mu.Unlock()
	return n
}

func (m *memtable) len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.evs)
}

// rows returns the rows appended so far. Callers must not modify the
// returned slice; it is safe to read because rows are never rewritten.
func (m *memtable) rows() []event.Event {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.evs[:len(m.evs):len(m.evs)]
}

func (m *memtable) groupCount(groupCol string, from, to int64, filterCol, filterVal string, out map[string]int64) int64 {
	var matched int64
	for _, e := range m.rows() {
		if e.TS < from || e.TS >= to {
			continue
		}
		if filterCol != "" {
			if v, _ := e.Field(filterCol); v != filterVal {
				continue
			}
		}
		k, _ := e.Field(groupCol)
		out[k]++
		matched++
	}
	return matched
}
