package store

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/Meetp369/ghstore/internal/event"
)

// gen returns n events with unique actors ("u<start+i>"), ts = 1000+start+i,
// repo cycling over 5 repos and type alternating Push/Watch.
func gen(start, n int) []event.Event {
	evs := make([]event.Event, n)
	for i := range evs {
		k := start + i
		typ := "PushEvent"
		if k%2 == 1 {
			typ = "WatchEvent"
		}
		evs[i] = event.Event{TS: int64(1000 + k), Type: typ, Repo: fmt.Sprintf("org/r%d", k%5), Actor: fmt.Sprintf("u%d", k)}
	}
	return evs
}

func open(t *testing.T, dir string, threshold int) *Store {
	t.Helper()
	s, err := Open(Options{Dir: dir, FlushThreshold: threshold, SyncWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mustAppend(t *testing.T, s *Store, evs []event.Event) {
	t.Helper()
	if err := s.Append(evs); err != nil {
		t.Fatal(err)
	}
}

func total(t *testing.T, s *Store) int64 {
	t.Helper()
	res, err := s.Query(Query{GroupBy: "type", Limit: -1})
	if err != nil {
		t.Fatal(err)
	}
	return res.Matched
}

// assertExactlyOnce checks that every actor u0..u(n-1) appears exactly once.
func assertExactlyOnce(t *testing.T, s *Store, n int) {
	t.Helper()
	res, err := s.Query(Query{GroupBy: "actor", Limit: -1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Matched != int64(n) || len(res.Groups) != n {
		t.Fatalf("matched %d rows in %d groups, want %d", res.Matched, len(res.Groups), n)
	}
	for _, g := range res.Groups {
		if g.Count != 1 {
			t.Fatalf("actor %s counted %d times", g.Key, g.Count)
		}
	}
}

func TestOpenRequiresDir(t *testing.T) {
	if _, err := Open(Options{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestQueryFromBufferOnly(t *testing.T) {
	s := open(t, t.TempDir(), 1000)
	defer s.Close()
	mustAppend(t, s, gen(0, 10))
	if got := total(t, s); got != 10 {
		t.Fatalf("total = %d", got)
	}
	if st := s.Stats(); st.Segments != 0 || st.BufferedRows != 10 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestRotationCreatesSegments(t *testing.T) {
	s := open(t, t.TempDir(), 100)
	defer s.Close()
	for i := 0; i < 10; i++ {
		mustAppend(t, s, gen(i*50, 50))
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	if st.Segments != 5 || st.SegmentRows != 500 || st.BufferedRows != 0 {
		t.Fatalf("stats = %+v", st)
	}
	assertExactlyOnce(t, s, 500)
}

func TestQuerySpansSegmentsAndBuffer(t *testing.T) {
	s := open(t, t.TempDir(), 100)
	defer s.Close()
	mustAppend(t, s, gen(0, 100)) // rotates
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, gen(100, 30)) // stays buffered
	st := s.Stats()
	if st.SegmentRows != 100 || st.BufferedRows != 30 {
		t.Fatalf("stats = %+v", st)
	}
	assertExactlyOnce(t, s, 130)
}

func TestQueryResults(t *testing.T) {
	s := open(t, t.TempDir(), 40)
	defer s.Close()
	mustAppend(t, s, gen(0, 100)) // ts 1000..1099
	s.Flush()
	mustAppend(t, s, gen(100, 20)) // ts 1100..1119, buffered

	tests := []struct {
		name string
		q    Query
		want []Group
	}{
		{"group by type", Query{GroupBy: "type"}, []Group{{"PushEvent", 60}, {"WatchEvent", 60}}},
		{"top 2 repos", Query{GroupBy: "repo", Limit: 2}, []Group{{"org/r0", 24}, {"org/r1", 24}}},
		{"filter push", Query{GroupBy: "type", FilterCol: "type", FilterVal: "PushEvent"}, []Group{{"PushEvent", 60}}},
		{"time range", Query{GroupBy: "type", From: 1000, To: 1010}, []Group{{"PushEvent", 5}, {"WatchEvent", 5}}},
		{"range across buffer edge", Query{GroupBy: "type", From: 1095, To: 1105}, []Group{{"PushEvent", 5}, {"WatchEvent", 5}}},
		{"range only buffer", Query{GroupBy: "repo", From: 1115, Limit: -1}, []Group{{"org/r0", 1}, {"org/r1", 1}, {"org/r2", 1}, {"org/r3", 1}, {"org/r4", 1}}},
		{"no match", Query{GroupBy: "repo", FilterCol: "type", FilterVal: "ForkEvent"}, []Group{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := s.Query(tt.q)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(res.Groups, tt.want) {
				t.Fatalf("got %v, want %v", res.Groups, tt.want)
			}
		})
	}
}

func TestQueryTiesSortByKey(t *testing.T) {
	s := open(t, t.TempDir(), 1000)
	defer s.Close()
	mustAppend(t, s, []event.Event{
		{TS: 1, Type: "T", Repo: "b"}, {TS: 1, Type: "T", Repo: "a"}, {TS: 1, Type: "T", Repo: "c"}, {TS: 1, Type: "T", Repo: "c"},
	})
	res, _ := s.Query(Query{GroupBy: "repo"})
	want := []Group{{"c", 2}, {"a", 1}, {"b", 1}}
	if !reflect.DeepEqual(res.Groups, want) {
		t.Fatalf("got %v, want %v", res.Groups, want)
	}
}

func TestSegmentSkipping(t *testing.T) {
	s := open(t, t.TempDir(), 100)
	defer s.Close()
	for i := 0; i < 4; i++ { // 4 segments of 100 rows, ts ranges disjoint
		mustAppend(t, s, gen(i*100, 100))
	}
	s.Flush()
	res, err := s.Query(Query{GroupBy: "type", From: 1150, To: 1160})
	if err != nil {
		t.Fatal(err)
	}
	if res.SegmentsScanned != 1 || res.SegmentsSkipped != 3 || res.Matched != 10 {
		t.Fatalf("scanned=%d skipped=%d matched=%d", res.SegmentsScanned, res.SegmentsSkipped, res.Matched)
	}
}

func TestQueryValidation(t *testing.T) {
	s := open(t, t.TempDir(), 100)
	defer s.Close()
	bad := []Query{
		{GroupBy: "stars"},
		{GroupBy: ""},
		{GroupBy: "repo", FilterCol: "stars", FilterVal: "1"},
		{GroupBy: "repo", From: 10, To: 5},
		{GroupBy: "repo", From: 5, To: 5},
	}
	for i, q := range bad {
		if _, err := s.Query(q); err == nil {
			t.Errorf("query %d (%+v): expected error", i, q)
		}
	}
}

func TestReopenAfterClose(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 100)
	for i := 0; i < 5; i++ {
		mustAppend(t, s, gen(i*50, 50))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := open(t, dir, 100)
	defer s2.Close()
	if r := s2.Recovery(); r.RowsReplayed != 0 || r.SegmentsLoaded != 3 {
		t.Fatalf("recovery after clean close = %+v", r)
	}
	assertExactlyOnce(t, s2, 250)
}

func TestCloseIsIdempotent(t *testing.T) {
	s := open(t, t.TempDir(), 100)
	mustAppend(t, s, gen(0, 5))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(gen(5, 1)); err != ErrClosed {
		t.Fatalf("Append after Close = %v, want ErrClosed", err)
	}
	if err := s.Flush(); err != ErrClosed {
		t.Fatalf("Flush after Close = %v, want ErrClosed", err)
	}
}

func TestFlushOnEmptyStore(t *testing.T) {
	s := open(t, t.TempDir(), 100)
	defer s.Close()
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.TotalRows != 0 || st.Segments != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestEmptyAppendIsNoop(t *testing.T) {
	s := open(t, t.TempDir(), 100)
	defer s.Close()
	if err := s.Append(nil); err != nil {
		t.Fatal(err)
	}
	if got := total(t, s); got != 0 {
		t.Fatalf("total = %d", got)
	}
}

// --- crash recovery ---

func TestRecoverUnflushedRowsFromWAL(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 1_000_000)
	mustAppend(t, s, gen(0, 300))
	s.crash()

	s2 := open(t, dir, 1_000_000)
	defer s2.Close()
	if r := s2.Recovery(); r.RowsReplayed != 300 || r.WALFilesReplayed != 1 {
		t.Fatalf("recovery = %+v", r)
	}
	assertExactlyOnce(t, s2, 300)
}

func TestRecoverMixOfSegmentsAndWAL(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 100)
	mustAppend(t, s, gen(0, 100))   // segment 1
	mustAppend(t, s, gen(100, 100)) // segment 2
	s.Flush()
	mustAppend(t, s, gen(200, 50)) // buffered only
	s.crash()

	s2 := open(t, dir, 100)
	defer s2.Close()
	r := s2.Recovery()
	if r.SegmentsLoaded != 2 || r.RowsReplayed != 50 {
		t.Fatalf("recovery = %+v", r)
	}
	assertExactlyOnce(t, s2, 250)
}

func TestRecoveredRowsGetFlushed(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 1_000_000)
	mustAppend(t, s, gen(0, 80))
	s.crash()
	s2 := open(t, dir, 1_000_000)
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	if st := s2.Stats(); st.SegmentRows != 80 || st.BufferedRows != 0 {
		t.Fatalf("stats = %+v", st)
	}
	s2.Close()
	walFiles, _ := os.ReadDir(walDir(dir))
	for _, f := range walFiles {
		if info, _ := f.Info(); info.Size() > 0 {
			t.Fatalf("WAL file %s still has data after flush", f.Name())
		}
	}
}

// Crash after the manifest was written but before the WAL file was deleted:
// the rows exist in both a segment and the WAL. Recovery must not count
// them twice.
func TestNoDuplicatesWhenWALOutlivesFlush(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 1_000_000)
	mustAppend(t, s, gen(0, 100))
	walFile := s.walPath(s.active.walID)
	saved, err := os.ReadFile(walFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil { // flushes and deletes the WAL
		t.Fatal(err)
	}
	if err := os.WriteFile(walFile, saved, 0o644); err != nil { // resurrect it
		t.Fatal(err)
	}
	s2 := open(t, dir, 1_000_000)
	defer s2.Close()
	if r := s2.Recovery(); r.StaleWALsRemoved != 1 || r.RowsReplayed != 0 {
		t.Fatalf("recovery = %+v", r)
	}
	assertExactlyOnce(t, s2, 100)
}

// Crash after a segment directory was written but before the manifest:
// the segment is an orphan and the rows are still in the WAL.
func TestOrphanSegmentIgnored(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 1_000_000)
	mustAppend(t, s, gen(0, 60))
	s.crash()
	orphan := filepath.Join(segDir(dir), "seg-00000099")
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(segDir(dir), "seg-00000001.tmp"), 0o755)

	s2 := open(t, dir, 1_000_000)
	defer s2.Close()
	if r := s2.Recovery(); r.OrphansRemoved != 2 || r.RowsReplayed != 60 {
		t.Fatalf("recovery = %+v", r)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("orphan segment not removed")
	}
	assertExactlyOnce(t, s2, 60)
}

func TestTornWALTailRecovered(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 1_000_000)
	mustAppend(t, s, gen(0, 10))
	mustAppend(t, s, gen(10, 10))
	walFile := s.walPath(s.active.walID)
	s.crash()
	data, _ := os.ReadFile(walFile)
	os.WriteFile(walFile, data[:len(data)-5], 0o644) // tear the second batch

	s2 := open(t, dir, 1_000_000)
	defer s2.Close()
	r := s2.Recovery()
	if r.RowsReplayed != 10 || r.TornWALTailsFound != 1 {
		t.Fatalf("recovery = %+v", r)
	}
	assertExactlyOnce(t, s2, 10)
}

func TestManyReopenCycles(t *testing.T) {
	dir := t.TempDir()
	n := 0
	for cycle := 0; cycle < 5; cycle++ {
		s := open(t, dir, 70)
		mustAppend(t, s, gen(n, 45))
		n += 45
		if cycle%2 == 0 {
			s.crash()
		} else if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	s := open(t, dir, 70)
	defer s.Close()
	assertExactlyOnce(t, s, n)
}

// --- concurrency (run with -race) ---

func TestConcurrentAppendsAndQueries(t *testing.T) {
	s := open(t, t.TempDir(), 500)
	defer s.Close()
	const writers, perWriter, batchSize = 8, 20, 50

	var wg sync.WaitGroup
	stop := make(chan struct{})
	queryErr := make(chan error, 1)
	// Readers query continuously while writers append and the flusher
	// compacts. Totals must never go down and never exceed what was sent.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last int64
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := s.Query(Query{GroupBy: "actor", Limit: -1})
				if err != nil {
					select {
					case queryErr <- err:
					default:
					}
					return
				}
				for _, g := range res.Groups {
					if g.Count != 1 {
						select {
						case queryErr <- fmt.Errorf("actor %s seen %d times mid-flush", g.Key, g.Count):
						default:
						}
						return
					}
				}
				if res.Matched < last {
					select {
					case queryErr <- fmt.Errorf("total went backwards: %d -> %d", last, res.Matched):
					default:
					}
					return
				}
				last = res.Matched
			}
		}()
	}

	var wwg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wwg.Add(1)
		go func(w int) {
			defer wwg.Done()
			for b := 0; b < perWriter; b++ {
				start := (w*perWriter + b) * batchSize
				if err := s.Append(gen(start, batchSize)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wwg.Wait()
	close(stop)
	wg.Wait()
	select {
	case err := <-queryErr:
		t.Fatal(err)
	default:
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	assertExactlyOnce(t, s, writers*perWriter*batchSize)
	if st := s.Stats(); st.Segments < 2 {
		t.Fatalf("expected several segments, got %+v", st)
	}
}

func TestConcurrentStatsDuringFlush(t *testing.T) {
	s := open(t, t.TempDir(), 100)
	defer s.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			s.Append(gen(i*20, 20))
		}
	}()
	for {
		select {
		case <-done:
			s.Flush()
			if st := s.Stats(); st.TotalRows != 1000 {
				t.Fatalf("total rows = %d", st.TotalRows)
			}
			return
		default:
			st := s.Stats()
			if st.TotalRows > 1000 {
				t.Fatalf("stats over-counted: %+v", st)
			}
		}
	}
}
