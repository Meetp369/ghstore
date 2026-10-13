package ingest

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Meetp369/ghstore/internal/event"
)

type sink struct {
	mu      sync.Mutex
	evs     []event.Event
	batches int
	failAt  int // fail on this batch number (1-based); 0 = never
}

func (s *sink) Append(evs []event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches++
	if s.failAt > 0 && s.batches == s.failAt {
		return errors.New("disk full")
	}
	s.evs = append(s.evs, evs...)
	return nil
}

func line(i int) string {
	return fmt.Sprintf(`{"id":"%d","type":"PushEvent","actor":{"login":"u%d"},"repo":{"name":"o/r%d"},"payload":{},"created_at":"2024-01-01T00:00:%02dZ"}`, i, i, i%3, i%60)
}

func lines(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString(line(i))
		b.WriteByte('\n')
	}
	return b.String()
}

func TestReaderIngestsAll(t *testing.T) {
	s := &sink{}
	st, err := Reader(context.Background(), s, strings.NewReader(lines(1000)), Options{Workers: 4, BatchSize: 64, ChunkSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if st.Events != 1000 || st.Lines != 1000 || st.Skipped != 0 || len(s.evs) != 1000 {
		t.Fatalf("stats %+v, sink has %d", st, len(s.evs))
	}
	seen := map[string]bool{}
	for _, e := range s.evs {
		if seen[e.Actor] {
			t.Fatalf("duplicate %s", e.Actor)
		}
		seen[e.Actor] = true
	}
}

func TestBadLinesSkipped(t *testing.T) {
	in := line(1) + "\nnot json\n\n" + `{"type":"PushEvent"}` + "\n" + line(2) + "\n"
	s := &sink{}
	st, err := Reader(context.Background(), s, strings.NewReader(in), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Events != 2 || st.Skipped != 2 || st.Lines != 4 {
		t.Fatalf("stats %+v", st)
	}
}

func TestBatchSizeRespected(t *testing.T) {
	s := &sink{}
	if _, err := Reader(context.Background(), s, strings.NewReader(lines(100)), Options{Workers: 1, BatchSize: 25}); err != nil {
		t.Fatal(err)
	}
	if s.batches != 4 {
		t.Fatalf("got %d batches, want 4", s.batches)
	}
}

func TestFilesGzipAndPlain(t *testing.T) {
	dir := t.TempDir()
	gz := filepath.Join(dir, "a.json.gz")
	f, _ := os.Create(gz)
	zw := gzip.NewWriter(f)
	zw.Write([]byte(lines(300)))
	zw.Close()
	f.Close()
	plain := filepath.Join(dir, "b.json")
	os.WriteFile(plain, []byte(lines(200)), 0o644)

	s := &sink{}
	st, err := Files(context.Background(), s, []string{gz, plain}, Options{Workers: 3})
	if err != nil {
		t.Fatal(err)
	}
	if st.Events != 500 || len(s.evs) != 500 {
		t.Fatalf("stats %+v", st)
	}
	if st.EventsPerSec() <= 0 {
		t.Fatal("throughput not computed")
	}
}

func TestMissingFile(t *testing.T) {
	_, err := Files(context.Background(), &sink{}, []string{"/nonexistent/x.json.gz"}, Options{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCorruptGzip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bad.json.gz")
	os.WriteFile(p, []byte("not gzip"), 0o644)
	if _, err := Files(context.Background(), &sink{}, []string{p}, Options{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestAppendErrorStopsRun(t *testing.T) {
	s := &sink{failAt: 2}
	_, err := Reader(context.Background(), s, strings.NewReader(lines(5000)), Options{Workers: 2, BatchSize: 50})
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("err = %v, want disk full", err)
	}
}

func TestCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Reader(ctx, &sink{}, strings.NewReader(lines(10000)), Options{ChunkSize: 1})
	if err == nil {
		t.Fatal("expected context error")
	}
}
