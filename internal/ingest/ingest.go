// Package ingest loads GH Archive hourly dumps (.json.gz or plain .json,
// one event per line) into a store using a pool of parser goroutines.
//
// Pipeline: one reader goroutine per input stream splits lines into chunks
// -> N parser workers decode JSON and build batches -> each worker calls
// store.Append, which serializes WAL writes. JSON decoding is the expensive
// step, so it is the one that is parallelized.
package ingest

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Meetp369/ghstore/internal/event"
)

// Appender is the subset of *store.Store used by ingest.
type Appender interface {
	Append([]event.Event) error
}

// Options tunes the pipeline.
type Options struct {
	Workers   int // parser goroutines; default 4
	BatchSize int // events per Append; default 2,000
	ChunkSize int // lines per channel message; default 512
}

func (o *Options) defaults() {
	if o.Workers <= 0 {
		o.Workers = 4
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 2000
	}
	if o.ChunkSize <= 0 {
		o.ChunkSize = 512
	}
}

// Stats reports what an ingest run did.
type Stats struct {
	Lines   int64
	Events  int64
	Skipped int64
	Elapsed time.Duration
}

// EventsPerSec is the ingest throughput.
func (s Stats) EventsPerSec() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.Events) / s.Elapsed.Seconds()
}

// Files ingests each path in order.
func Files(ctx context.Context, dst Appender, paths []string, opts Options) (Stats, error) {
	opened := make([]func() (io.ReadCloser, error), len(paths))
	for i, p := range paths {
		p := p
		opened[i] = func() (io.ReadCloser, error) { return openFile(p) }
	}
	return run(ctx, dst, opened, opts)
}

// Reader ingests newline-delimited GH Archive events from r.
func Reader(ctx context.Context, dst Appender, r io.Reader, opts Options) (Stats, error) {
	src := func() (io.ReadCloser, error) { return io.NopCloser(r), nil }
	return run(ctx, dst, []func() (io.ReadCloser, error){src}, opts)
}

func run(ctx context.Context, dst Appender, sources []func() (io.ReadCloser, error), opts Options) (Stats, error) {
	opts.defaults()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var st Stats
	var events, skipped int64
	start := time.Now()

	chunks := make(chan [][]byte, opts.Workers*2)
	var firstErr error
	var errOnce sync.Once
	fail := func(err error) {
		errOnce.Do(func() { firstErr = err; cancel() })
	}

	var wg sync.WaitGroup
	for w := 0; w < opts.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]event.Event, 0, opts.BatchSize)
			flush := func() bool {
				if len(batch) == 0 {
					return true
				}
				if err := dst.Append(batch); err != nil {
					fail(err)
					return false
				}
				atomic.AddInt64(&events, int64(len(batch)))
				batch = make([]event.Event, 0, opts.BatchSize)
				return true
			}
			for chunk := range chunks {
				for _, line := range chunk {
					e, err := event.ParseGHArchive(line)
					if err != nil {
						atomic.AddInt64(&skipped, 1)
						continue
					}
					batch = append(batch, e)
					if len(batch) >= opts.BatchSize && !flush() {
						return
					}
				}
			}
			flush()
		}()
	}

	// Reader: split each source into line chunks.
	func() {
		defer close(chunks)
		for _, open := range sources {
			rc, err := open()
			if err != nil {
				fail(err)
				return
			}
			n, err := splitLines(ctx, rc, chunks, opts.ChunkSize)
			rc.Close()
			st.Lines += n
			if err != nil {
				fail(err)
				return
			}
		}
	}()
	wg.Wait()

	st.Events = atomic.LoadInt64(&events)
	st.Skipped = atomic.LoadInt64(&skipped)
	st.Elapsed = time.Since(start)
	return st, firstErr
}

func splitLines(ctx context.Context, r io.Reader, out chan<- [][]byte, chunkSize int) (int64, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 64<<20) // GH events can be large
	var n int64
	chunk := make([][]byte, 0, chunkSize)
	send := func() error {
		if len(chunk) == 0 {
			return nil
		}
		select {
		case out <- chunk:
		case <-ctx.Done():
			return ctx.Err()
		}
		chunk = make([][]byte, 0, chunkSize)
		return nil
	}
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		chunk = append(chunk, append([]byte(nil), line...))
		n++
		if len(chunk) == chunkSize {
			if err := send(); err != nil {
				return n, err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return n, fmt.Errorf("ingest: read: %w", err)
	}
	return n, send()
}

type gzFile struct {
	*gzip.Reader
	f *os.File
}

func (g gzFile) Close() error {
	g.Reader.Close()
	return g.f.Close()
}

func openFile(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(path, ".gz") {
		return f, nil
	}
	zr, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("ingest: %s: %w", path, err)
	}
	return gzFile{Reader: zr, f: f}, nil
}
