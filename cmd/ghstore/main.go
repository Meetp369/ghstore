// Command ghstore ingests GH Archive dumps and serves group-by queries.
//
//	ghstore ingest -data ./data 2024-01-01-*.json.gz
//	ghstore serve  -data ./data -addr :8080 [files to ingest on start...]
//	ghstore query  -data ./data -group-by repo -type PushEvent -limit 10
//	ghstore stats  -data ./data
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/Meetp369/ghstore/internal/ingest"
	"github.com/Meetp369/ghstore/internal/server"
	"github.com/Meetp369/ghstore/internal/store"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: ghstore <command> [flags]

commands:
  ingest   load GH Archive .json.gz files into the store
  serve    run the HTTP API (optionally ingesting files first)
  query    run one group-by query and print JSON
  stats    print row counts and time range`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "ingest":
		err = cmdIngest(args)
	case "serve":
		err = cmdServe(args)
	case "query":
		err = cmdQuery(args)
	case "stats":
		err = cmdStats(args)
	default:
		usage()
	}
	if err != nil {
		log.Fatal(err)
	}
}

type common struct {
	data      string
	threshold int
	sync      bool
}

func commonFlags(fs *flag.FlagSet) *common {
	c := &common{}
	fs.StringVar(&c.data, "data", "./data", "data directory")
	fs.IntVar(&c.threshold, "flush-rows", 200_000, "rows buffered before a segment is flushed")
	fs.BoolVar(&c.sync, "sync", true, "fsync the WAL on every batch")
	return c
}

func (c *common) open() (*store.Store, error) {
	st, err := store.Open(store.Options{Dir: c.data, FlushThreshold: c.threshold, SyncWAL: c.sync,
		QueryWorkers: runtime.GOMAXPROCS(0)})
	if err != nil {
		return nil, err
	}
	if r := st.Recovery(); r.RowsReplayed > 0 || r.OrphansRemoved > 0 || r.StaleWALsRemoved > 0 {
		log.Printf("recovery: %+v", r)
	}
	return st, nil
}

func cmdIngest(args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	c := commonFlags(fs)
	workers := fs.Int("workers", runtime.GOMAXPROCS(0), "parser goroutines")
	batch := fs.Int("batch", 2000, "events per WAL batch")
	progress := fs.Bool("progress", false, "print 'acked <n>' after the run (used by the crash test)")
	fs.Parse(args)
	if fs.NArg() == 0 {
		return errors.New("ingest: no input files")
	}
	st, err := c.open()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stats, ierr := ingest.Files(ctx, st, fs.Args(), ingest.Options{Workers: *workers, BatchSize: *batch})
	log.Printf("ingested %d events (%d skipped) in %s: %.0f events/sec",
		stats.Events, stats.Skipped, stats.Elapsed.Round(time.Millisecond), stats.EventsPerSec())
	if *progress {
		fmt.Printf("acked %d\n", stats.Events)
	}
	cerr := st.Close()
	return errors.Join(ierr, cerr)
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	c := commonFlags(fs)
	addr := fs.String("addr", ":8080", "listen address")
	fs.Parse(args)
	st, err := c.open()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if fs.NArg() > 0 {
		go func() {
			stats, err := ingest.Files(ctx, st, fs.Args(), ingest.Options{Workers: runtime.GOMAXPROCS(0)})
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("ingest: %v", err)
			}
			log.Printf("ingested %d events in %s (%.0f events/sec)",
				stats.Events, stats.Elapsed.Round(time.Millisecond), stats.EventsPerSec())
		}()
	}

	srv := &http.Server{Addr: *addr, Handler: server.New(st), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shut)
	}()
	log.Printf("listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Printf("shutting down: flushing buffered rows")
	return st.Close()
}

func cmdQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	c := commonFlags(fs)
	q := store.Query{}
	fs.StringVar(&q.GroupBy, "group-by", "repo", "column to group by: type, repo, actor")
	typ := fs.String("type", "", "only count events of this type, e.g. PushEvent")
	from := fs.String("from", "", "start time, RFC 3339 (inclusive)")
	to := fs.String("to", "", "end time, RFC 3339 (exclusive)")
	fs.IntVar(&q.Limit, "limit", 10, "max groups (-1 for all)")
	fs.Parse(args)
	if *typ != "" {
		q.FilterCol, q.FilterVal = "type", *typ
	}
	var err error
	if q.From, err = rfc3339(*from); err != nil {
		return err
	}
	if q.To, err = rfc3339(*to); err != nil {
		return err
	}
	st, err := c.open()
	if err != nil {
		return err
	}
	defer st.Close()
	start := time.Now()
	res, err := st.Query(q)
	if err != nil {
		return err
	}
	took := time.Since(start)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		return err
	}
	log.Printf("query took %s", took)
	return nil
}

func cmdStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	c := commonFlags(fs)
	fs.Parse(args)
	st, err := c.open()
	if err != nil {
		return err
	}
	defer st.Close()
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(st.Stats())
}

func rfc3339(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, fmt.Errorf("bad time %q (want RFC 3339, e.g. 2024-01-01T15:00:00Z)", s)
	}
	return t.Unix(), nil
}
