// Command ghbench measures query latency against an already-loaded store.
//
// It runs N random group-by queries (random column, random time window,
// optional event-type filter) and reports p50/p95/p99 latency. Use it to
// produce the numbers in the README:
//
//	ghstore ingest -data ./data gh/*.json.gz     # prints events/sec
//	ghbench -data ./data -queries 1000           # prints p95
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/Meetp369/ghstore/internal/store"
)

var types = []string{"PushEvent", "CreateEvent", "PullRequestEvent", "WatchEvent", "IssueCommentEvent"}

func main() {
	data := flag.String("data", "./data", "data directory (already ingested)")
	n := flag.Int("queries", 1000, "number of queries")
	clients := flag.Int("clients", 1, "concurrent query clients")
	minWindow := flag.Duration("min-window", time.Hour, "shortest time window")
	seed := flag.Int64("seed", 42, "random seed")
	flag.Parse()

	st, err := store.Open(store.Options{Dir: *data, QueryWorkers: runtime.GOMAXPROCS(0)})
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()
	if err := st.Flush(); err != nil {
		log.Fatal(err)
	}
	stats := st.Stats()
	if stats.TotalRows == 0 {
		log.Fatal("store is empty; run `ghstore ingest` first")
	}
	span := stats.MaxTS - stats.MinTS + 1
	minW := int64(minWindow.Seconds())
	if minW > span {
		minW = span
	}

	rng := rand.New(rand.NewSource(*seed))
	queries := make([]store.Query, *n)
	for i := range queries {
		w := minW + rng.Int63n(span-minW+1)
		from := stats.MinTS + rng.Int63n(span-w+1)
		q := store.Query{GroupBy: []string{"repo", "actor", "type"}[rng.Intn(3)], From: from, To: from + w, Limit: 10}
		if rng.Intn(2) == 0 {
			q.FilterCol, q.FilterVal = "type", types[rng.Intn(len(types))]
		}
		queries[i] = q
	}

	// Warm-up so first-touch page faults are not counted.
	for _, q := range queries[:min(10, len(queries))] {
		st.Query(q)
	}

	lat := make([]time.Duration, len(queries))
	var matched int64
	var mu sync.Mutex
	next := make(chan int)
	var wg sync.WaitGroup
	start := time.Now()
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				t := time.Now()
				res, err := st.Query(queries[i])
				lat[i] = time.Since(t)
				if err != nil {
					log.Fatal(err)
				}
				mu.Lock()
				matched += res.Matched
				mu.Unlock()
			}
		}()
	}
	for i := range queries {
		next <- i
	}
	close(next)
	wg.Wait()
	wall := time.Since(start)

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration { return lat[int(p*float64(len(lat)-1))] }
	fmt.Fprintf(os.Stdout, `rows in store     %d (%d segments)
queries           %d (%d client(s))
avg rows matched  %d
p50               %s
p95               %s
p99               %s
max               %s
throughput        %.1f queries/sec
cpus              %d
`, stats.TotalRows, stats.Segments, len(lat), *clients, matched/int64(len(lat)),
		pct(0.50).Round(time.Microsecond*100), pct(0.95).Round(time.Microsecond*100),
		pct(0.99).Round(time.Microsecond*100), lat[len(lat)-1].Round(time.Microsecond*100),
		float64(len(lat))/wall.Seconds(), runtime.NumCPU())
}
