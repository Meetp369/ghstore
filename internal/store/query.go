package store

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/Meetp369/ghstore/internal/event"
)

// Query is a time-range group-by count, e.g. "top 10 repos by PushEvent
// count between 09:00 and 15:00".
type Query struct {
	GroupBy string // "type", "repo" or "actor"
	// From is the inclusive lower bound (Unix seconds); 0 means unbounded.
	From int64
	// To is the exclusive upper bound (Unix seconds); 0 means unbounded.
	To int64
	// FilterCol/FilterVal keep only rows where FilterCol == FilterVal.
	FilterCol string
	FilterVal string
	// Limit caps the number of groups returned; 0 means 10, -1 means all.
	Limit int
}

// Group is one row of a query result.
type Group struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

// Result is the answer to a Query.
type Result struct {
	Groups          []Group `json:"groups"`
	Matched         int64   `json:"matched_rows"`
	SegmentsScanned int     `json:"segments_scanned"`
	SegmentsSkipped int     `json:"segments_skipped"`
	BufferedScanned int     `json:"buffered_rows_scanned"`
}

// Query runs q against a consistent snapshot of the store. Segments are
// scanned in parallel; segments whose time range cannot match are skipped.
func (s *Store) Query(q Query) (Result, error) {
	if !event.ValidColumn(q.GroupBy) {
		return Result{}, fmt.Errorf("store: unknown group-by column %q", q.GroupBy)
	}
	if q.FilterCol != "" && !event.ValidColumn(q.FilterCol) {
		return Result{}, fmt.Errorf("store: unknown filter column %q", q.FilterCol)
	}
	from, to := q.From, q.To
	if to == 0 {
		to = math.MaxInt64
	}
	if from == 0 {
		from = math.MinInt64
	}
	if from >= to {
		return Result{}, fmt.Errorf("store: empty time range")
	}
	limit := q.Limit
	if limit == 0 {
		limit = 10
	}

	segs, mems := s.snapshot()
	var res Result

	// Fan out over segments with a bounded worker pool; each worker keeps a
	// private map so there is no lock contention while counting.
	work := make(chan int)
	type partial struct {
		counts  map[string]int64
		matched int64
		err     error
	}
	workers := s.opts.QueryWorkers
	if workers > len(segs) {
		workers = len(segs)
	}
	parts := make([]partial, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(p *partial) {
			defer wg.Done()
			p.counts = make(map[string]int64)
			for i := range work {
				n, err := segs[i].GroupCount(q.GroupBy, from, to, q.FilterCol, q.FilterVal, p.counts)
				if err != nil && p.err == nil {
					p.err = err
				}
				p.matched += n
			}
		}(&parts[w])
	}
	for i, g := range segs {
		if !g.Overlaps(from, to) {
			res.SegmentsSkipped++
			continue
		}
		res.SegmentsScanned++
		work <- i
	}
	close(work)
	wg.Wait()

	total := make(map[string]int64)
	for _, p := range parts {
		if p.err != nil {
			return Result{}, p.err
		}
		res.Matched += p.matched
		for k, v := range p.counts {
			total[k] += v
		}
	}
	for _, m := range mems {
		res.BufferedScanned += m.len()
		res.Matched += m.groupCount(q.GroupBy, from, to, q.FilterCol, q.FilterVal, total)
	}

	res.Groups = make([]Group, 0, len(total))
	for k, v := range total {
		res.Groups = append(res.Groups, Group{Key: k, Count: v})
	}
	sort.Slice(res.Groups, func(i, j int) bool {
		a, b := res.Groups[i], res.Groups[j]
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		return a.Key < b.Key
	})
	if limit > 0 && len(res.Groups) > limit {
		res.Groups = res.Groups[:limit]
	}
	return res, nil
}
