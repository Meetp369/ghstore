// Command ghgen writes synthetic hourly files in GH Archive's format
// (gzipped JSON, one event per line), for tests and offline runs.
//
// Real numbers for the README/resume should come from real GH Archive data
// (https://www.gharchive.org/); real events carry larger, more varied
// payloads, so parsing them is slower than parsing these.
//
//	ghgen -out ./gen -hours 24 -per-hour 100000
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var eventTypes = []struct {
	name   string
	weight int
}{
	{"PushEvent", 50}, {"CreateEvent", 12}, {"PullRequestEvent", 8}, {"WatchEvent", 7},
	{"IssueCommentEvent", 6}, {"DeleteEvent", 4}, {"IssuesEvent", 3},
	{"PullRequestReviewEvent", 3}, {"ForkEvent", 2}, {"ReleaseEvent", 2},
	{"PullRequestReviewCommentEvent", 2}, {"PublicEvent", 1},
}

type ghEvent struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Actor     map[string]any `json:"actor"`
	Repo      map[string]any `json:"repo"`
	Payload   map[string]any `json:"payload"`
	Public    bool           `json:"public"`
	CreatedAt string         `json:"created_at"`
}

func main() {
	out := flag.String("out", "./gen", "output directory")
	start := flag.String("start", "2024-01-01T00:00:00Z", "first hour (RFC 3339)")
	hours := flag.Int("hours", 24, "number of hourly files")
	perHour := flag.Int("per-hour", 100_000, "events per file")
	repos := flag.Int("repos", 200_000, "distinct repos (Zipf distributed)")
	actors := flag.Int("actors", 300_000, "distinct actors (Zipf distributed)")
	payload := flag.Int("payload-bytes", 1500, "filler bytes per event, to approximate real parse cost")
	seed := flag.Int64("seed", 1, "random seed")
	flag.Parse()

	t0, err := time.Parse(time.RFC3339, *start)
	if err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	rng := rand.New(rand.NewSource(*seed))
	repoZ := rand.NewZipf(rng, 1.2, 1, uint64(*repos-1))
	actorZ := rand.NewZipf(rng, 1.1, 1, uint64(*actors-1))
	total := 0
	for _, et := range eventTypes {
		total += et.weight
	}
	filler := strings.Repeat("x", *payload)
	id := 30_000_000_000

	for h := 0; h < *hours; h++ {
		hour := t0.Add(time.Duration(h) * time.Hour)
		name := fmt.Sprintf("%s-%d.json.gz", hour.Format("2006-01-02"), hour.Hour())
		path := filepath.Join(*out, name)
		f, err := os.Create(path)
		if err != nil {
			log.Fatal(err)
		}
		zw := gzip.NewWriter(f)
		bw := bufio.NewWriter(zw)
		enc := json.NewEncoder(bw)
		for i := 0; i < *perHour; i++ {
			r := rng.Intn(total)
			typ := eventTypes[0].name
			for _, et := range eventTypes {
				if r < et.weight {
					typ = et.name
					break
				}
				r -= et.weight
			}
			repo := repoZ.Uint64()
			actor := actorZ.Uint64()
			ts := hour.Add(time.Duration(rng.Int63n(int64(time.Hour))))
			id++
			ev := ghEvent{
				ID:   fmt.Sprint(id),
				Type: typ,
				Actor: map[string]any{"id": actor, "login": fmt.Sprintf("user%d", actor),
					"url": fmt.Sprintf("https://api.github.com/users/user%d", actor)},
				Repo: map[string]any{"id": repo, "name": fmt.Sprintf("org%d/repo%d", repo%5000, repo),
					"url": fmt.Sprintf("https://api.github.com/repos/org%d/repo%d", repo%5000, repo)},
				Payload:   map[string]any{"ref": "refs/heads/main", "body": filler},
				Public:    true,
				CreatedAt: ts.UTC().Format(time.RFC3339),
			}
			if err := enc.Encode(&ev); err != nil {
				log.Fatal(err)
			}
		}
		if err := bw.Flush(); err != nil {
			log.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			log.Fatal(err)
		}
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
		log.Printf("wrote %s (%d events)", path, *perHour)
	}
}
