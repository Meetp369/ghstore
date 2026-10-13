package server

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Meetp369/ghstore/internal/store"
)

func newServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(store.Options{Dir: t.TempDir(), FlushThreshold: 50})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(st))
	t.Cleanup(func() { srv.Close(); st.Close() })
	return srv, st
}

func body(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		typ := "PushEvent"
		if i%4 == 0 {
			typ = "WatchEvent"
		}
		fmt.Fprintf(&b, `{"type":"%s","actor":{"login":"u%d"},"repo":{"name":"o/r%d"},"created_at":"2024-01-01T10:%02d:00Z"}`+"\n", typ, i, i%2, i%60)
	}
	return b.String()
}

func post(t *testing.T, url string, r *http.Request) map[string]int64 {
	t.Helper()
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out map[string]int64
	json.NewDecoder(resp.Body).Decode(&out)
	return out
}

func get(t *testing.T, url string, wantCode int) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantCode {
		t.Fatalf("GET %s: status %d, want %d", url, resp.StatusCode, wantCode)
	}
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return out
}

func TestHealthz(t *testing.T) {
	srv, _ := newServer(t)
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz: %v %v", resp, err)
	}
}

func TestIngestThenQuery(t *testing.T) {
	srv, _ := newServer(t)
	req, _ := http.NewRequest("POST", srv.URL+"/ingest", strings.NewReader(body(120)))
	if got := post(t, srv.URL, req); got["ingested"] != 120 {
		t.Fatalf("ingest response %v", got)
	}
	out := get(t, srv.URL+"/query?group_by=type&limit=5", 200)
	groups := out["groups"].([]any)
	first := groups[0].(map[string]any)
	if first["key"] != "PushEvent" || first["count"].(float64) != 90 {
		t.Fatalf("groups = %v", groups)
	}
	if out["matched_rows"].(float64) != 120 {
		t.Fatalf("matched = %v", out["matched_rows"])
	}
}

func TestIngestGzipBody(t *testing.T) {
	srv, _ := newServer(t)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write([]byte(body(30)))
	zw.Close()
	req, _ := http.NewRequest("POST", srv.URL+"/ingest", &buf)
	req.Header.Set("Content-Encoding", "gzip")
	if got := post(t, srv.URL, req); got["ingested"] != 30 {
		t.Fatalf("ingest response %v", got)
	}
}

func TestQueryFilterAndRange(t *testing.T) {
	srv, st := newServer(t)
	req, _ := http.NewRequest("POST", srv.URL+"/ingest", strings.NewReader(body(60)))
	post(t, srv.URL, req)
	st.Flush()
	out := get(t, srv.URL+"/query?group_by=repo&type=WatchEvent&from=2024-01-01T10:00:00Z&to=2024-01-01T10:30:00Z", 200)
	// WatchEvent rows are i%4==0 with minute i: 0,4,...,28 -> 8 rows, all repo o/r0.
	if out["matched_rows"].(float64) != 8 {
		t.Fatalf("matched = %v", out["matched_rows"])
	}
}

func TestStats(t *testing.T) {
	srv, _ := newServer(t)
	req, _ := http.NewRequest("POST", srv.URL+"/ingest", strings.NewReader(body(10)))
	post(t, srv.URL, req)
	out := get(t, srv.URL+"/stats", 200)
	if out["total_rows"].(float64) != 10 {
		t.Fatalf("stats = %v", out)
	}
}

func TestQueryBadRequests(t *testing.T) {
	srv, _ := newServer(t)
	for _, q := range []string{
		"group_by=stars",
		"from=yesterday",
		"to=notatime",
		"limit=abc",
		"limit=-5",
		"type=PushEvent&repo=a/b",
		"from=100&to=50",
	} {
		t.Run(q, func(t *testing.T) {
			out := get(t, srv.URL+"/query?"+q, http.StatusBadRequest)
			if out["error"] == "" {
				t.Fatal("missing error message")
			}
		})
	}
}

func TestParseQueryAcceptsUnixSeconds(t *testing.T) {
	r := httptest.NewRequest("GET", "/query?from=1704103200&to=1704106800&actor=u1", nil)
	q, err := ParseQuery(r)
	if err != nil {
		t.Fatal(err)
	}
	if q.From != 1704103200 || q.To != 1704106800 || q.FilterCol != "actor" || q.FilterVal != "u1" || q.GroupBy != "repo" {
		t.Fatalf("parsed %+v", q)
	}
}

func TestWrongMethod(t *testing.T) {
	srv, _ := newServer(t)
	resp, err := http.Post(srv.URL+"/query", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
