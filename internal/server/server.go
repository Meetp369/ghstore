// Package server exposes a store over HTTP.
//
//	GET  /query?group_by=repo&type=PushEvent&from=...&to=...&limit=10
//	GET  /stats
//	POST /ingest      body: newline-delimited GH Archive events (gzip ok)
//	GET  /healthz
//
// from/to accept RFC 3339 timestamps or Unix seconds.
package server

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Meetp369/ghstore/internal/event"
	"github.com/Meetp369/ghstore/internal/ingest"
	"github.com/Meetp369/ghstore/internal/store"
)

// New returns an http.Handler serving st.
func New(st *store.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, st.Stats())
	})
	mux.HandleFunc("GET /query", func(w http.ResponseWriter, r *http.Request) {
		q, err := ParseQuery(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		start := time.Now()
		res, err := st.Query(q)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, struct {
			store.Result
			TookMS float64 `json:"took_ms"`
		}{res, float64(time.Since(start).Microseconds()) / 1000})
	})
	mux.HandleFunc("POST /ingest", func(w http.ResponseWriter, r *http.Request) {
		body := io.Reader(r.Body)
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			defer zr.Close()
			body = zr
		}
		stats, err := ingest.Reader(r.Context(), st, body, ingest.Options{})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]int64{"ingested": stats.Events, "skipped": stats.Skipped})
	})
	return mux
}

// ParseQuery builds a store.Query from URL parameters.
func ParseQuery(r *http.Request) (store.Query, error) {
	v := r.URL.Query()
	q := store.Query{GroupBy: v.Get("group_by")}
	if q.GroupBy == "" {
		q.GroupBy = "repo"
	}
	if !event.ValidColumn(q.GroupBy) {
		return q, fmt.Errorf("group_by must be one of %v", event.Columns)
	}
	for _, col := range event.Columns {
		if val := v.Get(col); val != "" {
			if q.FilterCol != "" {
				return q, fmt.Errorf("only one filter is supported")
			}
			q.FilterCol, q.FilterVal = col, val
		}
	}
	var err error
	if q.From, err = parseTime(v.Get("from")); err != nil {
		return q, fmt.Errorf("from: %w", err)
	}
	if q.To, err = parseTime(v.Get("to")); err != nil {
		return q, fmt.Errorf("to: %w", err)
	}
	if s := v.Get("limit"); s != "" {
		if q.Limit, err = strconv.Atoi(s); err != nil || q.Limit < -1 {
			return q, fmt.Errorf("limit: must be an integer >= -1")
		}
	}
	return q, nil
}

func parseTime(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, fmt.Errorf("want RFC 3339 or Unix seconds, got %q", s)
	}
	return t.Unix(), nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
