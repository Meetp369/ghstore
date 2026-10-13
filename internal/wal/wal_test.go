package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Meetp369/ghstore/internal/event"
)

func batch(start, n int) []event.Event {
	evs := make([]event.Event, n)
	for i := range evs {
		evs[i] = event.Event{TS: int64(start + i), Type: "PushEvent", Repo: fmt.Sprintf("r/%d", start+i), Actor: "a"}
	}
	return evs
}

func writeBatches(t *testing.T, path string, batches ...[]event.Event) {
	t.Helper()
	w, err := Create(path, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range batches {
		if err := w.Append(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.log")
	writeBatches(t, path, batch(0, 3), batch(3, 5))
	res, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if res.Batches != 2 || len(res.Events) != 8 || res.Truncated {
		t.Fatalf("got %d batches, %d events, truncated=%v", res.Batches, len(res.Events), res.Truncated)
	}
	for i, e := range res.Events {
		if e.TS != int64(i) {
			t.Fatalf("event %d has ts %d", i, e.TS)
		}
	}
}

func TestEmptyBatchIsNoop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.log")
	writeBatches(t, path, nil, batch(0, 1), []event.Event{})
	res, err := Replay(path)
	if err != nil || res.Batches != 1 {
		t.Fatalf("batches=%d err=%v", res.Batches, err)
	}
}

func TestReplayEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.log")
	writeBatches(t, path)
	res, err := Replay(path)
	if err != nil || len(res.Events) != 0 || res.Truncated {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestReplayMissingFile(t *testing.T) {
	if _, err := Replay(filepath.Join(t.TempDir(), "nope.log")); err == nil {
		t.Fatal("expected error")
	}
}

func TestReopenAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.log")
	writeBatches(t, path, batch(0, 2))
	writeBatches(t, path, batch(2, 2))
	res, _ := Replay(path)
	if len(res.Events) != 4 {
		t.Fatalf("got %d events after reopen", len(res.Events))
	}
}

// A crash mid-write leaves a partial record at the tail. Replay must keep
// every complete batch and drop the partial one, at every possible cut.
func TestTornTailAtEveryOffset(t *testing.T) {
	dir := t.TempDir()
	full := filepath.Join(dir, "full.log")
	writeBatches(t, full, batch(0, 4), batch(4, 4))
	data, err := os.ReadFile(full)
	if err != nil {
		t.Fatal(err)
	}
	first := len(data) / 2 // batches are the same size
	for cut := 0; cut < len(data); cut++ {
		p := filepath.Join(dir, fmt.Sprintf("cut-%d.log", cut))
		if err := os.WriteFile(p, data[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		res, err := Replay(p)
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		want := 0
		if cut >= first {
			want = 4
		}
		if len(res.Events) != want {
			t.Fatalf("cut %d: got %d events, want %d", cut, len(res.Events), want)
		}
		if cut != 0 && cut != first && !res.Truncated {
			t.Fatalf("cut %d: torn tail not reported", cut)
		}
	}
}

func TestCorruptPayloadStopsReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.log")
	writeBatches(t, path, batch(0, 4), batch(4, 4))
	data, _ := os.ReadFile(path)
	data[len(data)-3] ^= 0xFF // flip bits inside the second batch
	os.WriteFile(path, data, 0o644)
	res, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Events) != 4 || !res.Truncated {
		t.Fatalf("got %d events, truncated=%v; want 4, true", len(res.Events), res.Truncated)
	}
}

func TestCorruptLengthStopsReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.log")
	writeBatches(t, path, batch(0, 2))
	data, _ := os.ReadFile(path)
	data[0], data[1], data[2], data[3] = 0xFF, 0xFF, 0xFF, 0x7F
	os.WriteFile(path, data, 0o644)
	res, err := Replay(path)
	if err != nil || len(res.Events) != 0 || !res.Truncated {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestLargeBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.log")
	writeBatches(t, path, batch(0, 50_000))
	res, err := Replay(path)
	if err != nil || len(res.Events) != 50_000 {
		t.Fatalf("got %d events, err %v", len(res.Events), err)
	}
}
