package segment

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Meetp369/ghstore/internal/event"
)

// rows: ts 100..109, types alternate Push/Watch, repo cycles a,b,c.
func rows() []event.Event {
	repos := []string{"a", "b", "c"}
	var evs []event.Event
	for i := 9; i >= 0; i-- { // written unsorted on purpose
		typ := "PushEvent"
		if i%2 == 1 {
			typ = "WatchEvent"
		}
		evs = append(evs, event.Event{TS: int64(100 + i), Type: typ, Repo: repos[i%3], Actor: "u"})
	}
	return evs
}

func write(t *testing.T) *Segment {
	t.Helper()
	s, err := Write(filepath.Join(t.TempDir(), "seg"), 7, 3, rows())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

const all = math.MaxInt64

func TestWriteSetsMeta(t *testing.T) {
	s := write(t)
	want := Meta{ID: 7, Count: 10, MinTS: 100, MaxTS: 109, WALID: 3}
	if s.Meta != want {
		t.Fatalf("meta = %+v, want %+v", s.Meta, want)
	}
}

func TestRowsSortedByTime(t *testing.T) {
	s := write(t)
	for i := 1; i < len(s.TS); i++ {
		if s.TS[i] < s.TS[i-1] {
			t.Fatalf("ts not sorted at %d: %v", i, s.TS)
		}
	}
}

func TestWriteDoesNotModifyInput(t *testing.T) {
	in := rows()
	orig := append([]event.Event(nil), in...)
	if _, err := Write(filepath.Join(t.TempDir(), "seg"), 1, 1, in); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, orig) {
		t.Fatal("Write reordered its input")
	}
}

func TestDictionaryDeduplicates(t *testing.T) {
	s := write(t)
	if got := len(s.Cols["repo"].Dict); got != 3 {
		t.Fatalf("repo dict has %d entries, want 3", got)
	}
	if got := len(s.Cols["actor"].Dict); got != 1 {
		t.Fatalf("actor dict has %d entries, want 1", got)
	}
}

func TestOpenRoundTrip(t *testing.T) {
	s := write(t)
	o, err := Open(s.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if o.Meta != s.Meta || !reflect.DeepEqual(o.TS, s.TS) || !reflect.DeepEqual(o.Cols, s.Cols) {
		t.Fatal("reopened segment differs from written one")
	}
}

func TestWriteEmptyFails(t *testing.T) {
	if _, err := Write(filepath.Join(t.TempDir(), "seg"), 1, 1, nil); err != ErrEmpty {
		t.Fatalf("err = %v, want ErrEmpty", err)
	}
}

func TestNoTempDirLeftBehind(t *testing.T) {
	s := write(t)
	if _, err := os.Stat(s.Dir + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temp dir still exists: %v", err)
	}
}

func TestGroupCount(t *testing.T) {
	s := write(t)
	tests := []struct {
		name      string
		group     string
		from, to  int64
		fcol, fv  string
		want      map[string]int64
		wantMatch int64
	}{
		{"all repos", "repo", 0, all, "", "", map[string]int64{"a": 4, "b": 3, "c": 3}, 10},
		{"all types", "type", 0, all, "", "", map[string]int64{"PushEvent": 5, "WatchEvent": 5}, 10},
		{"range inclusive start", "repo", 100, 103, "", "", map[string]int64{"a": 1, "b": 1, "c": 1}, 3},
		{"range exclusive end", "repo", 103, 104, "", "", map[string]int64{"a": 1}, 1},
		{"filter by type", "repo", 0, all, "type", "PushEvent", map[string]int64{"a": 2, "b": 1, "c": 2}, 5},
		{"filter + range", "repo", 104, 110, "type", "WatchEvent", map[string]int64{"b": 1, "c": 1, "a": 1}, 3},
		{"filter value absent", "repo", 0, all, "type", "ForkEvent", map[string]int64{}, 0},
		{"range before data", "repo", 0, 100, "", "", map[string]int64{}, 0},
		{"range after data", "repo", 110, all, "", "", map[string]int64{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := map[string]int64{}
			n, err := s.GroupCount(tt.group, tt.from, tt.to, tt.fcol, tt.fv, out)
			if err != nil {
				t.Fatal(err)
			}
			if n != tt.wantMatch || !reflect.DeepEqual(out, tt.want) {
				t.Fatalf("got %v (%d matched), want %v (%d)", out, n, tt.want, tt.wantMatch)
			}
		})
	}
}

func TestGroupCountUnknownColumn(t *testing.T) {
	s := write(t)
	if _, err := s.GroupCount("stars", 0, all, "", "", map[string]int64{}); err == nil {
		t.Fatal("expected error for unknown group column")
	}
	if _, err := s.GroupCount("repo", 0, all, "stars", "1", map[string]int64{}); err == nil {
		t.Fatal("expected error for unknown filter column")
	}
}

func TestOverlaps(t *testing.T) {
	s := write(t) // [100, 109]
	cases := []struct {
		from, to int64
		want     bool
	}{{0, 100, false}, {0, 101, true}, {109, 200, true}, {110, 200, false}, {105, 106, true}}
	for _, c := range cases {
		if got := s.Overlaps(c.from, c.to); got != c.want {
			t.Errorf("Overlaps(%d,%d) = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestOpenDetectsCorruptColumn(t *testing.T) {
	s := write(t)
	os.WriteFile(filepath.Join(s.Dir, "repo.col"), []byte{1, 2, 3}, 0o644)
	if _, err := Open(s.Dir); err == nil {
		t.Fatal("expected error for corrupt column file")
	}
}

func TestOpenDetectsShortTS(t *testing.T) {
	s := write(t)
	os.WriteFile(filepath.Join(s.Dir, "ts.col"), make([]byte, 16), 0o644)
	if _, err := Open(s.Dir); err == nil {
		t.Fatal("expected error for short ts column")
	}
}
