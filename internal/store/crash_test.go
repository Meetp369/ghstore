package store

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestKill9Recovery runs a real writer process, SIGKILLs it at a random
// moment (mid-append, mid-flush, anywhere), reopens the store, and checks
// that every batch the writer had acknowledged is present exactly once.
//
// Iterations default to 5; set GHSTORE_CRASH_ITERS to run more, e.g.
//
//	GHSTORE_CRASH_ITERS=20 go test -run TestKill9Recovery -v ./internal/store
func TestKill9Recovery(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	iters := 5
	if v, err := strconv.Atoi(os.Getenv("GHSTORE_CRASH_ITERS")); err == nil && v > 0 {
		iters = v
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < iters; i++ {
		t.Run(fmt.Sprintf("iter%02d", i), func(t *testing.T) {
			dir := t.TempDir()
			killAfter := 5 + rng.Intn(60)
			acked := runAndKill(t, dir, killAfter)

			s, err := Open(Options{Dir: dir, FlushThreshold: 1000, SyncWAL: true})
			if err != nil {
				t.Fatalf("reopen after kill: %v", err)
			}
			defer s.Close()
			res, err := s.Query(Query{GroupBy: "actor", Limit: -1})
			if err != nil {
				t.Fatal(err)
			}
			seen := make(map[string]int64, len(res.Groups))
			for _, g := range res.Groups {
				if g.Count != 1 {
					t.Fatalf("actor %s present %d times (duplicate after recovery)", g.Key, g.Count)
				}
				seen[g.Key] = g.Count
			}
			for k := 0; k < acked; k++ {
				if seen[fmt.Sprintf("u%d", k)] != 1 {
					t.Fatalf("acknowledged row u%d lost (acked %d rows, recovered %d)", k, acked, len(seen))
				}
			}
			t.Logf("killed after %d acked rows; recovered %d rows, %+v", acked, len(seen), s.Recovery())
		})
	}
}

const batchRows = 100

// runAndKill starts the child writer and kills it once it has acknowledged
// killAfter batches. It returns the number of acknowledged rows.
func runAndKill(t *testing.T, dir string, killAfter int) int {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashChild$")
	cmd.Env = append(os.Environ(), "GHSTORE_CRASH_CHILD="+dir)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(out)
	acked := 0
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "acked ") {
			continue
		}
		acked, _ = strconv.Atoi(strings.TrimPrefix(line, "acked "))
		if acked >= killAfter*batchRows {
			break
		}
	}
	// The child keeps writing until the signal lands, so more rows than
	// "acked" may be on disk; those may or may not survive. Only rows we
	// saw acknowledged are guaranteed.
	cmd.Process.Kill() // SIGKILL: no deferred cleanup, no final flush
	cmd.Wait()
	if acked == 0 {
		t.Fatal("child acknowledged nothing")
	}
	return acked
}

// TestCrashChild is the writer process for TestKill9Recovery. It does
// nothing unless GHSTORE_CRASH_CHILD is set.
func TestCrashChild(t *testing.T) {
	dir := os.Getenv("GHSTORE_CRASH_CHILD")
	if dir == "" {
		t.Skip("helper process")
	}
	// Small threshold so kills often land during a flush.
	s, err := Open(Options{Dir: dir, FlushThreshold: 1000, SyncWAL: true})
	if err != nil {
		fmt.Println("open error:", err)
		os.Exit(1)
	}
	w := bufio.NewWriter(os.Stdout)
	for b := 0; ; b++ {
		if err := s.Append(gen(b*batchRows, batchRows)); err != nil {
			fmt.Println("append error:", err)
			os.Exit(1)
		}
		// Printed only after Append (and its fsync) returned.
		fmt.Fprintf(w, "acked %d\n", (b+1)*batchRows)
		w.Flush()
	}
}
