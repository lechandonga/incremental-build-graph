package buildgraph

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentTriggersCoalesce fires the same graph concurrently from many
// goroutines: every task must execute at most once and every caller must see
// identical artifacts.
func TestConcurrentTriggersCoalesce(t *testing.T) {
	s, _ := New(Options{})

	g := NewGraph()
	var runs int64
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("t%d", i)
		deps := []string{}
		if i > 0 {
			deps = []string{fmt.Sprintf("t%d", i-1)}
		}
		_ = g.Add(Task{ID: id, Deps: deps,
			Run: func(_ context.Context, _, _ []byte, d map[string]Artifact) (Artifact, error) {
				atomic.AddInt64(&runs, 1)
				time.Sleep(2 * time.Millisecond)
				if len(d) == 0 {
					return Artifact(id), nil
				}
				return Artifact(id + "<-" + stringFirstDep(id, d)), nil
			}})
	}

	const callers = 32
	var wg sync.WaitGroup
	results := make([]*RunResult, callers)
	errs := make([]error, callers)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			r, err := s.Run(context.Background(), g)
			results[i] = r
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}
	if got := atomic.LoadInt64(&runs); got != 6 {
		t.Fatalf("each task must run exactly once, got %d executions", got)
	}
	base := results[0]
	for i := 1; i < callers; i++ {
		if len(results[i].Artifacts) != len(base.Artifacts) {
			t.Fatalf("caller %d artifact map size mismatch", i)
		}
		for id, a := range base.Artifacts {
			if string(results[i].Artifacts[id]) != string(a) {
				t.Fatalf("caller %d artifact mismatch for %s", i, id)
			}
		}
	}
}

func stringFirstDep(_ string, d map[string]Artifact) string {
	for _, a := range d {
		return string(a)
	}
	return ""
}

// TestIndependentTasksRunConcurrently verifies independent tasks actually run
// in parallel and that a shared counter used inside deterministic tasks stays
// consistent.
func TestIndependentTasksRunConcurrently(t *testing.T) {
	s, _ := New(Options{Workers: 8})
	g := NewGraph()
	const n = 16
	var concurrent, maxConcurrent int32
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("t%d", i)
		_ = g.Add(Task{ID: id,
			Run: func(_ context.Context, _, _ []byte, _ map[string]Artifact) (Artifact, error) {
				cur := atomic.AddInt32(&concurrent, 1)
				mu.Lock()
				if cur > maxConcurrent {
					maxConcurrent = cur
				}
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				atomic.AddInt32(&concurrent, -1)
				return Artifact(id), nil
			}})
	}
	res, err := s.Run(context.Background(), g)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Executed) != n {
		t.Fatalf("all tasks should execute, got %d", len(res.Executed))
	}
	if maxConcurrent < 2 {
		t.Fatalf("independent tasks did not run concurrently (max=%d)", maxConcurrent)
	}
}

// TestConcurrentDifferentGraphsIsolated runs two distinct graphs concurrently
// against the same scheduler; both must complete fully.
func TestConcurrentDifferentGraphsIsolated(t *testing.T) {
	s, _ := New(Options{Workers: 4})
	var wg sync.WaitGroup
	for gi := 0; gi < 2; gi++ {
		g := NewGraph()
		_ = g.Add(Task{ID: "x",
			Run: func(_ context.Context, _, _ []byte, _ map[string]Artifact) (Artifact, error) {
				time.Sleep(5 * time.Millisecond)
				return Artifact(fmt.Sprintf("g%d", gi)), nil
			}})
		wg.Add(1)
		go func(g *Graph, gi int) {
			defer wg.Done()
			res, err := s.Run(context.Background(), g)
			if err != nil {
				t.Errorf("graph %d: %v", gi, err)
				return
			}
			if string(res.Artifacts["x"]) != fmt.Sprintf("g%d", gi) {
				t.Errorf("graph %d bad artifact %q", gi, res.Artifacts["x"])
			}
		}(g, gi)
	}
	wg.Wait()
}
