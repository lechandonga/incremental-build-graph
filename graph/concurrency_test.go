package graph

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestIndependentTasksRunConcurrently verifies that tasks without a
// dependency relationship are scheduled in parallel.
func TestIndependentTasksRunConcurrently(t *testing.T) {
	e := newTestEngine()
	var inFlight, maxInFlight int32
	const n = 20
	for i := 0; i < n; i++ {
		id := TaskID(fmt.Sprintf("t%d", i))
		e.Register(Task{
			ID:     id,
			Inputs: []byte(id),
			Run: func(ctx context.Context, in Inputs) (Artifact, error) {
				cur := atomic.AddInt32(&inFlight, 1)
				for {
					old := atomic.LoadInt32(&maxInFlight)
					if cur <= old || atomic.CompareAndSwapInt32(&maxInFlight, old, cur) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
				atomic.AddInt32(&inFlight, -1)
				return Artifact{Data: []byte("ok")}, nil
			},
		})
	}
	targets := make([]TaskID, n)
	for i := range targets {
		targets[i] = TaskID(fmt.Sprintf("t%d", i))
	}
	if _, err := e.Run(context.Background(), targets...); err != nil {
		t.Fatal(err)
	}
	if max := atomic.LoadInt32(&maxInFlight); max < 2 {
		t.Fatalf("independent tasks did not run concurrently, max in flight = %d", max)
	}
}

// TestConcurrentTriggersConsistent fires many identical Run calls at
// once against a cold cache. Every task must execute exactly once and
// every caller must observe identical, complete results.
func TestConcurrentTriggersConsistent(t *testing.T) {
	e := newTestEngine()
	var runs int32
	// Diamond with small artificial delays to widen the race window.
	mk := func(id TaskID, deps []TaskID, v int) Task {
		return Task{
			ID:     id,
			Deps:   deps,
			Inputs: encodeInt(v),
			Run: func(ctx context.Context, in Inputs) (Artifact, error) {
				time.Sleep(time.Duration(v%3) * time.Millisecond)
				atomic.AddInt32(&runs, 1)
				sum := v
				for _, d := range deps {
					a, _ := in.Artifact(d)
					sum += decodeInt(a.Data)
				}
				return Artifact{Data: encodeInt(sum)}, nil
			},
		}
	}
	e.Register(mk("a", nil, 1))
	e.Register(mk("b", nil, 2))
	e.Register(mk("c", []TaskID{"a", "b"}, 10))

	const callers = 32
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	results := make([]map[TaskID]Result, callers)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := e.Run(context.Background(), "c")
			if err != nil {
				errs <- err
				return
			}
			results[i] = res
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent run error: %v", err)
	}
	if got := atomic.LoadInt32(&runs); got != 3 {
		t.Fatalf("tasks executed %d times total, want exactly 3", got)
	}
	for i, res := range results {
		if res == nil {
			t.Fatalf("caller %d got nil results", i)
		}
		if decodeInt(res["c"].Output.Data) != 13 {
			t.Fatalf("caller %d got c=%d, want 13", i, decodeInt(res["c"].Output.Data))
		}
		for _, id := range []TaskID{"a", "b", "c"} {
			if _, ok := res[id]; !ok {
				t.Fatalf("caller %d missing result %s", i, id)
			}
		}
	}
}

// TestDependentTasksNeverOverlap ensures a task is never scheduled
// before all its dependencies complete.
func TestDependencyOrdering(t *testing.T) {
	e := newTestEngine()
	var orderMu sync.Mutex
	var finished []string
	mk := func(id TaskID, deps []TaskID) Task {
		return Task{
			ID:     id,
			Deps:   deps,
			Inputs: []byte(id),
			Run: func(ctx context.Context, in Inputs) (Artifact, error) {
				for _, d := range deps {
					if _, ok := in.Artifact(d); !ok {
						return Artifact{}, fmt.Errorf("%s started before %s", id, d)
					}
				}
				orderMu.Lock()
				finished = append(finished, string(id))
				orderMu.Unlock()
				return Artifact{Data: []byte(id)}, nil
			},
		}
	}
	e.Register(mk("a", nil))
	e.Register(mk("b", []TaskID{"a"}))
	e.Register(mk("c", []TaskID{"a"}))
	e.Register(mk("d", []TaskID{"b", "c"}))
	if _, err := e.Run(context.Background(), "d"); err != nil {
		t.Fatal(err)
	}
}
