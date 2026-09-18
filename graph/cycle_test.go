package graph

import (
	"context"
	"sync/atomic"
	"testing"
)

func noopTask(id TaskID, deps []TaskID) Task {
	return Task{
		ID:     id,
		Deps:   deps,
		Inputs: []byte(id),
		Run: func(ctx context.Context, in Inputs) (Artifact, error) {
			return Artifact{Data: []byte(id)}, nil
		},
	}
}

func TestCycleDetection(t *testing.T) {
	e := newTestEngine()
	e.Register(noopTask("a", []TaskID{"b"}))
	e.Register(noopTask("b", []TaskID{"c"}))
	e.Register(noopTask("c", []TaskID{"a"}))

	_, err := e.Run(context.Background(), "a")
	ce, ok := AsCycleError(err)
	if !ok {
		t.Fatalf("expected *CycleError, got %v", err)
	}
	if len(ce.Cycle) != 3 {
		t.Fatalf("cycle length = %d, want 3: %v", len(ce.Cycle), ce.Cycle)
	}
	// The reported cycle must actually be a cycle in the graph.
	byID := map[TaskID][]TaskID{
		"a": {"b"}, "b": {"c"}, "c": {"a"},
	}
	for i, node := range ce.Cycle {
		next := ce.Cycle[(i+1)%len(ce.Cycle)]
		found := false
		for _, d := range byID[node] {
			if d == next {
				found = true
			}
		}
		if !found {
			t.Fatalf("reported cycle not valid: %v", ce.Cycle)
		}
	}

	// Validate reports the same cycle.
	if _, ok := AsCycleError(e.Validate()); !ok {
		t.Fatalf("Validate should report cycle, got %v", e.Validate())
	}
}

func TestCycleDoesNotCorruptState(t *testing.T) {
	e := newTestEngine()
	var goodRuns int32
	registerAdder(e, "good", nil, 7, &goodRuns)
	// Cyclic group unrelated to "good".
	e.Register(noopTask("x", []TaskID{"y"}))
	e.Register(noopTask("y", []TaskID{"x"}))

	ctx := context.Background()
	// Succeed first to populate the cache for "good".
	if _, err := e.Run(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	// A cyclic run must fail.
	if _, err := e.Run(ctx, "x"); err == nil {
		t.Fatal("expected cycle error")
	}
	// Running the good target again must still be a clean cache hit;
	// the failed cycle run neither dropped nor dirtied its entry.
	res, err := e.Run(ctx, "good")
	if err != nil {
		t.Fatalf("good run after cycle: %v", err)
	}
	if !res["good"].Cached {
		t.Fatal("good must still be cached after a cycle elsewhere")
	}
	if runs := atomic.LoadInt32(&goodRuns); runs != 1 {
		t.Fatalf("good ran %d times, want 1", runs)
	}
	// The graph itself remains usable: Validate still reports only the
	// real cycle, nothing else got corrupted.
	if _, ok := AsCycleError(e.Validate()); !ok {
		t.Fatal("Validate must still report the cycle")
	}
}

func TestSelfCycle(t *testing.T) {
	e := newTestEngine()
	e.Register(noopTask("s", []TaskID{"s"}))
	_, err := e.Run(context.Background(), "s")
	ce, ok := AsCycleError(err)
	if !ok {
		t.Fatalf("expected self cycle, got %v", err)
	}
	if len(ce.Cycle) != 1 || ce.Cycle[0] != "s" {
		t.Fatalf("unexpected cycle: %v", ce.Cycle)
	}
}
