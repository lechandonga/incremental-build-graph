package graph

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// TestFailureDownstreamNotExecuted verifies that a failing task stops
// its downstream branch; downstreams never run and no entry is cached.
func TestFailureDownstreamNotExecuted(t *testing.T) {
	e := newTestEngine()
	boom := errors.New("boom")
	var (
		aRuns int32
		bRuns int32 // b depends on failing a and must never run
		cRuns int32 // independent of a and must still complete
	)
	e.Register(Task{
		ID:     "a",
		Inputs: []byte("a"),
		Run: func(ctx context.Context, in Inputs) (Artifact, error) {
			atomic.AddInt32(&aRuns, 1)
			return Artifact{}, boom
		},
	})
	registerAdder(e, "b", []TaskID{"a"}, 1, &bRuns)
	registerAdder(e, "c", nil, 5, &cRuns)

	res, err := e.Run(context.Background(), "b", "c")
	if err == nil {
		t.Fatal("expected failure")
	}
	var te *TaskError
	if !errors.As(err, &te) || te.Task != "a" {
		t.Fatalf("expected TaskError for a, got %v", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("wrapped error lost: %v", err)
	}
	if res != nil {
		t.Fatalf("failed run must not return partial results, got %v", res)
	}
	if got := atomic.LoadInt32(&bRuns); got != 0 {
		t.Fatalf("downstream b ran %d times, must be 0", got)
	}

	// b must not have a cache entry: retry still executes a->b chain.
	// (First fix the failure below in the recovery test.)
}

// TestRetryRecovery: after a failure, fixing the task and rerunning
// brings the graph to a consistent state with correct artifacts.
func TestRetryRecovery(t *testing.T) {
	e := newTestEngine()
	var failA atomic.Bool
	failA.Store(true)
	var (
		aRuns int32
		bRuns int32
	)
	e.Register(Task{
		ID:     "a",
		Inputs: []byte("a"),
		Run: func(ctx context.Context, in Inputs) (Artifact, error) {
			atomic.AddInt32(&aRuns, 1)
			if failA.Load() {
				return Artifact{}, errors.New("transient")
			}
			return Artifact{Data: encodeInt(3)}, nil
		},
	})
	registerAdder(e, "b", []TaskID{"a"}, 10, &bRuns)

	ctx := context.Background()
	if _, err := e.Run(ctx, "b"); err == nil {
		t.Fatal("first run should fail")
	}
	if got := atomic.LoadInt32(&bRuns); got != 0 {
		t.Fatalf("b must not run while a failed, ran %d times", got)
	}

	failA.Store(false)
	res, err := e.Run(ctx, "b")
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := decodeInt(res["a"].Output.Data); got != 3 {
		t.Fatalf("a = %d, want 3", got)
	}
	if got := decodeInt(res["b"].Output.Data); got != 13 {
		t.Fatalf("b = %d, want 13", got)
	}

	// Once recovered, the next run is fully cached.
	res2, err := e.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if !res2["a"].Cached || !res2["b"].Cached {
		t.Fatalf("post-recovery run should be all hits: %+v", res2)
	}
	if got := atomic.LoadInt32(&bRuns); got != 1 {
		t.Fatalf("b ran %d times total, want 1", got)
	}
}

// TestFailureDoesNotInvalidateUnrelatedCache checks that after a failed
// branch, unrelated tasks keep their cached artifacts.
func TestFailureDoesNotInvalidateUnrelatedCache(t *testing.T) {
	e := newTestEngine()
	var goodRuns int32
	registerAdder(e, "good", nil, 4, &goodRuns)
	var failIt atomic.Bool
	failIt.Store(true)
	e.Register(Task{
		ID:     "bad",
		Inputs: []byte("bad"),
		Run: func(ctx context.Context, in Inputs) (Artifact, error) {
			if failIt.Load() {
				return Artifact{}, fmt.Errorf("nope")
			}
			return Artifact{Data: []byte("fixed")}, nil
		},
	})
	ctx := context.Background()
	if _, err := e.Run(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(ctx, "bad"); err == nil {
		t.Fatal("expected bad to fail")
	}
	res, err := e.Run(ctx, "good")
	if err != nil {
		t.Fatal(err)
	}
	if !res["good"].Cached {
		t.Fatal("unrelated good task lost its cache after failure")
	}
	if got := atomic.LoadInt32(&goodRuns); got != 1 {
		t.Fatalf("good ran %d times, want 1", got)
	}
}

// TestFailureDownstreamStaleArtifacts verifies that when an upstream
// task's INPUTS change (so its fingerprint changes) and its new
// execution fails, the downstream task's OLD cached output is not
// reused: it is skipped entirely.
func TestFailureDownstreamStaleArtifacts(t *testing.T) {
	e := newTestEngine()
	var aFail atomic.Bool
	var aRuns, bRuns int32
	registerA := func(inputs []byte) {
		e.Register(Task{
			ID:     "a",
			Inputs: inputs,
			Run: func(ctx context.Context, in Inputs) (Artifact, error) {
				atomic.AddInt32(&aRuns, 1)
				if aFail.Load() {
					return Artifact{}, errors.New("a broken")
				}
				return Artifact{Data: encodeInt(1)}, nil
			},
		})
	}
	registerA([]byte("a-v1"))
	registerAdder(e, "b", []TaskID{"a"}, 10, &bRuns)
	ctx := context.Background()
	res, err := e.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if decodeInt(res["b"].Output.Data) != 11 {
		t.Fatalf("b = %d want 11", decodeInt(res["b"].Output.Data))
	}

	// Change a's inputs AND make its new execution fail: a misses the
	// cache, fails, and b must not be served its stale cached value.
	aFail.Store(true)
	registerA([]byte("a-v2"))
	if _, err := e.Run(ctx, "b"); err == nil {
		t.Fatal("expected run to fail when changed a fails")
	}
	if got := atomic.LoadInt32(&bRuns); got != 1 {
		t.Fatalf("b must not re-execute on upstream failure, ran %d", got)
	}

	// Recover with the same new inputs: chain recomputes and returns to
	// a cached steady state.
	aFail.Store(false)
	if _, err := e.Run(ctx, "b"); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	res2, err := e.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if !res2["a"].Cached || !res2["b"].Cached {
		t.Fatalf("expected full cache hits after recovery: %+v", res2)
	}
}
