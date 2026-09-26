package graph

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// byteTask registers a task whose Inputs (config) and emitted bytes are
// controlled independently, so tests can change the config while keeping
// the output byte-identical.
func registerByteTask(e *Engine, id TaskID, deps []TaskID, inputs []byte, output []byte, runs *int32) {
	e.Register(Task{
		ID:     id,
		Deps:   deps,
		Inputs: inputs,
		Run: func(ctx context.Context, in Inputs) (Artifact, error) {
			atomic.AddInt32(runs, 1)
			for _, d := range deps {
				if _, ok := in.Artifact(d); !ok {
					return Artifact{}, fmt.Errorf("missing dep %s", d)
				}
			}
			return Artifact{Data: output}, nil
		},
	})
}

// TestUpstreamRecomputedSameBytesDownstreamHits: when an upstream task's
// inputs/config change it is recomputed, but if the emitted bytes are
// identical to last time, downstream tasks must keep their cache hits
// and must not execute.
func TestUpstreamRecomputedSameBytesDownstreamHits(t *testing.T) {
	e := newTestEngine()
	var aRuns, bRuns, cRuns int32
	registerByteTask(e, "a", nil, []byte("cfg-v1"), []byte("A"), &aRuns)
	registerByteTask(e, "b", []TaskID{"a"}, []byte("b"), []byte("B"), &bRuns)
	registerByteTask(e, "c", []TaskID{"b"}, []byte("c"), []byte("C"), &cRuns)

	ctx := context.Background()
	if _, err := e.Run(ctx, "c"); err != nil {
		t.Fatal(err)
	}

	// Change a's config only; a's output bytes stay "A".
	registerByteTask(e, "a", nil, []byte("cfg-v2"), []byte("A"), &aRuns)
	res, err := e.Run(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if res["a"].Cached {
		t.Error("a inputs changed: a must recompute")
	}
	if !res["b"].Cached || !res["c"].Cached {
		t.Errorf("downstream must stay cached when upstream bytes are unchanged: %+v", res)
	}
	if got := atomic.LoadInt32(&aRuns); got != 2 {
		t.Errorf("a ran %d times, want 2", got)
	}
	if got := atomic.LoadInt32(&bRuns); got != 1 {
		t.Errorf("b ran %d times, want 1 (only upstream actually executed)", got)
	}
	if got := atomic.LoadInt32(&cRuns); got != 1 {
		t.Errorf("c ran %d times, want 1", got)
	}
}

// TestUpstreamBytesChangeInvalidatesDownstream: when the consumed
// upstream artifact bytes really change, every downstream must be
// invalidated and recomputed — never served stale output.
func TestUpstreamBytesChangeInvalidatesDownstream(t *testing.T) {
	e := newTestEngine()
	var aRuns, bRuns int32
	registerByteTask(e, "a", nil, []byte("cfg-v1"), []byte("A"), &aRuns)
	registerByteTask(e, "b", []TaskID{"a"}, []byte("b"), []byte("B"), &bRuns)

	ctx := context.Background()
	if _, err := e.Run(ctx, "b"); err != nil {
		t.Fatal(err)
	}

	// a's config AND output bytes change.
	registerByteTask(e, "a", nil, []byte("cfg-v2"), []byte("A2"), &aRuns)
	res, err := e.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if res["a"].Cached || res["b"].Cached {
		t.Errorf("upstream bytes changed: a and b must recompute: %+v", res)
	}
	if got := atomic.LoadInt32(&bRuns); got != 2 {
		t.Errorf("b ran %d times, want 2", got)
	}
}

// TestContentBasedHitAcrossRestart: the content-based hit rule must hold
// after a process restart with a persistent store.
func TestContentBasedHitAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	e := NewEngine(WithStore(mustFileStore(t, dir)))
	var aRuns, bRuns int32
	registerByteTask(e, "a", nil, []byte("cfg-v1"), []byte("A"), &aRuns)
	registerByteTask(e, "b", []TaskID{"a"}, []byte("b"), []byte("B"), &bRuns)
	if _, err := e.Run(ctx, "b"); err != nil {
		t.Fatal(err)
	}

	// Restart: new engine over the same directory; a's config changed
	// but its output bytes are identical.
	e2 := NewEngine(WithStore(mustFileStore(t, dir)))
	var aRuns2, bRuns2 int32
	registerByteTask(e2, "a", nil, []byte("cfg-v2"), []byte("A"), &aRuns2)
	registerByteTask(e2, "b", []TaskID{"a"}, []byte("b"), []byte("B"), &bRuns2)
	res, err := e2.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if res["a"].Cached {
		t.Error("a config changed: a must recompute after restart")
	}
	if !res["b"].Cached {
		t.Error("b must hit cache after restart: upstream bytes unchanged")
	}
	if got := atomic.LoadInt32(&aRuns2); got != 1 {
		t.Errorf("a ran %d times after restart, want 1", got)
	}
	if got := atomic.LoadInt32(&bRuns2); got != 0 {
		t.Errorf("b ran %d times after restart, want 0", got)
	}
}
