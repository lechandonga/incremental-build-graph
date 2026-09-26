package graph

import (
	"context"
	"sync/atomic"
	"testing"
)

// registerConstantTask registers a task whose own Inputs differ per
// registration (so its content fingerprint changes and it must
// recompute) but whose produced bytes are fixed, regardless of those
// inputs. Used to prove downstream hits depend on artifact BYTES, not
// on whether the upstream recomputed.
func registerConstantTask(e *Engine, id TaskID, deps []TaskID, inputs []byte, out []byte, runs *int32) {
	e.Register(Task{
		ID:     id,
		Deps:   deps,
		Inputs: inputs,
		Run: func(ctx context.Context, in Inputs) (Artifact, error) {
			atomic.AddInt32(runs, 1)
			return Artifact{Data: append([]byte(nil), out...)}, nil
		},
	})
}

// passthroughTask returns the exact bytes of its (single) upstream.
func registerPassthrough(e *Engine, id TaskID, dep TaskID, runs *int32) {
	e.Register(Task{
		ID:     id,
		Deps:   []TaskID{dep},
		Inputs: []byte(id + "-cfg"),
		Run: func(ctx context.Context, in Inputs) (Artifact, error) {
			atomic.AddInt32(runs, 1)
			a, _ := in.Artifact(dep)
			return Artifact{Data: append([]byte(nil), a.Data...)}, nil
		},
	})
}

// TestUpstreamRecomputeSameBytesDownstreamHit: the upstream's inputs
// change (so it genuinely recomputes) but it emits identical bytes; the
// downstream must stay cached and never execute.
func TestUpstreamRecomputeSameBytesDownstreamHit(t *testing.T) {
	e := newTestEngine()
	var aRuns, bRuns int32
	ctx := context.Background()

	registerConstantTask(e, "a", nil, []byte("a-cfg-v1"), []byte("A"), &aRuns)
	registerPassthrough(e, "b", "a", &bRuns)

	res, err := e.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if res["a"].Cached || res["b"].Cached {
		t.Fatalf("first run must execute both: %+v", res)
	}

	// Change a's own inputs/config; its produced bytes stay "A".
	registerConstantTask(e, "a", nil, []byte("a-cfg-v2"), []byte("A"), &aRuns)
	res, err = e.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if res["a"].Cached {
		t.Error("a must recompute because its inputs changed")
	}
	if !res["b"].Cached {
		t.Errorf("b must stay cached: a recomputed but its bytes are unchanged; %+v", res)
	}
	if got := atomic.LoadInt32(&aRuns); got != 2 {
		t.Errorf("a ran %d times, want 2", got)
	}
	if got := atomic.LoadInt32(&bRuns); got != 1 {
		t.Errorf("b ran %d times, want 1 (must not re-execute)", got)
	}
	if string(res["b"].Output.Data) != "A" {
		t.Errorf("b output = %q, want A", res["b"].Output.Data)
	}
}

// registerEchoTask produces bytes derived directly from its declared
// Inputs, so changing the inputs necessarily changes both its content
// fingerprint and its produced bytes.
func registerEchoTask(e *Engine, id TaskID, inputs []byte, runs *int32) {
	e.Register(Task{
		ID:     id,
		Inputs: append([]byte(nil), inputs...),
		Run: func(ctx context.Context, in Inputs) (Artifact, error) {
			atomic.AddInt32(runs, 1)
			return Artifact{Data: append([]byte(nil), inputs...)}, nil
		},
	})
}

// TestUpstreamBytesChangeInvalidatesDownstream: when a declared input
// change makes the upstream's produced bytes actually change, every
// downstream that consumed them must miss and recompute, and must never
// observe the old artifact.
func TestUpstreamBytesChangeInvalidatesDownstream(t *testing.T) {
	e := newTestEngine()
	var aRuns, bRuns int32
	ctx := context.Background()

	registerEchoTask(e, "a", []byte("A-v1"), &aRuns)
	registerPassthrough(e, "b", "a", &bRuns)

	res, err := e.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if string(res["b"].Output.Data) != "A-v1" {
		t.Fatalf("b = %q, want A-v1", res["b"].Output.Data)
	}

	// A declared input change: a recomputes and its bytes change.
	registerEchoTask(e, "a", []byte("A-v2"), &aRuns)
	res, err = e.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if res["a"].Cached || res["b"].Cached {
		t.Errorf("both must recompute after upstream bytes change: %+v", res)
	}
	if string(res["b"].Output.Data) != "A-v2" {
		t.Errorf("b must reflect new upstream bytes, got %q", res["b"].Output.Data)
	}
	if got := atomic.LoadInt32(&bRuns); got != 2 {
		t.Errorf("b ran %d times, want 2", got)
	}
}

// TestUpstreamSameBytesDownstreamHitAcrossRestart proves the
// byte-based hit survives a process restart: a recomputes after restart
// (different inputs, identical bytes) while b is served from the
// persisted entry without executing.
func TestUpstreamSameBytesDownstreamHitAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	var aRuns1, bRuns1 int32
	e1 := NewEngine(WithStore(mustFileStore(t, dir)))
	registerConstantTask(e1, "a", nil, []byte("cfg-v1"), []byte("A"), &aRuns1)
	registerPassthrough(e1, "b", "a", &bRuns1)
	if _, err := e1.Run(ctx, "b"); err != nil {
		t.Fatal(err)
	}

	// Restart: a's config changed (it must recompute) but its bytes are
	// still "A"; b must hit the persisted cache.
	var aRuns2, bRuns2 int32
	e2 := NewEngine(WithStore(mustFileStore(t, dir)))
	registerConstantTask(e2, "a", nil, []byte("cfg-v2"), []byte("A"), &aRuns2)
	registerPassthrough(e2, "b", "a", &bRuns2)
	res, err := e2.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if res["a"].Cached {
		t.Error("a must recompute after its config changed")
	}
	if !res["b"].Cached {
		t.Errorf("b must hit persisted cache on unchanged upstream bytes: %+v", res)
	}
	if atomic.LoadInt32(&aRuns2) != 1 {
		t.Error("a should execute once after restart")
	}
	if atomic.LoadInt32(&bRuns2) != 0 {
		t.Errorf("b should not execute after restart, ran %d times", atomic.LoadInt32(&bRuns2))
	}
	if string(res["b"].Output.Data) != "A" {
		t.Errorf("b = %q, want A", res["b"].Output.Data)
	}
}
