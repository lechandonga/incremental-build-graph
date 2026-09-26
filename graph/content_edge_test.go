package graph

import (
	"context"
	"sync/atomic"
	"testing"
)

// TestUpstreamBytesChangeAcrossRestart: after restart, a declared input
// change makes the upstream bytes change; the downstream persisted
// entry must NOT be reused.
func TestUpstreamBytesChangeAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	var a1, b1 int32
	e1 := NewEngine(WithStore(mustFileStore(t, dir)))
	registerEchoTask(e1, "a", []byte("v1"), &a1)
	registerPassthrough(e1, "b", "a", &b1)
	if _, err := e1.Run(ctx, "b"); err != nil {
		t.Fatal(err)
	}

	var a2, b2 int32
	e2 := NewEngine(WithStore(mustFileStore(t, dir)))
	registerEchoTask(e2, "a", []byte("v2"), &a2)
	registerPassthrough(e2, "b", "a", &b2)
	res, err := e2.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if res["a"].Cached || res["b"].Cached {
		t.Fatalf("both must recompute across restart when bytes change: %+v", res)
	}
	if string(res["b"].Output.Data) != "v2" {
		t.Fatalf("stale artifact reused: b=%q want v2", res["b"].Output.Data)
	}
	if atomic.LoadInt32(&b2) != 1 {
		t.Fatal("b must execute after restart")
	}
}

// registerSelectiveTask declares two deps but only reads one. The
// unread upstream changing must not invalidate this task; the read
// upstream changing must.
func registerSelectiveTask(e *Engine, id TaskID, read TaskID, ignore TaskID, runs *int32) {
	e.Register(Task{
		ID:     id,
		Deps:   []TaskID{read, ignore},
		Inputs: []byte("sel"),
		Run: func(ctx context.Context, in Inputs) (Artifact, error) {
			atomic.AddInt32(runs, 1)
			a, _ := in.Artifact(read)
			// Deliberately never read "ignore".
			return Artifact{Data: append(append([]byte(nil), a.Data...), '!')}, nil
		},
	})
}

func TestOnlyConsumedUpstreamsMatter(t *testing.T) {
	e := newTestEngine()
	var readRuns, ignoreRuns, selRuns int32
	ctx := context.Background()
	registerEchoTask(e, "read", []byte("R"), &readRuns)
	registerEchoTask(e, "ignore", []byte("G"), &ignoreRuns)
	registerSelectiveTask(e, "sel", "read", "ignore", &selRuns)
	if _, err := e.Run(ctx, "sel"); err != nil {
		t.Fatal(err)
	}

	// Change only the ignored upstream: sel must stay cached.
	registerEchoTask(e, "ignore", []byte("G2"), &ignoreRuns)
	res, err := e.Run(ctx, "sel")
	if err != nil {
		t.Fatal(err)
	}
	if !res["sel"].Cached {
		t.Errorf("sel must stay cached when an unread upstream changes: %+v", res)
	}
	if atomic.LoadInt32(&selRuns) != 1 {
		t.Errorf("sel ran %d times, want 1", atomic.LoadInt32(&selRuns))
	}

	// Change the consumed upstream: sel must recompute.
	registerEchoTask(e, "read", []byte("R2"), &readRuns)
	res, err = e.Run(ctx, "sel")
	if err != nil {
		t.Fatal(err)
	}
	if res["sel"].Cached {
		t.Errorf("sel must recompute when its consumed upstream bytes change")
	}
	if string(res["sel"].Output.Data) != "R2!" {
		t.Errorf("sel output %q want R2!", res["sel"].Output.Data)
	}
}
