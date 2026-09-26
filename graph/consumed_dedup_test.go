package graph

import (
	"context"
	"testing"
)

// TestConsumedDedup: reading the same declared dependency many times
// (and declaring it twice) records it once, and the persisted entry is
// stable across restart.
func TestConsumedDedup(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	mk := func(store Store) (*Engine, *int32) {
		en := NewEngine(WithStore(store))
		var aRuns, bRuns int32
		registerEchoTask(en, "a", []byte("A"), &aRuns)
		en.Register(Task{
			ID: "b", Deps: []TaskID{"a", "a"}, Inputs: []byte("b"),
			Run: func(ctx context.Context, in Inputs) (Artifact, error) {
				bRuns++
				in.Artifact("a")
				in.Artifact("a")
				a, _ := in.Artifact("a")
				return Artifact{Data: a.Data}, nil
			},
		})
		return en, &bRuns
	}

	e1, _ := mk(mustFileStore(t, dir))
	if _, err := e1.Run(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	st := mustFileStore(t, dir)
	entry, err := st.Get(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if len(entry.Consumed) != 1 || entry.Consumed[0].Task != "a" {
		t.Fatalf("expected exactly one consumed record for a, got %+v", entry.Consumed)
	}

	// Restart: b must hit (a bytes unchanged) and Consumed stays one.
	e2, bRuns2 := mk(mustFileStore(t, dir))
	res, err := e2.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if !res["b"].Cached {
		t.Fatal("b must hit after restart")
	}
	if *bRuns2 != 0 {
		t.Fatalf("b executed %d times after restart, want 0", *bRuns2)
	}
	entry2, _ := st.Get(ctx, "b")
	if len(entry2.Consumed) != 1 {
		t.Fatalf("consumed list not stable: %+v", entry2.Consumed)
	}
}
