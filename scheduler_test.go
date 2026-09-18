package buildgraph

import (
	"context"
	"sync/atomic"
	"testing"
)

func noop(_ context.Context, _, _ []byte, deps map[string]Artifact) (Artifact, error) {
	return Artifact("x"), nil
}

func TestBasicExecution(t *testing.T) {
	g := NewGraph()
	var runs int64
	mk := func(id string, deps ...string) Task {
		return Task{
			ID: id, Deps: deps,
			Run: func(_ context.Context, _, _ []byte, d map[string]Artifact) (Artifact, error) {
				atomic.AddInt64(&runs, 1)
				out := id
				for _, dep := range deps {
					out += "(" + string(d[dep]) + ")"
				}
				return Artifact(out), nil
			},
		}
	}
	if err := g.Add(mk("a")); err != nil {
		t.Fatal(err)
	}
	if err := g.Add(mk("b", "a")); err != nil {
		t.Fatal(err)
	}
	if err := g.Add(mk("c", "a", "b")); err != nil {
		t.Fatal(err)
	}
	s, err := New(Options{CacheDir: ""})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Run(context.Background(), g)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := string(res.Artifacts["c"]); got != "c(a)(b(a))" {
		t.Fatalf("unexpected artifact %q", got)
	}
	if len(res.Executed) != 3 || len(res.Cached) != 0 {
		t.Fatalf("expected all 3 executed, got %+v", res)
	}
}

func TestIncrementalHit(t *testing.T) {
	dir := t.TempDir()
	var runs int64
	g := NewGraph()
	_ = g.Add(Task{ID: "a", Input: []byte("in-a"),
		Run: func(_ context.Context, in, _ []byte, _ map[string]Artifact) (Artifact, error) {
			atomic.AddInt64(&runs, 1)
			return Artifact("a:" + string(in)), nil
		}})
	_ = g.Add(Task{ID: "b", Deps: []string{"a"},
		Run: func(_ context.Context, _, _ []byte, d map[string]Artifact) (Artifact, error) {
			atomic.AddInt64(&runs, 1)
			return Artifact("b:" + string(d["a"])), nil
		}})

	s1, _ := New(Options{CacheDir: dir})
	res1, err := s1.Run(context.Background(), g)
	if err != nil {
		t.Fatal(err)
	}
	if len(res1.Cached) != 0 || atomic.LoadInt64(&runs) != 2 {
		t.Fatalf("first run should execute everything: %+v runs=%d", res1, runs)
	}

	// Brand new scheduler simulating a process restart; same inputs.
	s2, _ := New(Options{CacheDir: dir})
	res2, err := s2.Run(context.Background(), g)
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt64(&runs) != 2 {
		t.Fatalf("second run should be fully cached, runs=%d", runs)
	}
	if len(res2.Cached) != 2 {
		t.Fatalf("expected 2 cache hits, got %+v", res2)
	}
	if string(res2.Artifacts["b"]) != "b:a:in-a" {
		t.Fatalf("artifact mismatch: %q", res2.Artifacts["b"])
	}
}

func TestInvalidationPropagation(t *testing.T) {
	dir := t.TempDir()
	var aRuns, bRuns, cRuns int64
	build := func(inputA string) *Graph {
		g := NewGraph()
		_ = g.Add(Task{ID: "a", Input: []byte(inputA),
			Run: func(_ context.Context, in, _ []byte, _ map[string]Artifact) (Artifact, error) {
				atomic.AddInt64(&aRuns, 1)
				return Artifact("a=" + string(in)), nil
			}})
		_ = g.Add(Task{ID: "b", Deps: []string{"a"}, Input: []byte("static"),
			Run: func(_ context.Context, _, _ []byte, d map[string]Artifact) (Artifact, error) {
				atomic.AddInt64(&bRuns, 1)
				return Artifact("b(" + string(d["a"]) + ")"), nil
			}})
		_ = g.Add(Task{ID: "c", Deps: []string{"b"},
			Run: func(_ context.Context, _, _ []byte, d map[string]Artifact) (Artifact, error) {
				atomic.AddInt64(&cRuns, 1)
				return Artifact("c(" + string(d["b"]) + ")"), nil
			}})
		_ = g.Add(Task{ID: "independent", Input: []byte("fixed"),
			Run: func(_ context.Context, in, _ []byte, _ map[string]Artifact) (Artifact, error) {
				return Artifact("ind:" + string(in)), nil
			}})
		return g
	}

	s, _ := New(Options{CacheDir: dir})
	if _, err := s.Run(context.Background(), build("v1")); err != nil {
		t.Fatal(err)
	}

	// Change input of a: a, b, c must rerun; independent must stay cached.
	res, err := s.Run(context.Background(), build("v2"))
	if err != nil {
		t.Fatal(err)
	}
	wantExec := map[string]bool{"a": true, "b": true, "c": true}
	if len(res.Executed) != 3 {
		t.Fatalf("expected a,b,c executed: %+v", res)
	}
	for _, id := range res.Executed {
		if !wantExec[id] {
			t.Fatalf("unexpected task executed: %s", id)
		}
	}
	if len(res.Cached) != 1 || res.Cached[0] != "independent" {
		t.Fatalf("expected only independent cached: %+v", res)
	}
	if string(res.Artifacts["c"]) != "c(b(a=v2))" {
		t.Fatalf("stale artifact: %q", res.Artifacts["c"])
	}
	if atomic.LoadInt64(&aRuns) != 2 || atomic.LoadInt64(&bRuns) != 2 || atomic.LoadInt64(&cRuns) != 2 {
		t.Fatalf("run counts wrong: %d %d %d", aRuns, bRuns, cRuns)
	}
}
