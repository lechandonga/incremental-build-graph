package buildgraph

import (
	"context"
	"errors"
	"testing"
)

func TestCycleDetection(t *testing.T) {
	g := NewGraph()
	mk := func(id string, deps ...string) Task {
		return Task{ID: id, Deps: deps, Run: noop}
	}
	_ = g.Add(mk("a", "b"))
	_ = g.Add(mk("b", "c"))
	_ = g.Add(mk("c", "a"))

	s, _ := New(Options{})
	_, err := s.Run(context.Background(), g)
	var ce *CycleError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CycleError, got %v", err)
	}
	if len(ce.Path) < 3 || ce.Path[0] != ce.Path[len(ce.Path)-1] {
		t.Fatalf("cycle path must be a closed walk, got %v", ce.Path)
	}
	// Every consecutive edge in the reported path must exist in the graph.
	for i := 0; i+1 < len(ce.Path); i++ {
		task, _ := g.Get(ce.Path[i])
		found := false
		for _, d := range task.Deps {
			if d == ce.Path[i+1] {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("cycle path uses non-existent edge %s -> %s", ce.Path[i], ce.Path[i+1])
		}
	}
}

func TestSelfDependencyIsCycle(t *testing.T) {
	g := NewGraph()
	_ = g.Add(Task{ID: "a", Deps: []string{"a"}, Run: noop})
	s, _ := New(Options{})
	_, err := s.Run(context.Background(), g)
	var ce *CycleError
	if !errors.As(err, &ce) {
		t.Fatalf("expected self dependency cycle, got %v", err)
	}
	if len(ce.Path) != 2 || ce.Path[0] != "a" || ce.Path[1] != "a" {
		t.Fatalf("unexpected path %v", ce.Path)
	}
}

func TestCycleDoesNotCorruptState(t *testing.T) {
	dir := t.TempDir()
	s, _ := New(Options{CacheDir: dir})

	// Seed valid cached output for a healthy graph.
	good := NewGraph()
	_ = good.Add(Task{ID: "a", Input: []byte("v1"), Run: func(_ context.Context, in, _ []byte, _ map[string]Artifact) (Artifact, error) {
		return Artifact("ok:" + string(in)), nil
	}})
	if _, err := s.Run(context.Background(), good); err != nil {
		t.Fatal(err)
	}

	// A cyclic graph must be rejected before execution...
	bad := NewGraph()
	_ = bad.Add(Task{ID: "x", Deps: []string{"y"}, Run: noop})
	_ = bad.Add(Task{ID: "y", Deps: []string{"x"}, Run: noop})
	if _, err := s.Run(context.Background(), bad); err == nil {
		t.Fatal("expected cycle error")
	}

	// ...and the previously cached result must still be a hit.
	res, err := s.Run(context.Background(), good)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Cached) != 1 || res.Cached[0] != "a" {
		t.Fatalf("healthy graph cache must survive cycle rejection: %+v", res)
	}
}

func TestUnknownDependency(t *testing.T) {
	g := NewGraph()
	_ = g.Add(Task{ID: "a", Deps: []string{"ghost"}, Run: noop})
	s, _ := New(Options{})
	if _, err := s.Run(context.Background(), g); err == nil {
		t.Fatal("expected unknown dependency error")
	}
}

func TestGraphAddValidation(t *testing.T) {
	g := NewGraph()
	if err := g.Add(Task{ID: "a", Run: noop}); err != nil {
		t.Fatal(err)
	}
	if err := g.Add(Task{ID: "a", Run: noop}); err == nil {
		t.Fatal("duplicate id must error")
	}
	if err := g.Add(Task{ID: ""}); err == nil {
		t.Fatal("empty id must error")
	}
	if err := g.Add(Task{ID: "b"}); err == nil {
		t.Fatal("nil run must error")
	}
}

func TestAcyclicGraphNoFalsePositive(t *testing.T) {
	g := NewGraph()
	mk := func(id string, deps ...string) Task { return Task{ID: id, Deps: deps, Run: noop} }
	// Diamond and shared dependency must not be misreported.
	_ = g.Add(mk("root", "left", "right"))
	_ = g.Add(mk("left", "base"))
	_ = g.Add(mk("right", "base"))
	_ = g.Add(mk("base"))
	s, _ := New(Options{})
	if _, err := s.Run(context.Background(), g); err != nil {
		t.Fatalf("diamond graph should execute: %v", err)
	}
}
