package graph

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

func newTestEngine() *Engine {
	return NewEngine(WithStore(NewMemoryStore()))
}

// incTask is a simple task that records how many times it ran and
// produces the sum of upstream outputs plus its own input value.
func registerAdder(e *Engine, id TaskID, deps []TaskID, input int, runs *int32) {
	e.Register(Task{
		ID:     id,
		Deps:   deps,
		Inputs: []byte(fmt.Sprintf(`{"v":%d}`, input)),
		Run: func(ctx context.Context, in Inputs) (Artifact, error) {
			atomic.AddInt32(runs, 1)
			sum := int32(input)
			for _, d := range deps {
				a, ok := in.Artifact(d)
				if !ok {
					return Artifact{}, fmt.Errorf("missing dep %s", d)
				}
				sum += int32(decodeInt(a.Data))
			}
			return Artifact{Data: encodeInt(int(sum))}, nil
		},
	})
}

func encodeInt(v int) []byte { return []byte(fmt.Sprintf("%d", v)) }
func decodeInt(b []byte) int {
	var v int
	fmt.Sscanf(string(b), "%d", &v)
	return v
}

func TestIncrementalHit(t *testing.T) {
	e := newTestEngine()
	var aRuns, bRuns, cRuns int32
	// a -> c, b -> c (diamond)
	registerAdder(e, "a", nil, 1, &aRuns)
	registerAdder(e, "b", nil, 2, &bRuns)
	registerAdder(e, "c", []TaskID{"a", "b"}, 10, &cRuns)

	ctx := context.Background()
	res, err := e.Run(ctx, "c")
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if got := decodeInt(res["c"].Output.Data); got != 13 {
		t.Fatalf("c = %d, want 13", got)
	}
	if res["a"].Cached || res["b"].Cached || res["c"].Cached {
		t.Fatalf("first run must be all misses: %+v", res)
	}

	// Second run with identical inputs: everything must hit.
	res, err = e.Run(ctx, "c")
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	for _, id := range []TaskID{"a", "b", "c"} {
		if !res[id].Cached {
			t.Errorf("task %s expected cache hit", id)
		}
	}
	if got := atomic.LoadInt32(&aRuns); got != 1 {
		t.Errorf("a ran %d times, want 1", got)
	}
	if got := atomic.LoadInt32(&bRuns); got != 1 {
		t.Errorf("b ran %d times, want 1", got)
	}
	if got := atomic.LoadInt32(&cRuns); got != 1 {
		t.Errorf("c ran %d times, want 1", got)
	}
}

func TestInvalidationPropagation(t *testing.T) {
	e := newTestEngine()
	var aRuns, bRuns, cRuns, dRuns int32
	registerAdder(e, "a", nil, 1, &aRuns)
	registerAdder(e, "b", []TaskID{"a"}, 0, &bRuns)
	registerAdder(e, "c", []TaskID{"a"}, 0, &cRuns)
	registerAdder(e, "d", []TaskID{"b", "c"}, 0, &dRuns)
	ctx := context.Background()
	if _, err := e.Run(ctx, "d"); err != nil {
		t.Fatal(err)
	}

	// Change only a: a, b, c, d must recompute.
	registerAdder(e, "a", nil, 2, &aRuns)
	res, err := e.Run(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []TaskID{"a", "b", "c", "d"} {
		if res[id].Cached {
			t.Errorf("task %s should have recomputed", id)
		}
	}

	// Change b only: a and c must stay cached, b and d recompute.
	registerAdder(e, "b", []TaskID{"a"}, 5, &bRuns)
	res, err = e.Run(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if !res["a"].Cached {
		t.Errorf("a should be cached, unaffected by b change")
	}
	if !res["c"].Cached {
		t.Errorf("c should be cached, unaffected by b change")
	}
	if res["b"].Cached || res["d"].Cached {
		t.Errorf("b and d should recompute")
	}
	if got := decodeInt(res["d"].Output.Data); got != 9 {
		t.Errorf("d = %d, want 9 (a=2, b=7, c=2)", got)
	}
}

func TestUnrelatedResultsUntouched(t *testing.T) {
	e := newTestEngine()
	var xRuns, yRuns int32
	registerAdder(e, "x", nil, 1, &xRuns)
	registerAdder(e, "y", nil, 1, &yRuns)
	ctx := context.Background()
	if _, err := e.Run(ctx, "x", "y"); err != nil {
		t.Fatal(err)
	}
	// Change x and only request x: y's cache entry must remain valid.
	registerAdder(e, "x", nil, 2, &xRuns)
	if _, err := e.Run(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	res, err := e.Run(ctx, "y")
	if err != nil {
		t.Fatal(err)
	}
	if !res["y"].Cached {
		t.Errorf("unrelated task y must still hit cache")
	}
	if runs := atomic.LoadInt32(&yRuns); runs != 1 {
		t.Errorf("y ran %d times, want 1", runs)
	}
}

func TestUnknownTask(t *testing.T) {
	e := newTestEngine()
	if _, err := e.Run(context.Background(), "nope"); err == nil {
		t.Fatal("expected error for unknown target")
	}
}

func TestEmptyTargets(t *testing.T) {
	e := newTestEngine()
	res, err := e.Run(context.Background())
	if err != nil {
		t.Fatalf("empty run: %v", err)
	}
	if len(res) != 0 {
		t.Fatalf("empty run should return no results, got %v", res)
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("empty graph should validate: %v", err)
	}
}

func TestDuplicateDepsCountedOnce(t *testing.T) {
	e := newTestEngine()
	var runs int32
	// Declare the same dependency twice; it must execute once and be
	// provided once per occurrence to the task.
	registerAdder(e, "a", nil, 1, &runs)
	e.Register(Task{
		ID:     "b",
		Deps:   []TaskID{"a", "a"},
		Inputs: []byte("b"),
		Run: func(ctx context.Context, in Inputs) (Artifact, error) {
			a, ok := in.Artifact("a")
			if !ok {
				return Artifact{}, errMissing
			}
			return Artifact{Data: a.Data}, nil
		},
	})
	res, err := e.Run(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if decodeInt(res["b"].Output.Data) != 1 {
		t.Fatalf("b output = %q, want 1", res["b"].Output.Data)
	}
}

var errMissing = sterr("missing artifact")

type sterr string

func (e sterr) Error() string { return string(e) }
