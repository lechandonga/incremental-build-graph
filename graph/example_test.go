package graph_test

import (
	"context"
	"fmt"

	"github.com/lechandonga/incremental-build-graph/graph"
)

// ExampleEngine demonstrates registering a small diamond-shaped graph,
// running it, and observing cache hits on the second run.
func ExampleEngine() {
	ctx := context.Background()
	e := graph.NewEngine()

	mk := func(id string, deps []string, v int) graph.Task {
		t := graph.Task{
			ID:     graph.TaskID(id),
			Inputs: []byte(fmt.Sprintf("%d", v)),
			Run: func(ctx context.Context, in graph.Inputs) (graph.Artifact, error) {
				sum := v
				for _, d := range deps {
					a, _ := in.Artifact(graph.TaskID(d))
					sum += int(a.Data[0] - '0')
				}
				return graph.Artifact{Data: []byte{byte('0' + sum)}}, nil
			},
		}
		for _, d := range deps {
			t.Deps = append(t.Deps, graph.TaskID(d))
		}
		return t
	}

	e.Register(mk("a", nil, 1))
	e.Register(mk("b", nil, 2))
	e.Register(mk("c", []string{"a", "b"}, 0))

	run := func(label string) {
		res, err := e.Run(ctx, "c")
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Printf("%s: c=%s a.cached=%v b.cached=%v c.cached=%v\n",
			label, res["c"].Output.Data,
			res["a"].Cached, res["b"].Cached, res["c"].Cached)
	}

	run("first ")
	run("second")
	// Output:
	// first : c=3 a.cached=false b.cached=false c.cached=false
	// second: c=3 a.cached=true b.cached=true c.cached=true
}
