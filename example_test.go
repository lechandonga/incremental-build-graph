package buildgraph_test

import (
	"context"
	"fmt"
	"log"

	buildgraph "github.com/lechandonga/incremental-build-graph"
)

// A small compile/link pipeline: changing the source only reruns compile and
// link; the independent docs task is served from cache.
func Example() {
	ctx := context.Background()

	build := func(source string) *buildgraph.Graph {
		g := buildgraph.NewGraph()
		_ = g.Add(buildgraph.Task{
			ID:    "compile",
			Input: []byte(source),
			Run: func(_ context.Context, in, _ []byte, _ map[string]buildgraph.Artifact) (buildgraph.Artifact, error) {
				return buildgraph.Artifact("obj(" + string(in) + ")"), nil
			},
		})
		_ = g.Add(buildgraph.Task{
			ID:   "link",
			Deps: []string{"compile"},
			Run: func(_ context.Context, _, _ []byte, deps map[string]buildgraph.Artifact) (buildgraph.Artifact, error) {
				return buildgraph.Artifact("bin[" + string(deps["compile"]) + "]"), nil
			},
		})
		_ = g.Add(buildgraph.Task{
			ID:    "docs",
			Input: []byte("README"),
			Run: func(_ context.Context, in, _ []byte, _ map[string]buildgraph.Artifact) (buildgraph.Artifact, error) {
				return buildgraph.Artifact("docs(" + string(in) + ")"), nil
			},
		})
		return g
	}

	s, err := buildgraph.New(buildgraph.Options{CacheDir: ""}) // use t.TempDir() in real code
	if err != nil {
		log.Fatal(err)
	}

	first, err := s.Run(ctx, build("source-v1"))
	if err != nil {
		log.Fatal(err)
	}
	second, err := s.Run(ctx, build("source-v2"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("link:", string(first.Artifacts["link"]))
	fmt.Println("reran:", second.Executed)
	fmt.Println("reused:", second.Cached)
	// Output:
	// link: bin[obj(source-v1)]
	// reran: [compile link]
	// reused: [docs]
}
