// Package buildgraph implements a dependency-graph scheduling engine with
// incremental recomputation, content-addressed persistent caching and
// concurrency-safe execution.
//
// The typical workflow is:
//
//	g := buildgraph.NewGraph()
//	_ = g.Add(buildgraph.Task{ID: "compile", Run: compileFn})
//	s, _ := buildgraph.New(buildgraph.Options{CacheDir: ".cache"})
//	res, err := s.Run(ctx, g)
package buildgraph

import (
	"context"
	"errors"
)

// Artifact is the opaque output produced by a task. Artifacts are content
// addressed by their hash when fingerprints of downstream tasks are computed.
type Artifact []byte

// TaskFunc executes a single task. It receives the task's own input and
// configuration together with the artifacts of every declared dependency.
type TaskFunc func(ctx context.Context, input, config []byte, deps map[string]Artifact) (Artifact, error)

// Task declares one node of the dependency graph.
type Task struct {
	// ID uniquely identifies the task within a graph.
	ID string
	// Input is the external input of the task; it participates in the fingerprint.
	Input []byte
	// Config is the task configuration; it participates in the fingerprint.
	Config []byte
	// Deps lists the IDs of upstream tasks whose artifacts are required.
	Deps []string
	// Run produces the task artifact. It must be deterministic with respect to
	// input, config and dependency artifacts.
	Run TaskFunc
}

// Graph is the set of tasks connected by dependency edges.
//
// A graph value must not be mutated while a Scheduler.Run is in flight; build
// it first and then share it read-only between concurrent runs.
type Graph struct {
	tasks map[string]*Task
	order []string
}

// NewGraph creates an empty graph.
func NewGraph() *Graph {
	return &Graph{tasks: make(map[string]*Task)}
}

// Add registers a task in the graph. It returns an error when the id is
// empty, duplicated or when the task has no run function. Dependencies are
// not validated here; Run validates the whole graph atomically.
func (g *Graph) Add(t Task) error {
	if t.ID == "" {
		return errors.New("buildgraph: task id must not be empty")
	}
	if g.tasks == nil {
		g.tasks = make(map[string]*Task)
	}
	if _, exists := g.tasks[t.ID]; exists {
		return errors.New("buildgraph: duplicate task id: " + t.ID)
	}
	if t.Run == nil {
		return errors.New("buildgraph: task " + t.ID + " has no run function")
	}
	cp := t
	// Copy slices so later mutation by the caller cannot corrupt the graph.
	cp.Input = append([]byte(nil), t.Input...)
	cp.Config = append([]byte(nil), t.Config...)
	cp.Deps = append([]string(nil), t.Deps...)
	g.tasks[t.ID] = &cp
	g.order = append(g.order, t.ID)
	return nil
}

// Get returns the task with the given id.
func (g *Graph) Get(id string) (*Task, bool) {
	t, ok := g.tasks[id]
	return t, ok
}

// Tasks returns all tasks in insertion order.
func (g *Graph) Tasks() []*Task {
	out := make([]*Task, 0, len(g.order))
	for _, id := range g.order {
		out = append(out, g.tasks[id])
	}
	return out
}
