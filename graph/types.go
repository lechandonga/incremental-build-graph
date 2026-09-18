// Package graph implements a dependency-graph scheduling engine with
// incremental recomputation and persistent cache invalidation.
package graph

import (
	"context"
	"errors"
)

// Version is the cache entry format version produced by this build.
const Version = 1

// TaskID identifies a task within a graph.
type TaskID string

// Task defines a unit of computation together with its declared
// dependencies.
type Task struct {
	// ID is the unique identifier of the task.
	ID TaskID
	// Deps lists the direct upstream task IDs in execution order.
	Deps []TaskID
	// Inputs is an opaque, stable description of the task's own inputs
	// and configuration (for example JSON). Its bytes are part of the
	// task fingerprint.
	Inputs []byte
	// Run executes the task given the resolved upstream artifacts.
	// It returns the task's artifact on success.
	Run func(ctx context.Context, in Inputs) (Artifact, error)
}

// Artifact is the opaque product of a successful task execution.
type Artifact struct {
	// Data is the artifact payload. It must be serializable by the
	// Codec configured on the Engine.
	Data []byte
}

// Inputs gives a task access to its upstream artifacts during execution.
type Inputs interface {
	// Artifact returns the artifact produced by upstream task id.
	Artifact(id TaskID) (Artifact, bool)
}

// Result is the outcome recorded for a task after a run of the graph.
type Result struct {
	Task        TaskID
	Output      Artifact
	Fingerprint string
	// Cached is true when the output was reused from the cache rather
	// than freshly executed.
	Cached bool
}

// CycleError describes a dependency cycle detected while resolving a
// target set.
type CycleError struct {
	// Cycle lists the task IDs forming the cycle, in dependency order,
	// without repeating the first node at the end.
	Cycle []TaskID
}

func (e *CycleError) Error() string { return "dependency cycle detected" }

// AsCycleError extracts a *CycleError from err, if present.
func AsCycleError(err error) (*CycleError, bool) {
	var ce *CycleError
	if errors.As(err, &ce) {
		return ce, true
	}
	return nil, false
}
