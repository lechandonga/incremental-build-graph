package buildgraph

import (
	"fmt"
	"strings"
)

// CycleError describes a dependency cycle detected in a graph.
type CycleError struct {
	// Path is the closed walk that forms the cycle, starting and ending with
	// the same task id, e.g. [a b c a].
	Path []string
}

func (e *CycleError) Error() string {
	return "buildgraph: dependency cycle detected: " + strings.Join(e.Path, " -> ")
}

// detectCycle returns a *CycleError if g contains a dependency cycle.
//
// Edges point from a task to its dependencies. Detection uses an iterative
// depth-first search with three colors; it never mutates graph state and is
// therefore safe to run before any caching or execution step.
func detectCycle(g *Graph) error {
	const (
		white = 0 // not visited
		gray  = 1 // on the current DFS stack
		black = 2 // fully explored
	)

	color := make(map[string]int, len(g.order))
	for _, id := range g.order {
		color[id] = white
	}

	for _, start := range g.order {
		if color[start] != white {
			continue
		}

		// Stack frames hold the node id and the index of the next edge to probe.
		type frame struct {
			id  string
			pos int
		}
		stack := []frame{{id: start, pos: 0}}
		color[start] = gray

		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			task := g.tasks[top.id]

			if top.pos < len(task.Deps) {
				next := task.Deps[top.pos]
				top.pos++
				switch color[next] {
				case gray:
					// Found a back edge: extract the cycle from the stack.
					path := []string{}
					begin := len(stack)
					for i := range stack {
						if stack[i].id == next {
							begin = i
							break
						}
					}
					for i := begin; i < len(stack); i++ {
						path = append(path, stack[i].id)
					}
					path = append(path, next)
					return &CycleError{Path: path}
				case white:
					color[next] = gray
					stack = append(stack, frame{id: next, pos: 0})
				}
				continue
			}

			color[top.id] = black
			stack = stack[:len(stack)-1]
		}
	}

	if len(color) != len(g.order) {
		// Defensive: should be unreachable because every node starts in color.
		return fmt.Errorf("buildgraph: internal cycle detection state mismatch")
	}
	return nil
}
