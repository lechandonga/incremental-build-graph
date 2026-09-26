package graph

import (
	"context"
	"fmt"
	"sync"
)

// resolution is the computed execution plan for one Run: the set of
// reachable tasks in a topological order. Per-task fingerprints are not
// computed up front: they depend on the actual artifact bytes emitted
// during this run, so they are derived at execution time in runTask.
type resolution struct {
	order []TaskID
	tasks map[TaskID]*Task
}

// resolve performs validation, cycle detection, reachability from
// targets and fingerprint computation. It never mutates engine or cache
// state, so a failure (unknown task or cycle) leaves graph and cache
// contents completely untouched.
func (e *Engine) resolve(targets []TaskID) (*resolution, error) {
	e.mu.RLock()
	tasks := make(map[TaskID]*Task, len(e.tasks))
	for id, t := range e.tasks {
		tasks[id] = t
	}
	e.mu.RUnlock()

	// Deduplicate targets while preserving first-seen order.
	seen := make(map[TaskID]bool, len(targets))
	var roots []TaskID
	for _, t := range targets {
		if !seen[t] {
			seen[t] = true
			roots = append(roots, t)
		}
	}

	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[TaskID]int)
	var order []TaskID

	type frame struct {
		id   TaskID
		next int
	}

	// Iterative postorder DFS over all roots.
	var stack []*frame
	for _, root := range roots {
		if color[root] == black {
			continue
		}
		if _, ok := tasks[root]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrUnknownTask, root)
		}
		color[root] = gray
		stack = append(stack, &frame{id: root})

		for len(stack) > 0 {
			f := stack[len(stack)-1]
			t := tasks[f.id]
			if f.next < len(t.Deps) {
				dep := t.Deps[f.next]
				f.next++
				switch color[dep] {
				case gray:
					// Back edge: the frames from the still-open
					// occurrence of dep to the top form the cycle.
					start := -1
					for i, g := range stack {
						if g.id == dep {
							start = i
							break
						}
					}
					cyc := make([]TaskID, 0, len(stack)-start)
					for _, g := range stack[start:] {
						cyc = append(cyc, g.id)
					}
					return nil, &CycleError{Cycle: cyc}
				case black:
					continue
				default:
					depTask, ok := tasks[dep]
					if !ok {
						return nil, fmt.Errorf("%w: %s (required by %s)",
							ErrUnknownTask, dep, f.id)
					}
					_ = depTask
					color[dep] = gray
					stack = append(stack, &frame{id: dep})
				}
				continue
			}
			// All dependencies resolved. Duplicate edges on the same
			// dependency are de-duplicated, matching the executor's
			// indegree handling.
			color[f.id] = black
			order = append(order, f.id)
			stack = stack[:len(stack)-1]
		}
	}

	return &resolution{order: order, tasks: tasks}, nil
}

const maxParallel = 64

// taskState tracks one task through a single run.
type taskState int

const (
	stPending taskState = iota
	stRunning
	stDone
	stFailed
	stSkipped
)

// execute runs the resolved plan. Tasks are scheduled concurrently as
// soon as all their dependencies have completed, so independent tasks
// run in parallel. A task with a cache entry whose fingerprint matches
// is reused without executing its Run function.
//
// A failed task aborts the affected branch: its downstream tasks are
// marked skipped and never observe partial or stale upstream products,
// and no stale cache entries are written. Successful results are
// committed to the cache atomically per task.
func (e *Engine) execute(ctx context.Context, r *resolution) (map[TaskID]Result, error) {
	n := len(r.order)
	indeg := make(map[TaskID]int, n)
	dependents := make(map[TaskID][]TaskID, n)
	for _, id := range r.order {
		t := r.tasks[id]
		seen := make(map[TaskID]bool, len(t.Deps))
		for _, dep := range t.Deps {
			if !seen[dep] {
				seen[dep] = true
				indeg[id]++
				dependents[dep] = append(dependents[dep], id)
			}
		}
	}

	sem := make(chan struct{}, maxParallel)
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}

	var (
		mu        sync.Mutex
		cond      = sync.NewCond(&mu)
		state     = make(map[TaskID]taskState, n)
		results   = make(map[TaskID]Result, n)
		ready     []TaskID
		remaining = n
		running   int
		firstErr  error
	)
	enqueue := func(id TaskID) { ready = append(ready, id) }
	for _, id := range r.order {
		if indeg[id] == 0 {
			enqueue(id)
		}
	}

	// propagateFailure marks every not-yet-finished transitive
	// dependent of id as skipped.
	propagateFailure := func(id TaskID) {
		var walk []TaskID
		walk = append(walk, dependents[id]...)
		for len(walk) > 0 {
			cur := walk[0]
			walk = walk[1:]
			if state[cur] == stDone || state[cur] == stFailed ||
				state[cur] == stSkipped {
				continue
			}
			state[cur] = stSkipped
			remaining--
			walk = append(walk, dependents[cur]...)
		}
	}

	dispatch := func(id TaskID) {
		go func() {
			select {
			case <-sem:
			case <-ctx.Done():
				mu.Lock()
				if state[id] == stRunning {
					state[id] = stFailed
					running--
					if firstErr == nil {
						firstErr = ctx.Err()
					}
					propagateFailure(id)
					remaining--
					cond.Broadcast()
				}
				mu.Unlock()
				return
			}
			defer func() { sem <- struct{}{} }()

			res, err := e.runTask(ctx, r, id, func(dep TaskID) (Result, bool) {
				mu.Lock()
				res, ok := results[dep]
				mu.Unlock()
				return res, ok
			})

			mu.Lock()
			defer mu.Unlock()
			running--
			if err != nil {
				state[id] = stFailed
				if firstErr == nil {
					firstErr = err
				}
				propagateFailure(id)
				remaining--
				cond.Broadcast()
				return
			}
			if firstErr != nil {
				// Another task already failed; discard this result.
				state[id] = stDone
				remaining--
				cond.Broadcast()
				return
			}
			state[id] = stDone
			results[id] = res
			remaining--
			for _, d := range dependents[id] {
				if state[d] != stPending {
					continue
				}
				indeg[d]--
				if indeg[d] == 0 {
					ready = append(ready, d)
				}
			}
			cond.Broadcast()
		}()
	}

	mu.Lock()
	for {
		for (firstErr == nil && len(ready) == 0 && remaining > 0) ||
			(firstErr != nil && running > 0) {
			cond.Wait()
		}
		if remaining == 0 {
			break
		}
		if firstErr != nil {
			// Drain everything still waiting; in-flight tasks are
			// allowed to finish atomically but their results are
			// discarded. This keeps concurrent triggers from
			// interleaving with half-finished work.
			for _, id := range ready {
				if state[id] == stPending {
					state[id] = stSkipped
					remaining--
				}
			}
			ready = nil
			if running == 0 {
				break
			}
			continue
		}
		batch := ready
		ready = nil
		for _, id := range batch {
			if state[id] != stPending {
				continue
			}
			state[id] = stRunning
			running++
			dispatch(id)
		}
	}
	mu.Unlock()

	if firstErr != nil {
		return nil, firstErr
	}
	out := make(map[TaskID]Result, n)
	for _, id := range r.order {
		out[id] = results[id]
	}
	return out, nil
}

// depResult fetches a completed dependency result.
type depResult func(id TaskID) (Result, bool)

// runTask produces one task Result. The fingerprint is derived from the
// task's own inputs plus the content hashes of the upstream artifacts it
// actually consumed during this run, so:
//
//   - a cache hit requires matching inputs/config AND byte-identical
//     consumed upstream artifacts;
//   - an upstream task that is recomputed but emits byte-identical
//     output does NOT invalidate its downstreams;
//   - any byte change in a consumed upstream artifact invalidates every
//     downstream that reads it.
//
// On a miss the task executes and the new entry is committed to the
// cache atomically per task.
func (e *Engine) runTask(ctx context.Context, r *resolution, id TaskID, deps depResult) (Result, error) {
	t := r.tasks[id]

	// Content-based fingerprint of this task as observed right now:
	// declared deps in order, duplicates collapsed, hashing the current
	// upstream artifact bytes.
	depContents := func() []depContent {
		seen := make(map[TaskID]bool, len(t.Deps))
		var dc []depContent
		for _, d := range t.Deps {
			if seen[d] {
				continue
			}
			seen[d] = true
			res, ok := deps(d)
			if !ok {
				continue
			}
			dc = append(dc, depContent{id: d, sum: artifactContentSum(res.Output)})
		}
		return dc
	}
	wantFP := fingerprint(t, depContents())

	if entry, err := e.store.Get(ctx, id); err == nil {
		if entry.Version == Version && entry.Fingerprint == wantFP {
			return Result{
				Task:        id,
				Output:      entry.Output,
				Fingerprint: wantFP,
				Cached:      true,
			}, nil
		}
	}
	// Any error (miss, corrupt, old version) falls through to a fresh
	// execution; corrupt entries are overwritten on success.

	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	in := &liveInputs{deps: deps}
	out, err := t.Run(ctx, in)
	if err != nil {
		return Result{}, &TaskError{Task: id, Err: err}
	}

	entry := Entry{
		Version:     Version,
		Task:        id,
		Fingerprint: wantFP,
		Output:      out,
	}
	if err := e.store.Put(ctx, entry); err != nil {
		return Result{}, &TaskError{Task: id, Err: err}
	}
	return Result{
		Task:        id,
		Output:      out,
		Fingerprint: wantFP,
		Cached:      false,
	}, nil
}

// liveInputs resolves upstream artifacts from completed results at
// execution time.
type liveInputs struct {
	deps depResult
}

func (in *liveInputs) Artifact(id TaskID) (Artifact, bool) {
	res, ok := in.deps(id)
	if !ok {
		return Artifact{}, false
	}
	return res.Output, true
}
