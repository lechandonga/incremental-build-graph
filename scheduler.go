package buildgraph

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
)

// Options configures a Scheduler.
type Options struct {
	// CacheDir is the directory used for persistent cache entries. When empty
	// an in-memory cache is used (reuse still works within one process, but
	// not across restarts).
	CacheDir string
	// Namespace isolates cache entries belonging to different graphs.
	Namespace string
	// Workers limits concurrently executed tasks; 0 means runtime.NumCPU.
	Workers int
}

// RunResult reports what one scheduler run did.
type RunResult struct {
	// Artifacts maps every task id to its produced artifact. The map is
	// shared by coalesced callers and must be treated as read-only.
	Artifacts map[string]Artifact
	// Executed lists the ids of tasks that were actually run (sorted).
	Executed []string
	// Cached lists the ids of tasks served from cache (sorted).
	Cached []string
}

// TaskError wraps a failure of an individual task.
type TaskError struct {
	TaskID string
	Err    error
}

func (e *TaskError) Error() string {
	return fmt.Sprintf("buildgraph: task %q failed: %v", e.TaskID, e.Err)
}

func (e *TaskError) Unwrap() error { return e.Err }

// Scheduler executes dependency graphs with incremental reuse.
type Scheduler struct {
	cache   Cache
	workers int

	mu      sync.Mutex
	waiters map[*Graph]*inflightRun
}

type inflightRun struct {
	done chan struct{}
	res  *RunResult
	err  error
}

// New creates a Scheduler with the given options.
func New(opts Options) (*Scheduler, error) {
	var cache Cache
	if opts.CacheDir != "" {
		fc, err := NewFileCache(opts.CacheDir, opts.Namespace)
		if err != nil {
			return nil, err
		}
		cache = fc
	} else {
		cache = NewMemoryCache()
	}
	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	return &Scheduler{cache: cache, workers: workers, waiters: make(map[*Graph]*inflightRun)}, nil
}

// validate checks structural invariants without mutating anything: every
// dependency must point at a registered task, and the graph must be acyclic.
func validate(g *Graph) error {
	for _, t := range g.Tasks() {
		for _, dep := range t.Deps {
			if _, ok := g.Get(dep); !ok {
				return fmt.Errorf("buildgraph: task %q depends on unknown task %q", t.ID, dep)
			}
		}
	}
	// Cycle detection performs no writes to cache or graph state, so a
	// reported cycle never corrupts existing state.
	return detectCycle(g)
}

// Run validates and executes the graph, reusing cached artifacts whenever the
// fingerprint matches.
//
// Concurrent Run calls for the same graph are coalesced: every caller shares
// one execution and receives the identical result. Unrelated tasks execute in
// parallel, while every task starts only after all of its dependencies have
// produced artifacts. On a task failure the run is aborted and the error is
// returned; no artifact for the failed task or its downstream is published.
func (s *Scheduler) Run(ctx context.Context, g *Graph) (*RunResult, error) {
	if err := validate(g); err != nil {
		return nil, err
	}

	// Coalescing is based on graph identity: callers triggering the same
	// *Graph instance concurrently share one execution. Structural equality
	// cannot be used because task closures (their actual behavior) are not
	// observable through graph data.
	s.mu.Lock()
	if w, ok := s.waiters[g]; ok {
		s.mu.Unlock()
		select {
		case <-w.done:
			return w.res, w.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	w := &inflightRun{done: make(chan struct{})}
	s.waiters[g] = w
	s.mu.Unlock()

	res, err := s.execute(ctx, g)

	s.mu.Lock()
	delete(s.waiters, g)
	s.mu.Unlock()
	w.res, w.err = res, err
	close(w.done)
	return res, err
}

// job is a unit of work handed to a worker. All dependency artifacts and the
// fingerprint are resolved by the coordinator before dispatch, so workers
// never touch the coordinator's mutable state.
type job struct {
	t            *Task
	depArtifacts map[string]Artifact
	fingerprint  []byte
}

type outcome struct {
	id       string
	artifact Artifact
	cached   bool
	err      error
}

func (s *Scheduler) execute(ctx context.Context, g *Graph) (*RunResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tasks := g.Tasks()
	total := len(tasks)

	// Unique dependency sets + indegrees for Kahn scheduling. Duplicate dep
	// declarations must not be counted twice.
	uniqueDeps := make(map[string]map[string]bool, total)
	indeg := make(map[string]int, total)
	for _, t := range tasks {
		set := make(map[string]bool, len(t.Deps))
		for _, dep := range t.Deps {
			set[dep] = true
		}
		uniqueDeps[t.ID] = set
		indeg[t.ID] = len(set)
	}

	jobs := make(chan job)
	results := make(chan outcome, total)
	var wg sync.WaitGroup
	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				results <- s.runTask(ctx, j)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	artifacts := make(map[string]Artifact, total)
	executed := make([]string, 0, total)
	cached := make([]string, 0, total)

	queue := make([]string, 0, total)
	for _, t := range tasks {
		if indeg[t.ID] == 0 {
			queue = append(queue, t.ID)
		}
	}

	spawned, completed := 0, 0
	var runErr error

	for completed < total && runErr == nil {
		var sendCh chan job
		var next job
		if len(queue) > 0 {
			id := queue[0]
			t := g.tasks[id]
			depArtifacts := make(map[string]Artifact, len(t.Deps))
			dh := make(map[string][]byte, len(t.Deps))
			for dep := range uniqueDeps[id] {
				a := artifacts[dep] // always present: dependency finished before enqueue
				depArtifacts[dep] = a
				dh[dep] = hashArtifact(a)
			}
			sendCh = jobs
			next = job{t: t, depArtifacts: depArtifacts, fingerprint: taskFingerprint(t, dh)}
		}

		select {
		case sendCh <- next:
			queue = queue[1:]
			spawned++
		case out := <-results:
			completed++
			if out.err != nil {
				runErr = out.err
				cancel()
				break
			}
			artifacts[out.id] = out.artifact
			if out.cached {
				cached = append(cached, out.id)
			} else {
				executed = append(executed, out.id)
			}
			for _, t := range tasks {
				if uniqueDeps[t.ID][out.id] {
					indeg[t.ID]--
					if indeg[t.ID] == 0 {
						queue = append(queue, t.ID)
					}
				}
			}
		case <-ctx.Done():
			runErr = ctx.Err()
		}
	}

	// Drain any outcomes from tasks that were already in flight so workers
	// can terminate; their results are discarded after a failure/cancel.
	for completed < spawned {
		<-results
		completed++
	}
	close(jobs)
	wg.Wait()

	if runErr != nil {
		return nil, runErr
	}

	sort.Strings(executed)
	sort.Strings(cached)
	return &RunResult{Artifacts: artifacts, Executed: executed, Cached: cached}, nil
}

// runTask resolves one task: cache lookup by fingerprint, execution on miss,
// and cache publication only after a successful run.
func (s *Scheduler) runTask(ctx context.Context, j job) outcome {
	t := j.t

	if artifact, ok := s.cache.Get(ctx, t.ID, j.fingerprint); ok {
		return outcome{id: t.ID, artifact: artifact, cached: true}
	}

	if err := ctx.Err(); err != nil {
		return outcome{id: t.ID, err: err}
	}

	artifact, err := t.Run(ctx, t.Input, t.Config, j.depArtifacts)
	if err != nil {
		// Nothing is published: failed tasks cannot seed stale results and
		// downstream tasks are never dispatched.
		return outcome{id: t.ID, err: &TaskError{TaskID: t.ID, Err: err}}
	}
	if err := s.cache.Put(ctx, t.ID, j.fingerprint, artifact); err != nil {
		// The artifact is correct even if durability failed; continue with
		// the in-memory value, and the next run simply recomputes this task.
		_ = err
	}
	return outcome{id: t.ID, artifact: artifact, cached: false}
}
