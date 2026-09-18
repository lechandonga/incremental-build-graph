package graph

import (
	"context"
	"sync"
)

// Option configures an Engine at construction time.
type Option func(*Engine)

// WithStore installs a custom cache Store.
func WithStore(s Store) Option {
	return func(e *Engine) { e.store = s }
}

// Engine holds a registered task graph and schedules incremental
// execution of requested targets.
type Engine struct {
	mu sync.RWMutex
	// runLock serializes whole-graph runs. Concurrent Run calls see
	// identical input and therefore produce identical results: the
	// second call simply observes the cache entries committed by the
	// first and returns cache hits.
	runLock sync.Mutex

	tasks map[TaskID]*Task
	store Store
}

// NewEngine constructs an Engine with the given options.
func NewEngine(opts ...Option) *Engine {
	e := &Engine{
		tasks: make(map[TaskID]*Task),
		store: NewMemoryStore(),
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Register adds or replaces a task definition. It does not touch any
// cached entries; stale entries are ignored automatically because their
// fingerprints no longer match.
func (e *Engine) Register(t Task) {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := t
	if cp.Deps != nil {
		cp.Deps = append([]TaskID(nil), t.Deps...)
	}
	if cp.Inputs != nil {
		cp.Inputs = append([]byte(nil), t.Inputs...)
	}
	e.tasks[t.ID] = &cp
}

// Run resolves and executes targets (and all of their transitive
// dependencies), reusing cached results whose fingerprints match.
// It returns the results keyed by task ID, or an error describing the
// first failure (cycle detection or task execution) encountered.
//
// Concurrent Run calls are serialized at graph granularity; they never
// execute the same task twice in parallel and never observe a partially
// written cache.
func (e *Engine) Run(ctx context.Context, targets ...TaskID) (map[TaskID]Result, error) {
	e.runLock.Lock()
	defer e.runLock.Unlock()

	r, err := e.resolve(targets)
	if err != nil {
		return nil, err
	}
	return e.execute(ctx, r)
}

// Validate checks that all dependencies resolve and that the full
// registered graph is acyclic. Existing graph state and cache contents
// are never modified; on a cycle it returns a *CycleError.
func (e *Engine) Validate() error {
	e.mu.RLock()
	ids := make([]TaskID, 0, len(e.tasks))
	for id := range e.tasks {
		ids = append(ids, id)
	}
	e.mu.RUnlock()
	_, err := e.resolve(ids)
	return err
}
