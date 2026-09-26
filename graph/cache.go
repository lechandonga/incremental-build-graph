package graph

import (
	"context"
	"fmt"
	"sync"
)

// ConsumedDep records one upstream artifact that a task actually
// consumed during its last execution: the upstream ID together with a
// hash of the exact artifact bytes it read.
type ConsumedDep struct {
	Task     TaskID
	DataHash string
}

// Entry is one persisted cache record for a task.
//
// An entry is only usable when:
//
//   - its logical Version equals the current Version;
//   - its content Fingerprint matches the one derived from the task's
//     current own inputs/config and the bytes of the upstream artifacts
//     it actually consumed (recorded in Consumed).
//
// Re-computing an upstream whose inputs changed but whose produced
// bytes are identical therefore does NOT invalidate downstream tasks:
// their content fingerprints are computed from artifact bytes, not from
// upstream fingerprints. Conversely, any change in a consumed upstream's
// bytes always changes the downstream fingerprint.
type Entry struct {
	Version     int
	Task        TaskID
	Fingerprint string
	// Consumed lists, in first-read order, the upstream artifacts the
	// task read during its last execution, each with a hash of the exact
	// bytes returned to the task.
	Consumed []ConsumedDep
	Output   Artifact
}

// Store persists cache entries across process restarts.
//
// Implementations must be safe for concurrent use. Get must return
// ErrCacheVersion or ErrCacheCorrupt for entries that cannot be used;
// the engine treats both as a clean miss.
type Store interface {
	Get(ctx context.Context, task TaskID) (Entry, error)
	Put(ctx context.Context, e Entry) error
	// Delete removes the entry for task, if any.
	Delete(ctx context.Context, task TaskID) error
}

// memStore is the default in-memory (non-persistent) Store.
type memStore struct {
	mu      sync.RWMutex
	entries map[TaskID]Entry
}

// NewMemoryStore returns a non-persistent Store, mainly useful for
// tests. Use NewFileStore for persistence across restarts.
func NewMemoryStore() Store {
	return &memStore{entries: make(map[TaskID]Entry)}
}

func (s *memStore) Get(ctx context.Context, task TaskID) (Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[task]
	if !ok {
		return Entry{}, ErrCacheMiss
	}
	if e.Version != Version {
		return Entry{}, fmt.Errorf("%w: entry version %d, want %d",
			ErrCacheVersion, e.Version, Version)
	}
	return e, nil
}

func (s *memStore) Put(ctx context.Context, e Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[e.Task] = e
	return nil
}

func (s *memStore) Delete(ctx context.Context, task TaskID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, task)
	return nil
}
