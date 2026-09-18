package graph

import (
	"context"
	"sync"
)

// Entry is one persisted cache record for a task.
//
// An entry is only usable when the task's fingerprint matches: the
// fingerprint covers the task inputs/config AND the fingerprints of all
// upstream artifacts, so a matching entry transitively guarantees that
// every upstream input is unchanged as well.
type Entry struct {
	Version     int
	Task        TaskID
	Fingerprint string
	Output      Artifact
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
