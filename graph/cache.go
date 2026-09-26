package graph

import (
	"context"
	"fmt"
	"sync"
)

// Entry is one persisted cache record for a task.
//
// An entry is only usable when the task's fingerprint matches: the
// fingerprint covers the task's own inputs/config AND the content
// hashes of the upstream artifacts it actually consumed, so a matching
// entry guarantees the task would observe byte-identical inputs if it
// ran again.
type Entry struct {
	Version     int
	Task        TaskID
	Fingerprint string
	Output      Artifact
}

// Store persists cache entries across process restarts.
//
// Implementations must be safe for concurrent use. Get must return
// ErrCacheVersion for entries written by an incompatible (older or
// newer) build and ErrCacheCorrupt for entries whose bytes cannot be
// trusted, so callers can distinguish the two failure kinds; the engine
// treats both as a clean miss and self-heals by overwriting on the next
// successful Put.
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
	// A recognizable entry written by an older/newer build is reported
	// as a version incompatibility so callers can distinguish it from
	// corruption; the engine treats both as a clean miss.
	if e.Version != Version {
		return e, fmt.Errorf("%w: entry version %d, want %d",
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
