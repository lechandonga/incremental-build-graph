package buildgraph

import (
	"bytes"
	"context"
	"sync"
)

// MemoryCache is a process-local Cache implementation. It provides the same
// keying semantics (task id + fingerprint) as FileCache and is safe for
// concurrent use, but stored artifacts do not survive a process restart.
type MemoryCache struct {
	mu      sync.RWMutex
	entries map[string][]byte // key: taskID + "\x00" + hex(fingerprint)
}

// NewMemoryCache returns an empty in-memory cache.
func NewMemoryCache() *MemoryCache {
	return &MemoryCache{entries: make(map[string][]byte)}
}

func memKey(taskID string, fingerprint []byte) string {
	var b bytes.Buffer
	b.WriteString(taskID)
	b.WriteByte(0)
	b.Write(fingerprint)
	return b.String()
}

func (c *MemoryCache) Get(_ context.Context, taskID string, fingerprint []byte) (Artifact, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	a, ok := c.entries[memKey(taskID, fingerprint)]
	if !ok {
		return nil, false
	}
	// Return a copy so callers cannot mutate cached state.
	return append(Artifact(nil), a...), true
}

func (c *MemoryCache) Put(_ context.Context, taskID string, fingerprint []byte, artifact Artifact) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[memKey(taskID, fingerprint)] = append(Artifact(nil), artifact...)
	return nil
}
