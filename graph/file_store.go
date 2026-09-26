package graph

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// FileStore persists cache entries as one file per task under a
// directory, so hits survive process restarts.
//
// Task identifiers are mapped to file paths through an injective,
// root-confined encoding (see entryRelPath), so ids containing path
// separators, "."/".." segments or other hierarchical fragments are
// fully legal and can never escape the store directory or alias another
// id. Different store directories are strictly isolated.
//
// Writes go through a uniquely named temp file followed by an atomic
// rename; an interrupted write therefore leaves at most a "*.tmp"
// orphan, never a partially visible entry. Temp orphans are swept once
// when the store is (re)opened, under an exclusive cross-process lock
// that also serializes concurrent writers, so orphans cannot accumulate
// indefinitely and valid entries are never removed.
type FileStore struct {
	dir string

	// mu serializes writes/sweeps within the process; flock serializes
	// across processes sharing the same directory.
	mu   sync.Mutex
	lock *fileLock
}

// NewFileStore creates (if needed) and returns a FileStore rooted at
// dir. Any interrupted temp files left in dir by a previous crashed
// process are cleaned up; committed entries are left untouched.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err

	}
	fl, err := newFileLock(dir)
	if err != nil {
		return nil, err
	}
	s := &FileStore{dir: dir, lock: fl}

	// Sweep exclusively so a concurrent opener/writer in another process
	// cannot have an in-flight temp file removed.
	if err := s.withLock(s.sweepTempFiles); err != nil {
		return nil, err
	}
	return s, nil
}

// withLock runs fn while holding the in-process mutex and the
// cross-process directory lock.
func (s *FileStore) withLock(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.lock.lock(); err != nil {
		return err
	}
	defer s.lock.unlock()
	return fn()
}

func (s *FileStore) path(task TaskID) string {
	return filepath.Join(s.dir, entryRelPath(task))
}

func (s *FileStore) Get(ctx context.Context, task TaskID) (Entry, error) {
	data, err := os.ReadFile(s.path(task))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Entry{}, ErrCacheMiss
		}
		return Entry{}, err
	}
	entry, err := decodeEntry(data)
	if err != nil {
		// ErrCacheVersion (unknown on-disk format) or ErrCacheCorrupt
		// (bad magic, truncation, checksum, malformed fields).
		return Entry{}, err
	}
	// A structurally valid file whose embedded task id does not match
	// the key it was read under is inconsistent content, not a version
	// problem (defensive: entryRelPath is injective, so this can only
	// arise from a swapped/forged file).
	if entry.Task != task {
		return Entry{}, fmt.Errorf("%w: entry task %q does not match key %q",
			ErrCacheCorrupt, entry.Task, task)
	}
	if entry.Version != Version {
		return entry, fmt.Errorf("%w: entry version %d, want %d",
			ErrCacheVersion, entry.Version, Version)
	}
	return entry, nil
}

func (s *FileStore) Put(ctx context.Context, e Entry) error {
	path := s.path(e.Task)
	return s.withLock(func() error {
		// The hashed layout may require a shard directory.
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		return writeFileRenamed(path, encodeEntry(e))
	})
}

func (s *FileStore) Delete(ctx context.Context, task TaskID) error {
	err := os.Remove(s.path(task))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// sweepTempFiles removes interrupted-write leftovers: temp files at the
// root and inside shard directories. Committed *.cache entries and shard
// directories (which may be in use by another handle) are never removed.
// The caller must hold the exclusive store lock.
func (s *FileStore) sweepTempFiles() error {
	rootEntries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, de := range rootEntries {
		name := de.Name()
		if !de.IsDir() {
			if hasPrefix(name, tmpPrefix) {
				_ = os.Remove(filepath.Join(s.dir, name))
			}
			continue
		}
		if !isShardDir(name) {
			continue
		}
		shard := filepath.Join(s.dir, name)
		entries, err := os.ReadDir(shard)
		if err != nil {
			// The directory may have vanished under us; that is fine.
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return err
		}
		for _, se := range entries {
			if !se.IsDir() && hasPrefix(se.Name(), tmpPrefix) {
				_ = os.Remove(filepath.Join(shard, se.Name()))
			}
		}
	}
	return nil
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// writeFileRenamed writes data to a uniquely named temp file in the same
// directory and atomically renames it into place. Readers therefore
// always observe either the previous complete file or the new complete
// file, never a partially written one. If the process is interrupted
// before the rename, the leftover temp file is reclaimed on the next
// NewFileStore.
func writeFileRenamed(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, tmpPrefix+"*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	cleanup := func() { _ = os.Remove(tmp) }
	if _, err := f.Write(data); err != nil {
		f.Close()
		cleanup()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		cleanup()
		return err
	}
	if err := f.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		cleanup()
		return err
	}
	return nil
}

var _ Store = (*FileStore)(nil)
