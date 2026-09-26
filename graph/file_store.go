package graph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

const (
	// lockFileName is the directory-wide lock file used to serialize
	// writes and residue cleanup across processes.
	lockFileName = ".lock"
	// tmpPrefix marks temp files from interrupted writes.
	tmpPrefix = ".tmp-"
	// entrySuffix terminates every encoded entry file name.
	entrySuffix = ".cache"
	// directKeyLimit keeps ordinary entry names short; longer task IDs
	// are content-hashed instead.
	directKeyLimit = 200
)

// FileStore persists cache entries as one file per task under a
// directory, so hits survive process restarts.
//
// Task IDs are encoded into flat, collision-free file names (only hex
// digits), so IDs containing path separators, "."/".." segments or any
// other byte are all legal and never escape the directory or collide.
// Distinct directories are strictly isolated.
//
// Writes use "locked cleanup + temp file + fsync + atomic rename".
// Temp files left behind by an interrupted writer are removed when the
// directory is reused; valid entries are never touched. A directory
// lock makes the cleanup and write safe under concurrent use from
// multiple processes.
//
// Files that decode as an unknown/old format version report
// ErrCacheVersion; truncated, tampered, checksum-mismatching or
// internally inconsistent files report ErrCacheCorrupt. Both are
// treated as a miss by the engine and the file is overwritten on the
// next successful Put (self-healing).
type FileStore struct {
	dir string

	// mu serializes locked operations between goroutines using this
	// store instance; flock serializes separate instances and separate
	// processes on the same directory.
	mu       sync.Mutex
	lockFile *os.File
}

// NewFileStore creates (if needed) and returns a FileStore rooted at
// dir. Any temp files left in dir by earlier, interrupted writers are
// removed while holding the directory lock; committed entries are left
// untouched.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(dir)
	if err == nil {
		dir = abs
	}
	lf, err := os.OpenFile(filepath.Join(dir, lockFileName),
		os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	s := &FileStore{dir: dir, lockFile: lf}
	if err := s.withLock(func() error {
		return cleanupTempFiles(s.dir)
	}); err != nil {
		_ = lf.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the directory lock handle. It is optional: when the
// process exits the OS releases the lock and closes the file anyway.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockFile == nil {
		return nil
	}
	_ = unlockDir(s.lockFile)
	err := s.lockFile.Close()
	s.lockFile = nil
	return err
}

// entryKey maps a task ID to its flat, collision-free file name.
//
// Ordinary IDs are hex-encoded byte-for-byte (a bijection over task
// IDs, so no two IDs ever collide). Over-long IDs fall back to a
// domain-separated SHA-256 of the ID. The output contains only hex
// digits plus fixed punctuation, hence never a path separator, so the
// file always lands directly inside the cache directory.
func entryKey(task TaskID) string {
	raw := []byte(task)
	if len(raw) <= directKeyLimit {
		return hex.EncodeToString(raw) + entrySuffix
	}
	sum := sha256.Sum256(append([]byte("task:"), raw...))
	return "h-" + hex.EncodeToString(sum[:]) + entrySuffix
}

func (s *FileStore) path(task TaskID) string {
	return filepath.Join(s.dir, entryKey(task))
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
		return Entry{}, err
	}
	// A checksum-valid file stored under this key must actually belong
	// to this task; otherwise its content is inconsistent.
	if entry.Task != task {
		return Entry{}, fmt.Errorf("%w: stored task %q does not match key %q",
			ErrCacheCorrupt, entry.Task, task)
	}
	if err := validateEntryVersion(entry); err != nil {
		return Entry{}, err
	}
	return entry, nil
}

func (s *FileStore) Put(ctx context.Context, e Entry) error {
	data := encodeEntry(e)
	target := s.path(e.Task)
	return s.withLock(func() error {
		// Remove interrupted-write residue before creating our own temp
		// file, so our temp file can never be swept.
		if err := cleanupTempFiles(s.dir); err != nil {
			return err
		}
		return writeFileRenamed(target, data)
	})
}

func (s *FileStore) Delete(ctx context.Context, task TaskID) error {
	err := os.Remove(s.path(task))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// withLock runs fn while holding both the in-process mutex and the
// cross-process advisory directory lock.
func (s *FileStore) withLock(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockFile == nil {
		return errors.New("filestore is closed")
	}
	if err := lockDir(s.lockFile); err != nil {
		return err
	}
	defer func() { _ = unlockDir(s.lockFile) }()
	return fn()
}

// cleanupTempFiles removes regular files left behind by interrupted
// writes (the temp files created by writeFileRenamed). It only matches
// the tmpPrefix and never descends into subdirectories or touches
// committed entries or the lock file.
func cleanupTempFiles(dir string) error {
	names, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, n := range names {
		name := n.Name()
		if len(name) < len(tmpPrefix) || name[:len(tmpPrefix)] != tmpPrefix {
			continue
		}
		// Only remove plain files; ignore directories, symlinks, etc.
		if t, lerr := n.Info(); lerr == nil && t.Mode().IsRegular() {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
	return nil
}

// writeFileRenamed writes data to a temp file in the same directory and
// atomically renames it into place. Readers therefore always observe
// either the previous complete file or the new complete file, never a
// partially written one.
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
