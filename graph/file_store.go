package graph

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// FileStore persists cache entries as one file per task under a
// directory, so hits survive process restarts.
//
// Files with a bad/unknown version, truncated bodies or checksum
// mismatches decode as ErrCacheCorrupt / ErrCacheVersion, which the
// engine treats as misses; the stale file is overwritten on the next
// successful Put.
type FileStore struct {
	dir string
}

// NewFileStore creates (if needed) and returns a FileStore rooted at
// dir.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(task TaskID) string {
	return filepath.Join(s.dir, string(task)+".cache")
}

func (s *FileStore) Get(ctx context.Context, task TaskID) (Entry, error) {
	data, err := os.ReadFile(s.path(task))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Entry{}, ErrCacheMiss
		}
		return Entry{}, err
	}
	return decodeEntry(data)
}

func (s *FileStore) Put(ctx context.Context, e Entry) error {
	data := encodeEntry(e)
	return writeFileRenamed(s.path(e.Task), data)
}

func (s *FileStore) Delete(ctx context.Context, task TaskID) error {
	err := os.Remove(s.path(task))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// writeFileRenamed writes data to a temp file in the same directory and
// atomically renames it into place. Readers therefore always observe
// either the previous complete file or the new complete file, never a
// partially written one.
func writeFileRenamed(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".tmp-*")
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
