//go:build windows

package graph

import "os"

// fileLock is a no-op on Windows: concurrent FileStore use within one
// process is still serialized by FileStore.mu. Cross-process sharing of
// one cache directory on Windows relies on atomic rename semantics.
type fileLock struct {
	f *os.File
}

func newFileLock(dir string) (*fileLock, error) {
	f, err := os.OpenFile(lockPath(dir), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) lock() error   { return nil }
func (l *fileLock) unlock() error { return nil }
