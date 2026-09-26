//go:build !windows

package graph

import (
	"os"
	"syscall"
)

// fileLock is an advisory, process-scoped lock over a store directory
// implemented with flock(2). Within one process, FileStore additionally
// serializes with a mutex; flock only needs to exclude other processes.
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

func (l *fileLock) lock() error {
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_EX)
}

func (l *fileLock) unlock() error {
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
