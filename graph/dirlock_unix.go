//go:build !windows

package graph

import "syscall"

// lockDir acquires an advisory exclusive lock on f (the directory's
// lock file). It serializes writers/cleaners across processes using the
// same cache directory. Locks are released by unlockDir (or
// automatically when the process dies), so a crashed writer never
// wedges the directory.
func lockDir(f interface {
	Fd() uintptr
}) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
}

// unlockDir releases the lock acquired by lockDir.
func unlockDir(f interface {
	Fd() uintptr
}) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
