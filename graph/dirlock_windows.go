//go:build windows

package graph

// On Windows cross-process advisory locking is provided by the sharing
// mode of the lock file itself; the in-process mutex still serializes
// multiple stores in one process. These are therefore no-ops.
func lockDir(f interface{ Fd() uintptr }) error   { return nil }
func unlockDir(f interface{ Fd() uintptr }) error { return nil }
