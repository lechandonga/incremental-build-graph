package graph

import "errors"

// ErrUnknownTask is returned when a referenced task is not registered.
var ErrUnknownTask = errors.New("unknown task")

// ErrTaskFailed wraps the execution error of a task.
var ErrTaskFailed = errors.New("task execution failed")

// ErrCacheMiss indicates that no usable cache entry exists for a task.
var ErrCacheMiss = errors.New("cache miss")

// ErrCacheVersion is returned by cache stores when an entry uses an
// unsupported format version. It is treated as a cache miss.
var ErrCacheVersion = errors.New("incompatible cache entry version")

// ErrCacheCorrupt is returned when a cache entry cannot be decoded. It
// is treated as a cache miss.
var ErrCacheCorrupt = errors.New("corrupt cache entry")

// TaskError pairs a task ID with the error that caused it to fail.
type TaskError struct {
	Task TaskID
	Err  error
}

func (e *TaskError) Error() string {
	return "task " + string(e.Task) + " failed: " + e.Err.Error()
}

func (e *TaskError) Unwrap() error { return e.Err }
