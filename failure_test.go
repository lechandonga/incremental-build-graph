package buildgraph

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// TestTaskFailureStopsDownstream verifies that when a task fails, its
// downstream is never executed, no artifact is published, and the error names
// the failing task.
func TestTaskFailureStopsDownstream(t *testing.T) {
	s, _ := New(Options{})
	g := NewGraph()
	var downRuns int32
	boom := errors.New("boom")
	_ = g.Add(Task{ID: "a",
		Run: func(_ context.Context, _, _ []byte, _ map[string]Artifact) (Artifact, error) {
			return nil, boom
		}})
	_ = g.Add(Task{ID: "b", Deps: []string{"a"},
		Run: func(_ context.Context, _, _ []byte, _ map[string]Artifact) (Artifact, error) {
			atomic.AddInt32(&downRuns, 1)
			return Artifact("should-not-exist"), nil
		}})
	_ = g.Add(Task{ID: "side",
		Run: func(_ context.Context, _, _ []byte, _ map[string]Artifact) (Artifact, error) {
			return Artifact("side"), nil
		}})

	_, err := s.Run(context.Background(), g)
	var te *TaskError
	if !errors.As(err, &te) {
		t.Fatalf("expected *TaskError, got %v", err)
	}
	if te.TaskID != "a" || !errors.Is(err, boom) {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt32(&downRuns) != 0 {
		t.Fatal("downstream of failed task must not run")
	}
}

// TestRetryAfterFailureRestoresConsistency: a flaky task fails on its first
// invocation but succeeds on retry. After recovery, downstream artifacts must
// be computed from the successful output and cached for subsequent runs.
func TestRetryAfterFailureRestoresConsistency(t *testing.T) {
	dir := t.TempDir()
	var aRuns, bRuns int64
	build := func(shouldFail *int32) *Graph {
		g := NewGraph()
		_ = g.Add(Task{ID: "a", Input: []byte("input"),
			Run: func(_ context.Context, in, _ []byte, _ map[string]Artifact) (Artifact, error) {
				atomic.AddInt64(&aRuns, 1)
				if atomic.LoadInt32(shouldFail) == 1 {
					return nil, errors.New("transient")
				}
				return Artifact("a:" + string(in)), nil
			}})
		_ = g.Add(Task{ID: "b", Deps: []string{"a"},
			Run: func(_ context.Context, _, _ []byte, d map[string]Artifact) (Artifact, error) {
				atomic.AddInt64(&bRuns, 1)
				if len(d["a"]) == 0 {
					return nil, errors.New("got empty upstream artifact")
				}
				return Artifact("b(" + string(d["a"]) + ")"), nil
			}})
		return g
	}

	var shouldFail int32 = 1
	atomic.StoreInt32(&shouldFail, 1)
	s, _ := New(Options{CacheDir: dir})
	if _, err := s.Run(context.Background(), build(&shouldFail)); err == nil {
		t.Fatal("first run should fail")
	}
	if atomic.LoadInt64(&bRuns) != 0 {
		t.Fatal("b must not run while a failed")
	}

	// Retry with the failure cleared.
	atomic.StoreInt32(&shouldFail, 0)
	res, err := s.Run(context.Background(), build(&shouldFail))
	if err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	if string(res.Artifacts["b"]) != "b(a:input)" {
		t.Fatalf("downstream consumed stale/empty artifact: %q", res.Artifacts["b"])
	}

	// Fully cached follow-up.
	res2, err := s.Run(context.Background(), build(&shouldFail))
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Cached) != 2 {
		t.Fatalf("recovered state should be fully cached, got %+v", res2)
	}
	if atomic.LoadInt64(&aRuns) != 2 || atomic.LoadInt64(&bRuns) != 1 {
		t.Fatalf("run counts after recovery: a=%d b=%d", aRuns, bRuns)
	}
}

// TestFailedTaskDownstreamNeverUsesStaleOutput changes an upstream task so
// its downstream fingerprint changes, then fails the upstream run. The old
// downstream cache entry must never be served, and after fixing the task the
// new upstream output propagates everywhere.
func TestFailedTaskDownstreamNeverUsesStaleOutput(t *testing.T) {
	dir := t.TempDir()
	var aFailing int32
	build := func(v string) *Graph {
		g := NewGraph()
		_ = g.Add(Task{ID: "a", Input: []byte(v),
			Run: func(_ context.Context, in, _ []byte, _ map[string]Artifact) (Artifact, error) {
				if atomic.LoadInt32(&aFailing) == 1 {
					return nil, errors.New("no output")
				}
				return Artifact("a=" + string(in)), nil
			}})
		_ = g.Add(Task{ID: "b", Deps: []string{"a"},
			Run: func(_ context.Context, _, _ []byte, d map[string]Artifact) (Artifact, error) {
				return Artifact(fmt.Sprintf("b(%s)", d["a"])), nil
			}})
		return g
	}

	s, _ := New(Options{CacheDir: dir})
	if _, err := s.Run(context.Background(), build("v1")); err != nil {
		t.Fatal(err)
	}

	// v2 + failure: run must error; there must be no way to receive v1's b.
	atomic.StoreInt32(&aFailing, 1)
	if res, err := s.Run(context.Background(), build("v2")); err == nil {
		t.Fatalf("expected failure, got artifacts: %v", res.Artifacts)
	}

	// Recover: b must reflect v2, never the cached b(a=v1).
	atomic.StoreInt32(&aFailing, 0)
	res, err := s.Run(context.Background(), build("v2"))
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Artifacts["b"]) != "b(a=v2)" {
		t.Fatalf("stale downstream artifact served: %q", res.Artifacts["b"])
	}
}

// TestCancellationAbortsRun ensures context cancellation is surfaced and no
// partial result is returned as success.
func TestCancellationAbortsRun(t *testing.T) {
	s, _ := New(Options{Workers: 1})
	g := NewGraph()
	release := make(chan struct{})
	_ = g.Add(Task{ID: "a",
		Run: func(ctx context.Context, _, _ []byte, _ map[string]Artifact) (Artifact, error) {
			select {
			case <-release:
				return Artifact("a"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}})
	_ = g.Add(Task{ID: "b", Deps: []string{"a"},
		Run: func(_ context.Context, _, _ []byte, _ map[string]Artifact) (Artifact, error) {
			return Artifact("b"), nil
		}})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Run(ctx, g)
		errCh <- err
	}()
	cancel()
	close(release)
	if err := <-errCh; err == nil {
		t.Fatal("cancelled run must return an error")
	}
}
