package graph

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// TestTaskMismatchIsCorrupt: a structurally valid file placed under the
// wrong key (embedded task id disagrees with the lookup key) is content
// inconsistency, i.e. ErrCacheCorrupt, never a version issue.
func TestTaskMismatchIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store := mustFileStore(t, dir)

	good := Entry{Version: Version, Task: "a",
		Fingerprint: "0000000000000000000000000000000000000000000000000000000000000000",
		Output:      Artifact{Data: []byte("a-out")}}
	if err := store.Put(ctx, good); err != nil {
		t.Fatal(err)
	}
	// Copy a's encoded bytes into b's slot.
	data, err := os.ReadFile(filepath.Join(dir, "a.cache"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.cache"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.Get(ctx, "b")
	if !errors.Is(err, ErrCacheCorrupt) {
		t.Fatalf("want ErrCacheCorrupt for mismatched task, got %v", err)
	}
	if errors.Is(err, ErrCacheVersion) {
		t.Fatal("task mismatch must not be reported as a version problem")
	}
}

// TestBothFailureKindsMissAndSelfHeal drives the engine over a
// version-incompatible entry and a corrupt entry and asserts each is
// treated as a clean miss, recomputed once, and self-healed on disk.
func TestBothFailureKindsMissAndSelfHeal(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(t *testing.T, dir string)
		want    error
	}{
		{
			name: "version",
			corrupt: func(t *testing.T, dir string) {
				p := filepath.Join(dir, "a.cache")
				b, err := os.ReadFile(p)
				if err != nil {
					t.Fatal(err)
				}
				b[4] = 99 // unsupported on-disk format version
				sum := sha256.Sum256(b[:len(b)-32])
				copy(b[len(b)-32:], sum[:])
				if err := os.WriteFile(p, b, 0o644); err != nil {
					t.Fatal(err)
				}
			},
			want: ErrCacheVersion,
		},
		{
			name: "corrupt",
			corrupt: func(t *testing.T, dir string) {
				p := filepath.Join(dir, "a.cache")
				if err := os.WriteFile(p, []byte("garbage-not-an-entry"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			want: ErrCacheCorrupt,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()
			e, _, _ := buildTwoNodeGraph(mustFileStore(t, dir))
			if _, err := e.Run(ctx, "b"); err != nil {
				t.Fatal(err)
			}
			tc.corrupt(t, dir)

			store := mustFileStore(t, dir)
			if _, err := store.Get(ctx, "a"); !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}

			e2, aRuns2, bRuns2 := buildTwoNodeGraph(store)
			res, err := e2.Run(ctx, "b")
			if err != nil {
				t.Fatal(err)
			}
			if res["a"].Cached {
				t.Fatal("bad entry must be a miss")
			}
			if !res["b"].Cached {
				t.Fatal("b unaffected and must hit")
			}
			if got := atomic.LoadInt32(aRuns2); got != 1 {
				t.Fatalf("a ran %d times, want 1", got)
			}
			if got := atomic.LoadInt32(bRuns2); got != 0 {
				t.Fatalf("b ran %d times, want 0", got)
			}
			// Self-healed: a fresh decode now succeeds.
			if _, err := store.Get(ctx, "a"); err != nil {
				t.Fatalf("entry did not self-heal: %v", err)
			}
		})
	}
}

// TestInterruptedTempSweptKeepsValid: interrupted writes leave temp
// files at the root and inside shard directories; reopening the store
// removes them while leaving every committed entry readable.
func TestInterruptedTempSweptKeepsValid(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store := mustFileStore(t, dir)

	// One readable-path entry and one hashed/sharded-path entry.
	entries := []Entry{
		{Version: Version, Task: "a",
			Fingerprint: "0000000000000000000000000000000000000000000000000000000000000001",
			Output:      Artifact{Data: []byte("a")}},
		{Version: Version, Task: "build/linux/deep/task",
			Fingerprint: "0000000000000000000000000000000000000000000000000000000000000002",
			Output:      Artifact{Data: []byte("b")}},
	}
	for _, en := range entries {
		if err := store.Put(ctx, en); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate crashed writers: root temp, shard temp, and an
	// unrelated directory the store must not touch.
	if err := os.WriteFile(filepath.Join(dir, ".tmp-abandoned1"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	shardDir := filepath.Dir(store.path("build/linux/deep/task"))
	if err := os.WriteFile(filepath.Join(shardDir, ".tmp-abandoned2"), []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(dir, "user-data")
	if err := os.MkdirAll(foreign, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "keep"), []byte("z"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Reopen: triggers the sweep.
	store2 := mustFileStore(t, dir)
	for _, en := range entries {
		if _, err := store2.Get(ctx, en.Task); err != nil {
			t.Fatalf("valid entry %q lost during sweep: %v", en.Task, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, ".tmp-abandoned1")); !os.IsNotExist(err) {
		t.Fatalf("root temp not swept: %v", err)
	}
	if _, err := os.Stat(filepath.Join(shardDir, ".tmp-abandoned2")); !os.IsNotExist(err) {
		t.Fatalf("shard temp not swept: %v", err)
	}
	if _, err := os.Stat(filepath.Join(foreign, "keep")); err != nil {
		t.Fatalf("foreign file wrongly removed: %v", err)
	}

	// A subsequent reopen is a no-op: no further leftovers accumulate.
	_ = mustFileStore(t, dir)
}
