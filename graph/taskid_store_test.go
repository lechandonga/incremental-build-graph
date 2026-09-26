package graph

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// weirdIDs are task identifiers that contain path separators, parent
// references, leading separators, NUL-ish punctuation, an empty ID and
// an over-long ID. All of them must be legal cache keys.
func weirdIDs() []TaskID {
	long := strings.Repeat("z", directKeyLimit+50)
	return []TaskID{
		"",
		"build/pkg/a",
		"build/pkg/a/",
		"../escape",
		"x/../../y",
		"a/./b",
		"/etc/passwd",
		"C:\\Windows\\System32",
		"..",
		".",
		"foo..bar",
		"name with spaces",
		"uni\u00e9/\u2603",
		TaskID(long),
	}
}

// assertContained verifies the store created no files outside dir and no
// subdirectories (only the flat entry files plus the lock file).
func assertContained(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, en := range entries {
		if en.IsDir() {
			t.Errorf("store created a subdirectory %q; keys must be flat", en.Name())
		}
	}
}

func TestArbitraryTaskIDsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store := mustFileStore(t, dir)

	ids := weirdIDs()
	for _, id := range ids {
		e := Entry{
			Version:     Version,
			Task:        id,
			Fingerprint: validFP,
			Output:      Artifact{Data: []byte("payload-" + string(id))},
		}
		if err := store.Put(ctx, e); err != nil {
			t.Fatalf("Put(%q): %v", id, err)
		}
	}
	assertContained(t, dir)

	// Nothing escaped the directory: the temp dir parent must not gain
	// files named like our IDs.
	parent, err := os.ReadDir(filepath.Dir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(parent) != 1 || parent[0].Name() != filepath.Base(dir) {
		t.Errorf("cache writes escaped dir; parent contents: %v", parent)
	}

	// All keys hit in the same instance.
	for _, id := range ids {
		got, err := store.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get(%q): %v", id, err)
		}
		if got.Task != id || string(got.Output.Data) != "payload-"+string(id) {
			t.Fatalf("round trip mismatch for %q: %+v", id, got)
		}
	}
}

func TestArbitraryTaskIDsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	ids := weirdIDs()

	s1 := mustFileStore(t, dir)
	for _, id := range ids {
		if err := s1.Put(ctx, Entry{
			Version:     Version,
			Task:        id,
			Fingerprint: validFP,
			Output:      Artifact{Data: []byte("v1-" + string(id))},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// "Restart": a brand new store over the same directory.
	s2 := mustFileStore(t, dir)
	for _, id := range ids {
		got, err := s2.Get(ctx, id)
		if err != nil {
			t.Fatalf("after restart Get(%q): %v", id, err)
		}
		if string(got.Output.Data) != "v1-"+string(id) {
			t.Fatalf("after restart %q payload mismatch: %q", id, got.Output.Data)
		}
	}

	// Delete each key across restart; afterwards every key is a clean
	// miss, and deleting again is a no-op error.
	for _, id := range ids {
		if err := s2.Delete(ctx, id); err != nil {
			t.Fatalf("Delete(%q): %v", id, err)
		}
		if err := s2.Delete(ctx, id); err != nil {
			t.Fatalf("second Delete(%q) should be idempotent: %v", id, err)
		}
		if _, err := s2.Get(ctx, id); !errors.Is(err, ErrCacheMiss) {
			t.Fatalf("deleted %q: expected ErrCacheMiss, got %v", id, err)
		}
	}
}

func TestCacheDirectoriesAreIsolated(t *testing.T) {
	d1, d2 := t.TempDir(), t.TempDir()
	ctx := context.Background()
	s1, s2 := mustFileStore(t, d1), mustFileStore(t, d2)
	ids := weirdIDs()

	for _, id := range ids {
		if err := s1.Put(ctx, Entry{
			Version:     Version,
			Task:        id,
			Fingerprint: validFP,
			Output:      Artifact{Data: []byte("in-d1")},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// d2 must see none of d1's entries.
	for _, id := range ids {
		if _, err := s2.Get(ctx, id); !errors.Is(err, ErrCacheMiss) {
			t.Fatalf("directory leak: d2 could read %q from d1 (err=%v)", id, err)
		}
	}

	// Writing the same key with different bytes in d2 must not overwrite
	// d1's entry.
	for _, id := range ids {
		if err := s2.Put(ctx, Entry{
			Version:     Version,
			Task:        id,
			Fingerprint: validFP,
			Output:      Artifact{Data: []byte("in-d2")},
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		g1, err := s1.Get(ctx, id)
		if err != nil {
			t.Fatalf("d1 entry vanished for %q: %v", id, err)
		}
		if string(g1.Output.Data) != "in-d1" {
			t.Fatalf("d1 entry for %q overwritten: %q", id, g1.Output.Data)
		}
		g2, err := s2.Get(ctx, id)
		if err != nil || string(g2.Output.Data) != "in-d2" {
			t.Fatalf("d2 entry mismatch for %q: %+v err=%v", id, g2, err)
		}
	}
}

// TestEntryKeysAreCollisionFree guards the flat encoding: distinct task
// IDs must map to distinct file names.
func TestEntryKeysAreCollisionFree(t *testing.T) {
	seen := make(map[string]TaskID)
	for _, id := range weirdIDs() {
		k := entryKey(id)
		if strings.ContainsAny(k, "/\\") {
			t.Fatalf("key for %q contains a separator: %q", id, k)
		}
		if other, dup := seen[k]; dup {
			t.Fatalf("key collision between %q and %q (%q)", other, id, k)
		}
		seen[k] = id
	}
}

// TestWeirdIDsThroughEngine exercises hierarchical IDs in full graph
// runs, including a downstream dependency, across a simulated restart.
func TestWeirdIDsThroughEngine(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	build := func() (*Engine, map[TaskID]*int32) {
		en := NewEngine(WithStore(mustFileStore(t, dir)))
		counters := map[TaskID]*int32{}
		for _, id := range []TaskID{"src/x", "out/bin/app"} {
			id := id
			var c int32
			counters[id] = &c
			var deps []TaskID
			var in0 []byte
			if id == "out/bin/app" {
				deps = []TaskID{"src/x"}
				in0 = []byte("cfg")
			} else {
				in0 = []byte("hello")
			}
			en.Register(Task{
				ID: id, Deps: deps, Inputs: in0,
				Run: func(ctx context.Context, in Inputs) (Artifact, error) {
					atomic.AddInt32(counters[id], 1)
					data := []byte("done:" + string(id))
					if a, ok := in.Artifact("src/x"); ok {
						data = append(append([]byte(nil), a.Data...), '/', byte(len(id)))
					}
					return Artifact{Data: data}, nil
				},
			})
		}
		return en, counters
	}

	e1, c1 := build()
	if _, err := e1.Run(ctx, "out/bin/app"); err != nil {
		t.Fatal(err)
	}

	e2, c2 := build()
	res, err := e2.Run(ctx, "out/bin/app")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []TaskID{"src/x", "out/bin/app"} {
		if !res[id].Cached {
			t.Errorf("%s must hit persisted cache after restart", id)
		}
		if atomic.LoadInt32(c2[id]) != 0 {
			t.Errorf("%s executed %d times after restart, want 0", id, atomic.LoadInt32(c2[id]))
		}
		if atomic.LoadInt32(c1[id]) != 1 {
			t.Errorf("%s first-run executions=%d want 1", id, atomic.LoadInt32(c1[id]))
		}
	}
}
