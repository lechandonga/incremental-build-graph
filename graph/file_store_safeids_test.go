package graph

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestEntryRelPathInjective checks that distinct (including
// path-separator-bearing) identifiers never map to the same file, and
// never escape the store root.
func TestEntryRelPathInjective(t *testing.T) {
	ids := []TaskID{
		"", "a", "a/b", "a%2Fb", "a\\b", "a/../b", "..", ".", "/abs",
		"trailing/", "x//y", "deep/nested/id", "nul", "con", "com1",
		"UPPER", "a.b.c", "a b", "a:b", TaskID(strings.Repeat("x", 300)),
		"a\x00b", "%2e%2e", "a/", "/",
	}
	seen := make(map[string]TaskID, len(ids))
	for _, id := range ids {
		rel := filepath.FromSlash(entryRelPath(id))
		clean := filepath.Clean(rel)
		if clean == "." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) ||
			clean == ".." || filepath.IsAbs(clean) {
			t.Fatalf("id %q escapes root: rel=%q", id, rel)
		}
		if other, ok := seen[rel]; ok {
			t.Fatalf("collision between %q and %q -> %q", other, id, rel)
		}
		seen[rel] = id
	}
}

// TestPathLikeTaskIDs verifies identifiers containing path separators or
// hierarchical fragments work end to end: put, get, hit across restart,
// delete.
func TestPathLikeTaskIDs(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	ids := []TaskID{
		"build/linux/x86_64/compile",
		"..",
		".",
		"a/../b",
		"/leading-slash",
		"trailing-slash/",
		"windows\\style\\path",
		"nul",
		"UPPER/CASE",
		TaskID(strings.Repeat("very-long/", 40) + "tail"),
		"with space/and dot.file",
		"pct%2fencoded",
		"",
	}

	store := mustFileStore(t, dir)
	for i, id := range ids {
		e := Entry{
			Version:     Version,
			Task:        id,
			Fingerprint: "0000000000000000000000000000000000000000000000000000000000000000",
			Output:      Artifact{Data: []byte("out-" + string(rune('a'+i%26)))},
		}
		if err := store.Put(ctx, e); err != nil {
			t.Fatalf("put %q: %v", id, err)
		}
	}

	// Immediate get.
	for _, id := range ids {
		if _, err := store.Get(ctx, id); err != nil {
			t.Fatalf("get %q: %v", id, err)
		}
	}

	// Simulate restart: nothing on disk may be lost.
	store2 := mustFileStore(t, dir)
	for _, id := range ids {
		got, err := store2.Get(ctx, id)
		if err != nil {
			t.Fatalf("after restart get %q: %v", id, err)
		}
		if got.Task != id {
			t.Fatalf("id round trip: got %q want %q", got.Task, id)
		}
	}

	// Every stored file must physically live under dir.
	if err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil || strings.HasPrefix(rel, "..") {
			t.Fatalf("file escaped root: %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Delete each one; subsequent get is a clean miss.
	for _, id := range ids {
		if err := store2.Delete(ctx, id); err != nil {
			t.Fatalf("delete %q: %v", id, err)
		}
		if _, err := store2.Get(ctx, id); err != ErrCacheMiss {
			t.Fatalf("get %q after delete = %v, want ErrCacheMiss", id, err)
		}
	}
}

// TestStoreDirectoriesIsolated: an entry in one directory can never be
// read or overwritten through a store rooted at another directory.
func TestStoreDirectoriesIsolated(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	ctx := context.Background()

	s1 := mustFileStore(t, dir1)
	s2 := mustFileStore(t, dir2)
	id := TaskID("shared/nested/id")

	e1 := Entry{Version: Version, Task: id, Fingerprint: "0000000000000000000000000000000000000000000000000000000000000001",
		Output: Artifact{Data: []byte("from-dir-1")}}
	if err := s1.Put(ctx, e1); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Get(ctx, id); err != ErrCacheMiss {
		t.Fatalf("dir2 must not see dir1 entry, got %v", err)
	}

	// Writing through dir2 must not touch dir1.
	e2 := Entry{Version: Version, Task: id, Fingerprint: "0000000000000000000000000000000000000000000000000000000000000002",
		Output: Artifact{Data: []byte("from-dir-2")}}
	if err := s2.Put(ctx, e2); err != nil {
		t.Fatal(err)
	}
	got1, err := s1.Get(ctx, id)
	if err != nil {
		t.Fatalf("dir1 entry lost: %v", err)
	}
	if string(got1.Output.Data) != "from-dir-1" {
		t.Fatalf("dir1 entry overwritten by dir2: %q", got1.Output.Data)
	}
	got2, err := s2.Get(ctx, id)
	if err != nil || string(got2.Output.Data) != "from-dir-2" {
		t.Fatalf("dir2 entry wrong: %v %q", err, got2.Output.Data)
	}

	// Deleting in dir2 leaves dir1 intact.
	if err := s2.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Get(ctx, id); err != nil {
		t.Fatalf("dir1 entry deleted via dir2: %v", err)
	}
}

// TestConcurrentStoresSameDirectory exercises concurrent writers and
// (re)openers on one directory: sweeps must never remove committed
// entries or in-flight temp files, and no temp files may remain.
func TestConcurrentStoresSameDirectory(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	base := mustFileStore(t, dir)
	const writers, openers, rounds = 8, 8, 25
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				id := TaskID("task/" + string(rune('a'+w)) + "/" +
					string(rune('a'+i%26)))
				e := Entry{Version: Version, Task: id, Fingerprint: "0000000000000000000000000000000000000000000000000000000000000000",
					Output: Artifact{Data: []byte{byte(w), byte(i)}}}
				if err := base.Put(ctx, e); err != nil {
					t.Errorf("put: %v", err)
					return
				}
				if _, err := base.Get(ctx, id); err != nil {
					t.Errorf("get: %v", err)
					return
				}
			}
		}(w)
	}
	for o := 0; o < openers; o++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				if _, err := NewFileStore(dir); err != nil {
					t.Errorf("reopen: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// No temp leftovers anywhere in the directory.
	if err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), tmpPrefix) {
			t.Errorf("temp file left behind: %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// All committed entries still readable through a fresh store.
	final := mustFileStore(t, dir)
	for w := 0; w < writers; w++ {
		for i := 0; i < rounds; i++ {
			id := TaskID("task/" + string(rune('a'+w)) + "/" +
				string(rune('a'+i%26)))
			if _, err := final.Get(ctx, id); err != nil {
				t.Fatalf("entry lost after concurrent use: %q: %v", id, err)
			}
		}
	}
}
