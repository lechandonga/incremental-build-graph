package graph

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// hierarchicalID builds a path-style task ID of exactly n bytes: short
// repeated "seg/" fragments finished by a leaf fragment, so it contains
// real path separators and hierarchy like production task names.
func hierarchicalID(n int) TaskID {
	var b strings.Builder
	for b.Len()+4 <= n {
		b.WriteString("seg/")
	}
	b.WriteString(strings.Repeat("l", n-b.Len()))
	return TaskID(b.String())
}

// mediumIDLengths spans the range the old direct/hash threshold got
// wrong: IDs in [125,200] produced file names longer than the 255-byte
// filesystem component limit (hex doubling + suffix), while both shorter
// and much longer IDs worked. It also includes hash-only lengths far
// beyond any possible direct name, to prove arbitrary lengths work.
func mediumIDLengths() []int {
	return []int{124, 125, 126, 130, 150, 160, 180, 199, 200, 201, 250, 400, 2000}
}

// TestMediumHierarchicalTaskIDsRoundTrip is the regression test for the
// "medium length" failure: every legal path-style ID, including ones in
// the 125..200 byte range, must write/hit/restart/delete, and its entry
// file name must fit the common 255-byte filesystem component limit.
func TestMediumHierarchicalTaskIDsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	var ids []TaskID
	for _, n := range mediumIDLengths() {
		id := hierarchicalID(n)
		if len([]byte(id)) != n {
			t.Fatalf("test helper produced %d bytes, want %d", len([]byte(id)), n)
		}
		ids = append(ids, id)
	}

	// The encoded file name must be legal for filesystems whose
	// NAME_MAX is 255 (ext4, tmpfs, xfs, ...).
	for _, id := range ids {
		if got := len(entryKey(id)); got > 255 {
			t.Fatalf("entryKey(%d-byte id) is %d bytes, exceeds 255", len([]byte(id)), got)
		}
	}

	s1 := mustFileStore(t, dir)
	for _, id := range ids {
		e := Entry{
			Version:     Version,
			Task:        id,
			Fingerprint: validFP,
			Output:      Artifact{Data: []byte("payload-" + string(id))},
		}
		if err := s1.Put(ctx, e); err != nil {
			t.Fatalf("Put(%d-byte hierarchical id): %v", len([]byte(id)), err)
		}
	}
	assertContained(t, dir)

	// No half-written residue may remain.
	for _, n := range listNames(t, dir) {
		if strings.HasPrefix(n, tmpPrefix) {
			t.Fatalf("temp residue left behind: %q", n)
		}
	}

	// Same-instance hits.
	for _, id := range ids {
		got, err := s1.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get(%d-byte id): %v", len([]byte(id)), err)
		}
		if got.Task != id || string(got.Output.Data) != "payload-"+string(id) {
			t.Fatalf("round trip mismatch for %d-byte id", len([]byte(id)))
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// Distinct IDs must never overwrite one another: exactly one entry
	// file per ID (plus the lock file).
	if infos, err := os.ReadDir(dir); err != nil {
		t.Fatal(err)
	} else {
		entryFiles := 0
		for _, in := range infos {
			if strings.HasSuffix(in.Name(), entrySuffix) {
				entryFiles++
			}
		}
		if entryFiles != len(ids) {
			t.Fatalf("got %d entry files, want %d (collision/overwrite?)", entryFiles, len(ids))
		}
	}

	// Restart: a new store over the same directory must still hit.
	s2 := mustFileStore(t, dir)
	for _, id := range ids {
		got, err := s2.Get(ctx, id)
		if err != nil {
			t.Fatalf("after restart Get(%d-byte id): %v", len([]byte(id)), err)
		}
		if string(got.Output.Data) != "payload-"+string(id) {
			t.Fatalf("after restart payload mismatch for %d-byte id", len([]byte(id)))
		}
	}

	// Delete must work and be idempotent.
	for _, id := range ids {
		if err := s2.Delete(ctx, id); err != nil {
			t.Fatalf("Delete(%d-byte id): %v", len([]byte(id)), err)
		}
		if _, err := s2.Get(ctx, id); !errors.Is(err, ErrCacheMiss) {
			t.Fatalf("deleted %d-byte id: expected miss, got %v", len([]byte(id)), err)
		}
		if err := s2.Delete(ctx, id); err != nil {
			t.Fatalf("second Delete should be idempotent: %v", err)
		}
	}
}

// TestMediumIDsIsolatedAcrossDirectories checks the previously broken
// length range is still strictly isolated between cache directories.
func TestMediumIDsIsolatedAcrossDirectories(t *testing.T) {
	d1, d2 := t.TempDir(), t.TempDir()
	ctx := context.Background()
	s1, s2 := mustFileStore(t, d1), mustFileStore(t, d2)

	var ids []TaskID
	for _, n := range mediumIDLengths() {
		ids = append(ids, hierarchicalID(n))
	}
	for _, id := range ids {
		if err := s1.Put(ctx, Entry{
			Version: Version, Task: id, Fingerprint: validFP,
			Output: Artifact{Data: []byte("in-d1")},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := s2.Get(ctx, id); !errors.Is(err, ErrCacheMiss) {
			t.Fatalf("directory leak for %d-byte id: %v", len([]byte(id)), err)
		}
		if err := s2.Put(ctx, Entry{
			Version: Version, Task: id, Fingerprint: validFP,
			Output: Artifact{Data: []byte("in-d2")},
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		g1, err := s1.Get(ctx, id)
		if err != nil || string(g1.Output.Data) != "in-d1" {
			t.Fatalf("d1 entry broken/overwritten for %d-byte id: %+v %v", len([]byte(id)), g1, err)
		}
		g2, err := s2.Get(ctx, id)
		if err != nil || string(g2.Output.Data) != "in-d2" {
			t.Fatalf("d2 entry mismatch for %d-byte id: %+v %v", len([]byte(id)), g2, err)
		}
	}
}

// TestMediumIDsDoNotDisturbOtherEntries verifies medium-ID writes and
// deletes leave valid entries of other (short and long) IDs untouched,
// and never leave residue.
func TestMediumIDsDoNotDisturbOtherEntries(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustFileStore(t, dir)

	others := []TaskID{
		"short/name",
		hierarchicalID(10),
		hierarchicalID(directKeyLimit + 50),
	}
	for _, id := range others {
		if err := s.Put(ctx, Entry{
			Version: Version, Task: id, Fingerprint: validFP,
			Output: Artifact{Data: []byte("keep-" + string(id)[:min(len(id), 8)])},
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, n := range mediumIDLengths() {
		id := hierarchicalID(n)
		if err := s.Put(ctx, Entry{
			Version: Version, Task: id, Fingerprint: validFP,
			Output: Artifact{Data: []byte("med")},
		}); err != nil {
			t.Fatalf("Put medium: %v", err)
		}
		if err := s.Delete(ctx, id); err != nil {
			t.Fatalf("Delete medium: %v", err)
		}
	}

	for _, id := range others {
		if _, err := s.Get(ctx, id); err != nil {
			t.Fatalf("unrelated entry %q damaged: %v", id, err)
		}
	}
	for _, n := range listNames(t, dir) {
		if strings.HasPrefix(n, tmpPrefix) {
			t.Fatalf("temp residue: %q", n)
		}
	}
}

// TestMediumHierarchicalIDThroughEngine runs the full graph with a
// medium-length hierarchical ID and verifies cross-restart hits.
func TestMediumHierarchicalIDThroughEngine(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	leaf := hierarchicalID(160)
	root := TaskID("out/" + string(leaf)) // root is ~164 bytes, also medium

	build := func() (*Engine, *int32, *int32) {
		en := NewEngine(WithStore(mustFileStore(t, dir)))
		var leafRuns, rootRuns int32
		en.Register(Task{
			ID: leaf, Inputs: []byte("hello"),
			Run: func(ctx context.Context, in Inputs) (Artifact, error) {
				atomic.AddInt32(&leafRuns, 1)
				return Artifact{Data: []byte("leaf-data")}, nil
			},
		})
		en.Register(Task{
			ID: root, Deps: []TaskID{leaf}, Inputs: []byte("cfg"),
			Run: func(ctx context.Context, in Inputs) (Artifact, error) {
				atomic.AddInt32(&rootRuns, 1)
				a, _ := in.Artifact(leaf)
				return Artifact{Data: append(append([]byte(nil), a.Data...), '/')}, nil
			},
		})
		return en, &leafRuns, &rootRuns
	}

	e1, leaf1, root1 := build()
	if _, err := e1.Run(ctx, root); err != nil {
		t.Fatalf("first run with medium id: %v", err)
	}

	e2, leaf2, root2 := build()
	res, err := e2.Run(ctx, root)
	if err != nil {
		t.Fatalf("restart run with medium id: %v", err)
	}
	if !res[leaf].Cached || !res[root].Cached {
		t.Fatalf("both tasks must hit after restart: %+v", res)
	}
	if atomic.LoadInt32(leaf2) != 0 || atomic.LoadInt32(root2) != 0 {
		t.Fatalf("tasks executed after restart: leaf=%d root=%d",
			atomic.LoadInt32(leaf2), atomic.LoadInt32(root2))
	}
	if atomic.LoadInt32(leaf1) != 1 || atomic.LoadInt32(root1) != 1 {
		t.Fatalf("first run counters: leaf=%d root=%d",
			atomic.LoadInt32(leaf1), atomic.LoadInt32(root1))
	}
}

// TestMediumIDsConcurrentSameDirectory exercises concurrent independent
// stores (like multiple processes) hammering keys from the broken length
// range alongside shorter keys: no overwrite, no partial reads, no
// residue.
func TestMediumIDsConcurrentSameDirectory(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	const goroutines = 6
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s, err := NewFileStore(dir)
			if err != nil {
				errCh <- err
				return
			}
			for _, n := range mediumIDLengths() {
				id := TaskID("g" + itoa(g) + "/" + string(hierarchicalID(n)))
				payload := itoa(g) + ":" + itoa(n)
				if err := s.Put(ctx, Entry{
					Version: Version, Task: id, Fingerprint: validFP,
					Output: Artifact{Data: []byte(payload)},
				}); err != nil {
					errCh <- err
					return
				}
				got, err := s.Get(ctx, id)
				if err != nil {
					errCh <- err
					return
				}
				if string(got.Output.Data) != payload {
					errCh <- errMismatch
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	for _, n := range listNames(t, dir) {
		if strings.HasPrefix(n, tmpPrefix) {
			t.Fatalf("temp residue after concurrent run: %q", n)
		}
	}
	// Every file in the directory must still be a decodable, consistent
	// entry: no partial content ever became visible.
	infos, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range infos {
		name := in.Name()
		if !strings.HasSuffix(name, entrySuffix) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		entry, err := decodeEntry(data)
		if err != nil {
			t.Fatalf("entry file %q not decodable: %v", name, err)
		}
		if entryKey(entry.Task) != name {
			t.Fatalf("entry file %q does not match its task key", name)
		}
	}
}

// TestMediumEntryKeysAreFlatCollisionFreeAndBounded guards the key
// encoding around the length boundary: keys are separator-free, fit the
// 255-byte component limit, and distinct hierarchical IDs never map to
// the same file name (including across the direct/hashed boundary).
func TestMediumEntryKeysAreFlatCollisionFreeAndBounded(t *testing.T) {
	seen := make(map[string]TaskID)
	check := func(id TaskID) {
		k := entryKey(id)
		if strings.ContainsAny(k, `/\`) {
			t.Fatalf("key for %d-byte id contains a separator: %q", len([]byte(id)), k)
		}
		if len(k) > 255 {
			t.Fatalf("key for %d-byte id is %d bytes, exceeds 255", len([]byte(id)), len(k))
		}
		if other, dup := seen[k]; dup {
			t.Fatalf("key collision: %d-byte id and %d-byte id both map to %q",
				len([]byte(other)), len([]byte(id)), k)
		}
		seen[k] = id
	}
	for _, n := range mediumIDLengths() {
		check(hierarchicalID(n))
	}
	// Exact direct/hashed boundary IDs and their neighbours, built with a
	// different pattern so they are distinct from the hierarchical set.
	for n := directKeyLimit - 1; n <= directKeyLimit+1; n++ {
		check(TaskID("q/" + strings.Repeat("q", n-2)))
	}
}
