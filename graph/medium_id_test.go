package graph

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
)

// mediumIDs returns hierarchical, path-style task identifiers whose
// lengths straddle the old broken window: with directKeyLimit = 200 the
// hex-encoded file names of IDs between 125 and 200 bytes exceeded the
// 255-byte NAME_MAX and failed with "file name too long", while shorter
// and hashed longer IDs worked. Every length in 1..260 must work.
func mediumIDs() []TaskID {
	var ids []TaskID
	// Hierarchical IDs of every length from 1 to 260, built from
	// path-style segments so they look like real task names.
	for n := 1; n <= 260; n++ {
		base := "team-alpha/service-" + strings.Repeat("comp/", 40) + "task"
		if n <= len(base) {
			ids = append(ids, TaskID(base[:n]))
			continue
		}
		ids = append(ids, TaskID(base+strings.Repeat("x", n-len(base))))
	}
	// A few realistic medium-length hierarchical names.
	ids = append(ids,
		TaskID("org/team-a/service-b/module-c/pkg-d/"+strings.Repeat("sub/", 20)+"build"),
		TaskID(strings.Repeat("layer/", 24)+":compile"),
	)
	return ids
}

// TestMediumLengthIDsRoundTrip covers write + hit for hierarchical IDs
// of every length in the previously failing window and beyond.
func TestMediumLengthIDsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store := mustFileStore(t, dir)

	ids := mediumIDs()
	for _, id := range ids {
		e := Entry{
			Version:     Version,
			Task:        id,
			Fingerprint: validFP,
			Output:      Artifact{Data: []byte(fmt.Sprintf("payload-%d", len(id)))},
		}
		if err := store.Put(ctx, e); err != nil {
			t.Fatalf("Put(id len=%d): %v", len(id), err)
		}
	}
	assertContained(t, dir)

	for _, id := range ids {
		got, err := store.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get(id len=%d): %v", len(id), err)
		}
		want := fmt.Sprintf("payload-%d", len(id))
		if got.Task != id || string(got.Output.Data) != want {
			t.Fatalf("round trip mismatch for id len=%d", len(id))
		}
	}
}

// TestMediumLengthIDsAcrossRestart verifies that medium-length
// hierarchical IDs survive a process restart and can be deleted.
func TestMediumLengthIDsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	ids := mediumIDs()

	s1 := mustFileStore(t, dir)
	for _, id := range ids {
		if err := s1.Put(ctx, Entry{
			Version:     Version,
			Task:        id,
			Fingerprint: validFP,
			Output:      Artifact{Data: []byte(fmt.Sprintf("v1-%d", len(id)))},
		}); err != nil {
			t.Fatalf("Put(id len=%d): %v", len(id), err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := mustFileStore(t, dir)
	for _, id := range ids {
		got, err := s2.Get(ctx, id)
		if err != nil {
			t.Fatalf("after restart Get(id len=%d): %v", len(id), err)
		}
		if want := fmt.Sprintf("v1-%d", len(id)); string(got.Output.Data) != want {
			t.Fatalf("after restart id len=%d payload=%q want %q",
				len(id), got.Output.Data, want)
		}
	}

	for _, id := range ids {
		if err := s2.Delete(ctx, id); err != nil {
			t.Fatalf("Delete(id len=%d): %v", len(id), err)
		}
		if _, err := s2.Get(ctx, id); !errors.Is(err, ErrCacheMiss) {
			t.Fatalf("deleted id len=%d: expected ErrCacheMiss, got %v", len(id), err)
		}
	}
	// Deleting must not leave residue: only the lock file remains.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, en := range entries {
		if en.Name() != lockFileName {
			t.Errorf("residue left after deleting all entries: %q", en.Name())
		}
	}
}

// TestMediumLengthIDsNoCrossTalk ensures that hierarchical IDs which
// share prefixes or differ only in a late segment never read or
// overwrite each other's entries, across the direct/hashed boundary.
func TestMediumLengthIDsNoCrossTalk(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store := mustFileStore(t, dir)

	prefix := "org/team/svc/" + strings.Repeat("mod/", 28) // 132 bytes
	ids := []TaskID{
		TaskID(prefix),                 // direct-encoded boundary region
		TaskID(prefix + "a"),           // one segment char longer
		TaskID(prefix + "b"),           // same length, different content
		TaskID(prefix + "a/x"),         // extends a direct-encoded ID
		TaskID(prefix[:len(prefix)-1]), // one char shorter
	}
	for i, id := range ids {
		if err := store.Put(ctx, Entry{
			Version:     Version,
			Task:        id,
			Fingerprint: validFP,
			Output:      Artifact{Data: []byte(fmt.Sprintf("entry-%d", i))},
		}); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	for i, id := range ids {
		got, err := store.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if want := fmt.Sprintf("entry-%d", i); string(got.Output.Data) != want {
			t.Fatalf("id %d read entry of another id: got %q want %q",
				i, got.Output.Data, want)
		}
	}
	// Deleting one must not disturb the others.
	if err := store.Delete(ctx, ids[1]); err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		_, err := store.Get(ctx, id)
		if i == 1 {
			if !errors.Is(err, ErrCacheMiss) {
				t.Fatalf("deleted id still readable: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("id %d disturbed by deleting a sibling: %v", i, err)
		}
	}
}

// TestEntryKeyFitsNameMax pins the invariant behind the fix: no legal
// task ID, of any length, may produce a file name that exceeds the
// 255-byte per-component limit or contains a separator.
func TestEntryKeyFitsNameMax(t *testing.T) {
	for n := 0; n <= 600; n++ {
		id := TaskID(strings.Repeat("a/b", n))
		k := entryKey(id)
		if len(k) > 255 {
			t.Fatalf("entryKey for id len=%d is %d bytes, exceeds NAME_MAX", len(id), len(k))
		}
		if strings.ContainsAny(k, "/\\") {
			t.Fatalf("entryKey for id len=%d contains a separator: %q", len(id), k)
		}
	}
	// The boundary itself: the longest directly encoded name must fit.
	if got := 2*directKeyLimit + len(entrySuffix); got > 255 {
		t.Fatalf("directKeyLimit=%d yields %d-byte names, exceeds NAME_MAX",
			directKeyLimit, got)
	}
}

// TestMediumLengthIDsConcurrent hammers one cache directory from many
// goroutines with medium-length hierarchical IDs: parallel writes,
// hits and deletes must stay consistent under -race.
func TestMediumLengthIDsConcurrent(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store := mustFileStore(t, dir)

	prefix := "org/team/svc/" + strings.Repeat("mod/", 28)
	const workers = 8
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := TaskID(fmt.Sprintf("%sworker-%d", prefix, w))
			payload := fmt.Sprintf("data-%d", w)
			for i := 0; i < 25; i++ {
				if err := store.Put(ctx, Entry{
					Version:     Version,
					Task:        id,
					Fingerprint: validFP,
					Output:      Artifact{Data: []byte(payload)},
				}); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				got, err := store.Get(ctx, id)
				if err != nil {
					t.Errorf("Get after Put: %v", err)
					return
				}
				if string(got.Output.Data) != payload {
					t.Errorf("read torn/foreign entry: got %q want %q",
						got.Output.Data, payload)
					return
				}
				if i%3 == 0 {
					if err := store.Delete(ctx, id); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	// Every worker's final state must be coherent: either a complete
	// entry with that worker's own payload, or a clean miss.
	for w := 0; w < workers; w++ {
		id := TaskID(fmt.Sprintf("%sworker-%d", prefix, w))
		got, err := store.Get(ctx, id)
		if errors.Is(err, ErrCacheMiss) {
			continue
		}
		if err != nil {
			t.Fatalf("final Get(%q): %v", id, err)
		}
		if want := fmt.Sprintf("data-%d", w); string(got.Output.Data) != want {
			t.Fatalf("final entry for %q holds foreign payload %q", id, got.Output.Data)
		}
	}
}
