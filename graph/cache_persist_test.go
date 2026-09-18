package graph

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func buildTwoNodeGraph(store Store) (*Engine, *int32, *int32) {
	e := NewEngine(WithStore(store))
	var aRuns, bRuns int32
	registerAdder(e, "a", nil, 1, &aRuns)
	registerAdder(e, "b", []TaskID{"a"}, 10, &bRuns)
	return e, &aRuns, &bRuns
}

// TestFileStoreRestartHit verifies cache hits survive a process
// restart: a brand new engine over the same directory reuses results.
func TestFileStoreRestartHit(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	e, aRuns, bRuns := buildTwoNodeGraph(mustFileStore(t, dir))
	res, err := e.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if res["b"].Cached {
		t.Fatal("first run must miss")
	}

	// Simulate restart: new engine, same directory.
	e2, aRuns2, bRuns2 := buildTwoNodeGraph(mustFileStore(t, dir))
	res2, err := e2.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if !res2["a"].Cached || !res2["b"].Cached {
		t.Fatalf("after restart both tasks must hit cache: %+v", res2)
	}
	if got := atomic.LoadInt32(aRuns2); got != 0 {
		t.Fatalf("a executed after restart: %d", got)
	}
	if got := atomic.LoadInt32(bRuns2); got != 0 {
		t.Fatalf("b executed after restart: %d", got)
	}
	if atomic.LoadInt32(aRuns)+atomic.LoadInt32(bRuns) != 2 {
		t.Fatal("first engine counters unchanged sanity check")
	}
}

// TestCorruptEntriesAreMisses writes garbage / truncated / tampered
// files and verifies they are treated as misses and self-heal.
func TestCorruptEntriesAreMisses(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	e, _, _ := buildTwoNodeGraph(mustFileStore(t, dir))
	if _, err := e.Run(ctx, "b"); err != nil {
		t.Fatal(err)
	}

	cases := map[string][]byte{
		"garbage": []byte("not a cache file at all!!"),
		"truncated": func() []byte {
			b, _ := os.ReadFile(filepath.Join(dir, "a.cache"))
			return b[:len(b)/2]
		}(),
		"tampered": func() []byte {
			b, _ := os.ReadFile(filepath.Join(dir, "a.cache"))
			b = append([]byte(nil), b...)
			b[len(b)-2] ^= 0xFF
			return b
		}(),
	}
	for name, corrupt := range cases {
		if err := os.WriteFile(filepath.Join(dir, "a.cache"), corrupt, 0o644); err != nil {
			t.Fatal(err)
		}
		store, err := NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Get(ctx, "a"); err == nil {
			t.Fatalf("case %s: expected error on corrupt entry", name)
		}
		// Engine treats it as a miss, recomputes, and self-heals.
		e2, aRuns2, bRuns2 := buildTwoNodeGraph(store)
		res, err := e2.Run(ctx, "b")
		if err != nil {
			t.Fatalf("case %s: run after corruption: %v", name, err)
		}
		if res["a"].Cached {
			t.Fatalf("case %s: corrupt a must be a miss", name)
		}
		if !res["b"].Cached {
			t.Fatalf("case %s: b fingerprint unchanged and must hit", name)
		}
		if got := atomic.LoadInt32(aRuns2); got != 1 {
			t.Fatalf("case %s: a should run exactly once, got %d", name, got)
		}
		if got := atomic.LoadInt32(bRuns2); got != 0 {
			t.Fatalf("case %s: b should not run, got %d", name, got)
		}
		// File now decodes cleanly.
		if _, err := store.Get(ctx, "a"); err != nil {
			t.Fatalf("case %s: entry did not self-heal: %v", name, err)
		}
	}
}

// TestOldVersionEntry verifies entries carrying an unsupported logical
// version are ignored as misses and replaced.
func TestOldVersionEntry(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store := mustFileStore(t, dir)
	e, _, _ := buildTwoNodeGraph(store)
	if _, err := e.Run(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	// Overwrite with an entry whose logical Version is older but whose
	// on-disk format is current.
	old := Entry{
		Version:     Version - 1,
		Task:        "a",
		Fingerprint: "doesnt-matter",
		Output:      Artifact{Data: []byte("ancient")},
	}
	if err := store.Put(ctx, old); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, "a")
	if err != nil {
		t.Fatalf("decoding old-format file should succeed: %v", err)
	}
	if got.Version != Version-1 {
		t.Fatalf("version round-trip wrong: %d", got.Version)
	}
	e2, aRuns, _ := buildTwoNodeGraph(mustFileStore(t, dir))
	res, err := e2.Run(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if res["a"].Cached {
		t.Fatal("old-version entry must not be used")
	}
	if v := atomic.LoadInt32(aRuns); v != 1 {
		t.Fatalf("a should recompute once, got %d", v)
	}
}

// TestOldOnDiskFormat verifies a previous on-disk format version byte
// is rejected as incompatible and treated as a miss.
func TestOldOnDiskFormat(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	e, _, _ := buildTwoNodeGraph(mustFileStore(t, dir))
	if _, err := e.Run(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "a.cache")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// Flip format version byte (magic = 4 bytes, then version).
	b[4] = 99
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	store := mustFileStore(t, dir)
	if _, err := store.Get(ctx, "a"); err == nil {
		t.Fatal("expected ErrCacheVersion for unknown format byte")
	}
	// Still recoverable.
	e2, aRuns, _ := buildTwoNodeGraph(store)
	res, err := e2.Run(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if res["a"].Cached {
		t.Fatal("incompatible format must be treated as miss")
	}
	if v := atomic.LoadInt32(aRuns); v != 1 {
		t.Fatalf("a should run once, got %d", v)
	}
}

// TestEncodeDecodeRoundTrip exercises the codec directly.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	e := Entry{
		Version:     Version,
		Task:        "task-x",
		Fingerprint: "fp123",
		Output:      Artifact{Data: []byte{0, 1, 2, 3, 255}},
	}
	got, err := decodeEntry(encodeEntry(e))
	if err != nil {
		t.Fatal(err)
	}
	if got.Task != e.Task || got.Fingerprint != e.Fingerprint ||
		got.Version != e.Version || string(got.Output.Data) != string(e.Output.Data) {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func mustFileStore(t *testing.T, dir string) *FileStore {
	t.Helper()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestRestartInvalidation verifies that changing an input and then
// restarting (new engine, same cache dir) invalidates the changed task
// and its downstream, while an unrelated branch keeps hitting.
func TestRestartInvalidation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	e := NewEngine(WithStore(mustFileStore(t, dir)))
	var xRuns, aRuns, bRuns int32
	registerAdder(e, "x", nil, 100, &xRuns) // unrelated
	registerAdder(e, "a", nil, 1, &aRuns)
	registerAdder(e, "b", []TaskID{"a"}, 10, &bRuns)
	if _, err := e.Run(ctx, "x", "b"); err != nil {
		t.Fatal(err)
	}

	// Restart with changed input for a.
	e2 := NewEngine(WithStore(mustFileStore(t, dir)))
	var xRuns2, aRuns2, bRuns2 int32
	registerAdder(e2, "x", nil, 100, &xRuns2)
	registerAdder(e2, "a", nil, 2, &aRuns2) // input changed
	registerAdder(e2, "b", []TaskID{"a"}, 10, &bRuns2)
	res, err := e2.Run(ctx, "x", "b")
	if err != nil {
		t.Fatal(err)
	}
	if !res["x"].Cached {
		t.Error("unrelated x must remain cached across restart+change")
	}
	if res["a"].Cached || res["b"].Cached {
		t.Error("a and b must recompute after input change")
	}
	if got := decodeInt(res["b"].Output.Data); got != 12 {
		t.Errorf("b = %d, want 12", got)
	}
	if atomic.LoadInt32(&xRuns2) != 0 {
		t.Error("x executed after restart")
	}
}
