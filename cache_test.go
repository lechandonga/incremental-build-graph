package buildgraph

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestFileCacheRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c, err := NewFileCache(dir, "ns")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	fp := []byte("fingerprint-value-0123456789")
	if _, ok := c.Get(ctx, "task", fp); ok {
		t.Fatal("fresh cache should miss")
	}
	if err := c.Put(ctx, "task", fp, Artifact("payload")); err != nil {
		t.Fatal(err)
	}
	a, ok := c.Get(ctx, "task", fp)
	if !ok || string(a) != "payload" {
		t.Fatalf("round trip failed: %q %v", a, ok)
	}
	// Different fingerprint must miss even for the same task.
	if _, ok := c.Get(ctx, "task", []byte("other-fp")); ok {
		t.Fatal("fingerprint mismatch must miss")
	}
}

// corruptAllEntries flips/truncates every *.bin file in dir.
func corruptAllEntries(t *testing.T, dir string, mutate func([]byte) []byte) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".bin") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, mutate(raw), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCorruptedCacheIsMiss(t *testing.T) {
	dir := t.TempDir()
	var runs int64
	taskFn := func(_ context.Context, in, _ []byte, _ map[string]Artifact) (Artifact, error) {
		atomic.AddInt64(&runs, 1)
		return Artifact("out-" + string(in)), nil
	}

	run := func() {
		g := NewGraph()
		_ = g.Add(Task{ID: "a", Input: []byte("v1"), Run: taskFn})
		s, _ := New(Options{CacheDir: dir})
		res, err := s.Run(context.Background(), g)
		if err != nil {
			t.Fatal(err)
		}
		if string(res.Artifacts["a"]) != "out-v1" {
			t.Fatalf("corrupted cache produced wrong artifact %q", res.Artifacts["a"])
		}
	}

	run()
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("expected 1 run, got %d", got)
	}

	cases := map[string]func([]byte) []byte{
		"truncated":      func(b []byte) []byte { return b[:len(b)/2] },
		"empty":          func(b []byte) []byte { return []byte{} },
		"bit-flip":       func(b []byte) []byte { b[len(b)-1] ^= 0xff; return b },
		"payload-tamper": func(b []byte) []byte { b[10] ^= 0xff; return b },
		"garbage":        func(b []byte) []byte { return []byte("not a cache entry at all") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			before := atomic.LoadInt64(&runs)
			corruptAllEntries(t, dir, mutate)
			run() // corrupted entry: miss -> recompute -> rewrite valid entry
			if atomic.LoadInt64(&runs) != before+1 {
				t.Fatalf("corruption %q must cause exactly one recomputation", name)
			}
			// Immediately after rewrite, a further run must hit again.
			run()
			if atomic.LoadInt64(&runs) != before+1 {
				t.Fatalf("rebuilt entry must be a hit for %q", name)
			}
		})
	}
}

func TestOldVersionEntryIsMiss(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewFileCache(dir, "ns")
	ctx := context.Background()
	fp := []byte("fp")
	if err := c.Put(ctx, "a", fp, Artifact("data")); err != nil {
		t.Fatal(err)
	}
	// Forge an entry with an older format version byte (offset 4).
	corruptAllEntries(t, dir, func(b []byte) []byte {
		b[4] = cacheFormatVersion - 1
		// Recompute the trailing checksum so the only mismatch is version.
		sum := checksumOf(b[:len(b)-32])
		copy(b[len(b)-32:], sum)
		return b
	})
	if _, ok := c.Get(ctx, "a", fp); ok {
		t.Fatal("old version entry must be treated as a miss")
	}
}

func TestCacheNamespaceIsolation(t *testing.T) {
	dir := t.TempDir()
	c1, _ := NewFileCache(dir, "graph-1")
	c2, _ := NewFileCache(dir, "graph-2")
	ctx := context.Background()
	fp := []byte("fp")
	if err := c1.Put(ctx, "same-task-id", fp, Artifact("one")); err != nil {
		t.Fatal(err)
	}
	if _, ok := c2.Get(ctx, "same-task-id", fp); ok {
		t.Fatal("namespaces must be isolated")
	}
	a, ok := c1.Get(ctx, "same-task-id", fp)
	if !ok || string(a) != "one" {
		t.Fatal("own-namespace lookup failed")
	}
}

func TestMemoryCacheConcurrentAccess(t *testing.T) {
	c := NewMemoryCache()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "t"
			fp := []byte{byte(i)}
			_ = c.Put(ctx, id, fp, Artifact{byte(i)})
			if a, ok := c.Get(ctx, id, fp); ok && a[0] != byte(i) {
				t.Errorf("got %d want %d", a[0], i)
			}
		}(i)
	}
	wg.Wait()
}
