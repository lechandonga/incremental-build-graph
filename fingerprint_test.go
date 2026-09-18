package buildgraph

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestFingerprintDeterministicAndOrderInsensitive(t *testing.T) {
	t1 := &Task{ID: "x", Input: []byte("i"), Config: []byte("c"), Deps: []string{"p", "q"}}
	t2 := &Task{ID: "x", Input: []byte("i"), Config: []byte("c"), Deps: []string{"q", "p"}}
	dh := map[string][]byte{"p": []byte("HASH-P"), "q": []byte("HASH-Q")}
	f1 := taskFingerprint(t1, dh)
	f2 := taskFingerprint(t2, dh)
	if string(f1) != string(f2) {
		t.Fatal("fingerprint must be independent of dependency declaration order")
	}
	// Any input change must change the fingerprint.
	t3 := &Task{ID: "x", Input: []byte("i2"), Config: []byte("c"), Deps: []string{"p", "q"}}
	if string(taskFingerprint(t3, dh)) == string(f1) {
		t.Fatal("changed input must change fingerprint")
	}
	t4 := &Task{ID: "x", Input: []byte("i"), Config: []byte("c2"), Deps: []string{"p", "q"}}
	if string(taskFingerprint(t4, dh)) == string(f1) {
		t.Fatal("changed config must change fingerprint")
	}
	dh2 := map[string][]byte{"p": []byte("HASH-P2"), "q": []byte("HASH-Q")}
	if string(taskFingerprint(t1, dh2)) == string(f1) {
		t.Fatal("changed dependency artifact must change fingerprint")
	}
}

func TestConfigChangeInvalidates(t *testing.T) {
	dir := t.TempDir()
	var runs int64
	build := func(cfg string) *Graph {
		g := NewGraph()
		_ = g.Add(Task{ID: "t", Input: []byte("in"), Config: []byte(cfg),
			Run: func(_ context.Context, in, c []byte, _ map[string]Artifact) (Artifact, error) {
				atomic.AddInt64(&runs, 1)
				return Artifact(string(in) + "/" + string(c)), nil
			}})
		return g
	}
	s, _ := New(Options{CacheDir: dir})
	r1, err := s.Run(context.Background(), build("cfg1"))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.Run(context.Background(), build("cfg1"))
	if err != nil {
		t.Fatal(err)
	}
	r3, err := s.Run(context.Background(), build("cfg2"))
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.Cached) != 0 || len(r2.Cached) != 1 || len(r3.Executed) != 1 {
		t.Fatalf("config change invalidation wrong: %+v %+v %+v", r1, r2, r3)
	}
	if string(r3.Artifacts["t"]) != "in/cfg2" {
		t.Fatalf("wrong artifact after config change: %q", r3.Artifacts["t"])
	}
}

func TestDuplicateDependencyDeclaration(t *testing.T) {
	s, _ := New(Options{})
	g := NewGraph()
	var aRuns, bRuns int32
	_ = g.Add(Task{ID: "a",
		Run: func(_ context.Context, _, _ []byte, _ map[string]Artifact) (Artifact, error) {
			atomic.AddInt32(&aRuns, 1)
			return Artifact("A"), nil
		}})
	_ = g.Add(Task{ID: "b", Deps: []string{"a", "a"},
		Run: func(_ context.Context, _, _ []byte, d map[string]Artifact) (Artifact, error) {
			atomic.AddInt32(&bRuns, 1)
			if len(d) != 1 {
				t.Errorf("dependency artifacts must be keyed uniquely, got %d", len(d))
			}
			return Artifact("B" + string(d["a"])), nil
		}})
	res, err := s.Run(context.Background(), g)
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&aRuns) != 1 || atomic.LoadInt32(&bRuns) != 1 {
		t.Fatal("tasks must run exactly once even with duplicated deps")
	}
	if string(res.Artifacts["b"]) != "BA" {
		t.Fatalf("bad artifact %q", res.Artifacts["b"])
	}
}

// TestInterruptedWriteRecovery simulates a crash during Put by leaving a temp
// file in the cache dir. Startup must remove it, and the entry must be rebuilt
// without affecting correctness.
func TestInterruptedWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	// Leftover temp file from an interrupted write.
	if err := os.WriteFile(filepath.Join(dir, ".entry-deadbeef"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	var runs int64
	g := NewGraph()
	_ = g.Add(Task{ID: "t",
		Run: func(_ context.Context, _, _ []byte, _ map[string]Artifact) (Artifact, error) {
			atomic.AddInt64(&runs, 1)
			return Artifact("ok"), nil
		}})
	s, err := New(Options{CacheDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Run(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".bin" {
			t.Fatalf("stale temp file not cleaned: %s", e.Name())
		}
	}
	if _, err := s.Run(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt64(&runs) != 1 {
		t.Fatal("second run must be a cache hit after interrupted-write cleanup")
	}
}

func TestEmptyGraph(t *testing.T) {
	s, _ := New(Options{})
	res, err := s.Run(context.Background(), NewGraph())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Artifacts) != 0 || len(res.Executed) != 0 || len(res.Cached) != 0 {
		t.Fatalf("empty graph yields empty result, got %+v", res)
	}
}
