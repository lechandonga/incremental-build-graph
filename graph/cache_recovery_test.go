package graph

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// corruptKinds are byte sequences that must all decode as
// ErrCacheCorrupt (never ErrCacheVersion).
func corruptKinds(validFile []byte) map[string][]byte {
	return map[string][]byte{
		"garbage":   []byte("definitely not a cache file!!!!"),
		"truncated": validFile[:len(validFile)/2],
		"tampered":  func() []byte { b := append([]byte(nil), validFile...); b[len(b)-2] ^= 0xFF; return b }(),
		"bad magic": func() []byte { b := append([]byte(nil), validFile...); b[0] ^= 0xFF; return b }(),
		"trailing":  append(append([]byte(nil), validFile...), 0xAA),
		"consumed-bad-fp": func() []byte {
			// Re-encode a structurally wrong entry: fingerprint field
			// with non-hex content.
			e := Entry{Version: Version, Task: "a", Fingerprint: "z"}
			return encodeEntry(e)
		}(),
	}
}

func assertErrClass(t *testing.T, err error, sentinel error, label string) {
	t.Helper()
	if !errors.Is(err, sentinel) {
		t.Fatalf("%s: expected %v, got %v", label, sentinel, err)
	}
	if sentinel == ErrCacheVersion && errors.Is(err, ErrCacheCorrupt) {
		t.Fatalf("%s: ErrCacheVersion must not also match ErrCacheCorrupt", label)
	}
	if sentinel == ErrCacheCorrupt && errors.Is(err, ErrCacheVersion) {
		t.Fatalf("%s: ErrCacheCorrupt must not also match ErrCacheVersion", label)
	}
}

// TestVersionAndCorruptionDistinguished verifies the two failure
// classes are distinguishable and BOTH act as a miss that self-heals.
func TestVersionAndCorruptionDistinguished(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store0 := mustFileStore(t, dir)
	e0, _, _ := buildTwoNodeGraph(store0)
	if _, err := e0.Run(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	valid, err := os.ReadFile(store0.path("a"))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("corruption kinds", func(t *testing.T) {
		for name, bad := range corruptKinds(valid) {
			if err := os.WriteFile(store0.path("a"), bad, 0o644); err != nil {
				t.Fatal(err)
			}
			st := mustFileStore(t, dir)
			_, gerr := st.Get(ctx, "a")
			assertErrClass(t, gerr, ErrCacheCorrupt, name)
			// Engine treats it as a miss, recomputes a, and heals; b
			// (whose consumed bytes are unchanged) stays cached.
			en, aRuns, bRuns := buildTwoNodeGraph(st)
			res, rerr := en.Run(ctx, "b")
			if rerr != nil {
				t.Fatalf("%s: %v", name, rerr)
			}
			if res["a"].Cached {
				t.Fatalf("%s: a must recompute", name)
			}
			if !res["b"].Cached {
				t.Fatalf("%s: b must stay cached", name)
			}
			if got := atomic.LoadInt32(aRuns); got != 1 {
				t.Fatalf("%s: a runs=%d want 1", name, got)
			}
			if got := atomic.LoadInt32(bRuns); got != 0 {
				t.Fatalf("%s: b runs=%d want 0", name, got)
			}
			if _, err := st.Get(ctx, "a"); err != nil {
				t.Fatalf("%s: did not self-heal: %v", name, err)
			}
		}
	})

	t.Run("unsupported on-disk format", func(t *testing.T) {
		b := append([]byte(nil), valid...)
		b[4] = 99
		sum := sha256.Sum256(b[:len(b)-sha256.Size])
		copy(b[len(b)-sha256.Size:], sum[:])
		if err := os.WriteFile(store0.path("a"), b, 0o644); err != nil {
			t.Fatal(err)
		}
		st := mustFileStore(t, dir)
		_, gerr := st.Get(ctx, "a")
		assertErrClass(t, gerr, ErrCacheVersion, "unknown format byte")
	})

	t.Run("unsupported logical version", func(t *testing.T) {
		st := mustFileStore(t, dir)
		old := Entry{
			Version: Version + 50, Task: "a", Fingerprint: validFP,
			Output: Artifact{Data: []byte("future")},
		}
		if err := st.Put(ctx, old); err != nil {
			t.Fatal(err)
		}
		_, gerr := st.Get(ctx, "a")
		assertErrClass(t, gerr, ErrCacheVersion, "future logical version")
		// And it self-heals through the engine.
		en, aRuns, _ := buildTwoNodeGraph(st)
		res, rerr := en.Run(ctx, "a")
		if rerr != nil {
			t.Fatal(rerr)
		}
		if res["a"].Cached {
			t.Fatal("future-version entry must be a miss")
		}
		if atomic.LoadInt32(aRuns) != 1 {
			t.Fatalf("a runs=%d want 1", atomic.LoadInt32(aRuns))
		}
		if _, err := st.Get(ctx, "a"); err != nil {
			t.Fatalf("did not self-heal: %v", err)
		}
	})
}

// TestOrphanTempFilesCleanedOnReuse seeds residue from interrupted
// writes alongside a valid committed entry. Reusing the directory must
// remove the orphans, leave the valid entry intact, and normal writes
// must keep working.
func TestOrphanTempFilesCleanedOnReuse(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustFileStore(t, dir)
	if err := s.Put(ctx, Entry{
		Version: Version, Task: "keep", Fingerprint: validFP,
		Output: Artifact{Data: []byte("good")},
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate crashed writers.
	for _, name := range []string{".tmp-abcd", ".tmp-xyz123"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("partial"), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	// A file that looks temp-like but is a directory must be preserved
	// (cleanup only removes regular files).
	if err := os.Mkdir(filepath.Join(dir, ".tmp-dir"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Reuse the directory.
	s2 := mustFileStore(t, dir)
	if _, err := s2.Get(ctx, "keep"); err != nil {
		t.Fatalf("valid entry must survive cleanup: %v", err)
	}
	names, _ := os.ReadDir(dir)
	for _, n := range names {
		if strings.HasPrefix(n.Name(), ".tmp-") && n.Type().IsRegular() {
			t.Errorf("orphan %q was not cleaned", n.Name())
		}
	}
	if _, err := os.Stat(filepath.Join(dir, ".tmp-dir")); err != nil {
		t.Errorf("non-regular temp-named entry must be left alone: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, lockFileName)); err != nil {
		t.Errorf("lock file missing: %v", err)
	}

	// Writes continue to work and do not accumulate residue.
	if err := s2.Put(ctx, Entry{
		Version: Version, Task: "more", Fingerprint: validFP,
		Output: Artifact{Data: []byte("m")},
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := s2.Get(ctx, "more"); err != nil || string(got.Output.Data) != "m" {
		t.Fatalf("post-cleanup write broken: %+v %v", got, err)
	}
	for _, dn := range mustReadDir(t, dir) {
		if strings.HasPrefix(dn.Name(), ".tmp-") && dn.Type().IsRegular() {
			t.Errorf("temp residue accumulated after Put: %q", dn.Name())
		}
	}
}

func mustReadDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	ns, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

func listNames(t *testing.T, dir string) []string {
	t.Helper()
	ns, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, n := range ns {
		out = append(out, n.Name())
	}
	return out
}

// TestLockSerializesCleanup verifies the directory lock blocks a second
// holder: an artificially held lock prevents another store from
// entering its locked section until released.
func TestLockSerializesCleanup(t *testing.T) {
	dir := t.TempDir()
	s1 := mustFileStore(t, dir)
	if err := lockDir(s1.lockFile); err != nil {
		t.Fatal(err)
	}
	acquired := make(chan struct{})
	go func() {
		s2 := mustFileStore(t, dir) // NewFileStore cleans up under lock
		_ = s2.withLock(func() error { close(acquired); return nil })
	}()
	select {
	case <-acquired:
		t.Fatal("second holder entered the locked section while lock held")
	case <-time.After(50 * time.Millisecond):
	}
	if err := unlockDir(s1.lockFile); err != nil {
		t.Fatal(err)
	}
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("second holder never acquired the lock after release")
	}
}

// TestConcurrentFileStoresSameDirectory hammers several independent
// store instances (like several engines/processes sharing a dir) with
// concurrent Puts/Gets/Deletes plus orphans present at construction. No
// read may ever see a partial file, valid entries must remain readable,
// and residue must not accumulate.
func TestConcurrentFileStoresSameDirectory(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	// Pre-existing orphans from a prior interrupted run.
	for i := 0; i < 8; i++ {
		p := filepath.Join(dir, ".tmp-seed-"+string(rune('a'+i)))
		if err := os.WriteFile(p, []byte("x"), 0o640); err != nil {
			t.Fatal(err)
		}
	}

	const stores = 6
	var wg sync.WaitGroup
	errCh := make(chan error, stores*3)
	for g := 0; g < stores; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s, err := NewFileStore(dir)
			if err != nil {
				errCh <- err
				return
			}
			for i := 0; i < 40; i++ {
				id := TaskID("grp/" + string(rune('a'+g)) + "/" + itoa(i))
				e := Entry{
					Version: Version, Task: id, Fingerprint: validFP,
					Output: Artifact{Data: []byte(itoa(g) + ":" + itoa(i))},
				}
				if err := s.Put(ctx, e); err != nil {
					errCh <- err
					return
				}
				got, err := s.Get(ctx, id)
				if err != nil {
					errCh <- err
					return
				}
				if string(got.Output.Data) != itoa(g)+":"+itoa(i) {
					errCh <- errMismatch
					return
				}
				if i%5 == 0 {
					if err := s.Delete(ctx, id); err != nil {
						errCh <- err
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// No regular temp residue remains.
	for _, n := range listNames(t, dir) {
		if strings.HasPrefix(n, ".tmp-") {
			t.Fatalf("temp residue left after concurrent use: %q", n)
		}
	}
}

var errMismatch = sterr("concurrent read returned wrong bytes")

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}
