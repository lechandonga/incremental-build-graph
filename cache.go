package buildgraph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// cacheFormatVersion is the on-disk cache format version understood by this build.
const cacheFormatVersion byte = 1

var cacheMagic = []byte("IBGC")

// Cache stores task artifacts keyed by (task id, fingerprint).
type Cache interface {
	// Get returns the artifact stored for taskID/fingerprint. A miss, a
	// version mismatch or a corrupted/truncated entry must report ok == false.
	Get(ctx context.Context, taskID string, fingerprint []byte) (Artifact, bool)
	// Put stores an artifact durably and atomically.
	Put(ctx context.Context, taskID string, fingerprint []byte, artifact Artifact) error
}

// FileCache is a filesystem backed Cache. One entry file is kept per task
// within a namespace; updating a task replaces its previous entry atomically.
type FileCache struct {
	dir       string
	namespace string
}

// NewFileCache opens (creating if needed) a file cache rooted at dir.
func NewFileCache(dir, namespace string) (*FileCache, error) {
	if dir == "" {
		return nil, errors.New("buildgraph: cache dir must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	c := &FileCache{dir: dir, namespace: namespace}
	// Remove temp files left behind by processes killed mid-Put. They never
	// replaced their target entry, so deleting them cannot remove valid data.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if name := e.Name(); len(name) > len(".entry-") && name[:len(".entry-")] == ".entry-" {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
	return c, nil
}

// entryPath returns the on-disk path of a task entry. The task id is hashed
// together with the namespace so arbitrary ids become filesystem safe and
// different namespaces never collide.
func (c *FileCache) entryPath(taskID string) string {
	h := sha256.New()
	h.Write([]byte(c.namespace))
	h.Write([]byte{0})
	h.Write([]byte(taskID))
	name := hex.EncodeToString(h.Sum(nil)) + ".bin"
	return filepath.Join(c.dir, name)
}

// encodeEntry serializes a cache entry:
//
//	magic(4) || version(1) || fpLen(2) || fingerprint ||
//	artifactLen(8) || artifact || sha256(all preceding bytes)(32)
func encodeEntry(fingerprint, artifact []byte) []byte {
	if len(fingerprint) > 0xffff {
		// Fingerprints are hashes; this can never trigger in practice.
		panic("buildgraph: fingerprint too large")
	}
	var lenBuf [8]byte
	buf := bytes.NewBuffer(nil)
	buf.Write(cacheMagic)
	buf.WriteByte(cacheFormatVersion)
	binary.BigEndian.PutUint16(lenBuf[:2], uint16(len(fingerprint)))
	buf.Write(lenBuf[:2])
	buf.Write(fingerprint)
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(artifact)))
	buf.Write(lenBuf[:])
	buf.Write(artifact)

	sum := sha256.Sum256(buf.Bytes())
	buf.Write(sum[:])
	return buf.Bytes()
}

// decodeEntry parses and validates a cache entry, returning the artifact only
// when magic, version, framing, checksum and fingerprint all match.
func decodeEntry(raw, fingerprint []byte) (Artifact, bool) {
	r := bytes.NewReader(raw)

	magic := make([]byte, len(cacheMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, false
	}
	if !bytes.Equal(magic, cacheMagic) {
		// Unknown/legacy magic: treat as miss so the entry gets rebuilt.
		return nil, false
	}

	version, err := r.ReadByte()
	if err != nil {
		return nil, false
	}
	if version != cacheFormatVersion {
		// Old or future version: incompatible, safe miss.
		return nil, false
	}

	var head [2]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, false
	}
	fpLen := binary.BigEndian.Uint16(head[:])
	storedFP := make([]byte, fpLen)
	if _, err := io.ReadFull(r, storedFP); err != nil {
		return nil, false
	}
	if !bytes.Equal(storedFP, fingerprint) {
		return nil, false
	}

	var lenBuf [8]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, false
	}
	artifactLen := binary.BigEndian.Uint64(lenBuf[:])
	// Guard against absurd/hostile length prefixes before allocating.
	if artifactLen > uint64(r.Len())-sha256.Size {
		return nil, false
	}
	artifact := make([]byte, artifactLen)
	if _, err := io.ReadFull(r, artifact); err != nil {
		return nil, false
	}

	checksum := make([]byte, sha256.Size)
	if _, err := io.ReadFull(r, checksum); err != nil {
		return nil, false
	}
	if r.Len() != 0 {
		return nil, false // trailing bytes: entry is not in a known shape
	}

	// Checksum is taken over everything preceding it, which covers magic,
	// version, fingerprint and the artifact.
	covered := len(raw) - sha256.Size
	sum := sha256.Sum256(raw[:covered])
	if !bytes.Equal(sum[:], checksum) {
		return nil, false
	}
	return artifact, true
}

func (c *FileCache) Get(ctx context.Context, taskID string, fingerprint []byte) (Artifact, bool) {
	if err := ctx.Err(); err != nil {
		return nil, false
	}
	raw, err := os.ReadFile(c.entryPath(taskID))
	if err != nil {
		return nil, false // missing entry or unreadable cache: miss
	}
	a, ok := decodeEntry(raw, fingerprint)
	if !ok {
		return nil, false // corrupted/truncated/version-mismatched entry: miss
	}
	return a, true
}

// Put writes the entry to a temp file in the same directory, fsyncs it and
// renames it into place. A crash leaves either the previous entry or the new
// one, never a partially written target file.
func (c *FileCache) Put(ctx context.Context, taskID string, fingerprint []byte, artifact Artifact) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := c.entryPath(taskID)
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".entry-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(encodeEntry(fingerprint, artifact)); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return err
	}
	return nil
}
