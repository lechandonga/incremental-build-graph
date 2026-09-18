package buildgraph

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// fingerprintVersion is mixed into every fingerprint so that changing the
// fingerprinting scheme itself invalidates previously cached entries.
const fingerprintVersion byte = 1

// writeBytes appends a length-prefixed byte slice to b. Length framing makes
// the encoding unambiguous regardless of field contents.
func writeBytes(b []byte, data []byte) []byte {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(data)))
	b = append(b, lenBuf[:]...)
	return append(b, data...)
}

// taskFingerprint computes the stable fingerprint of a task from its input,
// config and the content hashes of its dependency artifacts.
//
// The fingerprint changes if and only if one of those inputs changes:
//
//	H(version || len(input)  || input
//	                || len(config) || config
//	                || for each dep id in sorted order:
//	                     len(id) || id || depHash)
//
// Dependency ids are sorted so the fingerprint is independent of Deps
// declaration order.
func taskFingerprint(t *Task, depHashes map[string][]byte) []byte {
	b := make([]byte, 0, 64)
	b = append(b, fingerprintVersion)
	b = writeBytes(b, t.Input)
	b = writeBytes(b, t.Config)

	ids := make([]string, 0, len(t.Deps))
	for _, dep := range t.Deps {
		ids = append(ids, dep)
	}
	sort.Strings(ids)
	for _, dep := range ids {
		b = writeBytes(b, []byte(dep))
		b = append(b, depHashes[dep]...)
	}

	sum := sha256.Sum256(b)
	return sum[:]
}

// hashArtifact returns the stable content hash (raw SHA-256) of an artifact.
func hashArtifact(a Artifact) []byte {
	sum := sha256.Sum256(a)
	return sum[:]
}
