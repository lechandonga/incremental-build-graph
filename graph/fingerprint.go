package graph

import (
	"crypto/sha256"
	"encoding/hex"
)

// depContent pairs a consumed dependency with the SHA-256 hash (hex) of
// the exact artifact bytes the task read from it.
type depContent struct {
	id  TaskID
	sum string
}

// artifactContentSum returns the hex SHA-256 of an artifact payload.
// Downstream fingerprints are derived from these hashes, so a downstream
// task is invalidated exactly when the bytes of an upstream artifact it
// actually consumed change.
func artifactContentSum(a Artifact) string {
	sum := sha256.Sum256(a.Data)
	return hex.EncodeToString(sum[:])
}

// fingerprint computes a stable fingerprint for a task from:
//
//   - the task ID;
//   - the task's own Inputs (inputs and configuration);
//   - the content hashes of the upstream artifacts the task actually
//     read, in declared dependency order (duplicates collapsed).
//
// Upstream fingerprints are deliberately NOT embedded: an upstream task
// may be recomputed (its own inputs/config changed) while emitting
// byte-identical output. In that case downstream fingerprints stay
// stable and downstream results are reused. Conversely, any byte change
// in a consumed upstream artifact changes this fingerprint and forces a
// recompute.
//
// Domain-separated length-prefixed framing prevents structural
// collisions.
func fingerprint(t *Task, deps []depContent) string {
	h := sha256.New()
	writeField := func(prefix byte, b []byte) {
		h.Write([]byte{prefix})
		var l [8]byte
		l[0] = byte(len(b) >> 56)
		l[1] = byte(len(b) >> 48)
		l[2] = byte(len(b) >> 40)
		l[3] = byte(len(b) >> 32)
		l[4] = byte(len(b) >> 24)
		l[5] = byte(len(b) >> 16)
		l[6] = byte(len(b) >> 8)
		l[7] = byte(len(b))
		h.Write(l[:])
		h.Write(b)
	}
	writeField('t', []byte(t.ID))
	writeField('i', t.Inputs)
	// Dependencies contribute (id, content-hash) pairs in declared
	// order; the count is included so different dependency shapes cannot
	// collide.
	var depBuf []byte
	depBuf = appendUvarint(depBuf, uint64(len(deps)))
	for _, d := range deps {
		depBuf = appendLenPrefixed(depBuf, []byte(d.id))
		depBuf = appendLenPrefixed(depBuf, []byte(d.sum))
	}
	writeField('d', depBuf)
	return hex.EncodeToString(h.Sum(nil))
}

// appendUvarint appends a LEB128 unsigned integer.
func appendUvarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

// appendLenPrefixed appends an 8-byte big-endian length followed by raw.
func appendLenPrefixed(dst, raw []byte) []byte {
	var l [8]byte
	v := uint64(len(raw))
	l[0] = byte(v >> 56)
	l[1] = byte(v >> 48)
	l[2] = byte(v >> 40)
	l[3] = byte(v >> 32)
	l[4] = byte(v >> 24)
	l[5] = byte(v >> 16)
	l[6] = byte(v >> 8)
	l[7] = byte(v)
	dst = append(dst, l[:]...)
	return append(dst, raw...)
}
