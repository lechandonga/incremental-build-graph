package graph

import (
	"crypto/sha256"
	"encoding/hex"
)

// contentFingerprint computes the stable content fingerprint of a task
// from:
//
//   - the task ID;
//   - the task's own Inputs (its declared inputs and configuration);
//   - the ordered set of upstream artifacts it ACTUALLY consumed, where
//     each contributor is the upstream ID plus a hash of the exact
//     artifact bytes the task read.
//
// Because contributors are keyed by artifact BYTES rather than by
// upstream fingerprints, re-running an upstream whose inputs/config
// changed but whose produced bytes are byte-for-byte identical does not
// change this fingerprint: the downstream keeps hitting the cache. On
// the other hand, any change in the bytes of a consumed upstream always
// changes the fingerprint and forces every consuming downstream to be
// recomputed; a stale downstream artifact can never be reused.
//
// Domain-separated length-prefixed framing is used so that structurally
// different inputs can never collide.
func contentFingerprint(t *Task, consumed []ConsumedDep) string {
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
	// Consumed artifacts contribute in first-read order; the count is
	// included and each entry carries the upstream ID so that swapping
	// two upstreams cannot collide.
	var cBuf []byte
	cBuf = appendUvarint(cBuf, uint64(len(consumed)))
	for _, c := range consumed {
		cBuf = appendBytes(cBuf, []byte(c.Task))
		cBuf = appendBytes(cBuf, []byte(c.DataHash))
	}
	writeField('c', cBuf)
	return hex.EncodeToString(h.Sum(nil))
}

// artifactHash returns the stable hash of an artifact's consumed bytes.
func artifactHash(b []byte) string {
	return hashString("artifact:", b)
}

func appendUvarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func hashString(prefix string, b []byte) string {
	h := sha256.New()
	h.Write([]byte(prefix))
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}
