package graph

import (
	"crypto/sha256"
	"encoding/hex"
)

// fingerprint computes a stable fingerprint for a task from its own
// inputs/config and the ordered fingerprints of its direct
// dependencies. Because dependency fingerprints recursively embed their
// own upstream inputs, a matching fingerprint guarantees the whole
// transitive input set is unchanged.
//
// Domain-separated length-prefixed framing is used so that structurally
// different inputs can never collide.
func fingerprint(t *Task, depFingerprints []string) string {
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
	// Dependencies contribute in declared order; the count is included
	// so different dependency shapes cannot collide either.
	var depBuf []byte
	depBuf = appendUvarint(depBuf, uint64(len(depFingerprints)))
	for _, df := range depFingerprints {
		depBuf = append(depBuf, df...)
		depBuf = append(depBuf, 0)
	}
	writeField('d', depBuf)
	return hex.EncodeToString(h.Sum(nil))
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
