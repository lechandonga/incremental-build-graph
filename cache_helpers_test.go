package buildgraph

import "crypto/sha256"

// checksumOf mirrors the checksum rule used by encodeEntry (test-only helper).
func checksumOf(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}
