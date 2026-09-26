package graph

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
)

// fileMagic prefixes every on-disk cache entry.
var fileMagic = []byte("IBGC")

// onDiskVersion is the format version written to disk. Bumped whenever
// the wire format changes incompatibly. A file carrying any other
// format byte decodes as ErrCacheVersion.
const onDiskVersion = 2

// encodeEntry serializes an Entry into the versioned, length-prefixed
// on-disk format:
//
//	magic (4) | version (1) | payload | checksum (32)
//
// where payload is a fixed sequence of length-prefixed (uint32 BE)
// fields: logical version(uint32), task id, fingerprint, output data,
// consumed-upstreams blob. The consumed-upstreams blob records exactly
// which upstream artifact bytes the task actually read:
//
//	count (uvarint) | ( task id (len-prefixed) | data hash
//	(len-prefixed) ) * count
//
// The trailing checksum is SHA-256 over everything preceding it
// (magic + version + payload), so truncation, tampering or bit rot is
// always detected.
func encodeEntry(e Entry) []byte {
	var buf bytes.Buffer
	buf.Write(fileMagic)
	buf.WriteByte(byte(onDiskVersion))

	var v [4]byte
	binary.BigEndian.PutUint32(v[:], uint32(e.Version))
	buf.Write(appendBytes(nil, v[:]))
	buf.Write(appendBytes(nil, []byte(e.Task)))
	buf.Write(appendBytes(nil, []byte(e.Fingerprint)))
	buf.Write(appendBytes(nil, e.Output.Data))
	buf.Write(appendBytes(nil, encodeConsumed(e.Consumed)))

	sum := sha256.Sum256(buf.Bytes())
	buf.Write(sum[:])
	return buf.Bytes()
}

// encodeConsumed serializes the consumed-upstream list.
func encodeConsumed(cs []ConsumedDep) []byte {
	var b []byte
	b = appendUvarint(b, uint64(len(cs)))
	for _, c := range cs {
		b = appendBytes(b, []byte(c.Task))
		b = appendBytes(b, []byte(c.DataHash))
	}
	return b
}

// decodeConsumed parses the blob produced by encodeConsumed.
func decodeConsumed(raw []byte) ([]ConsumedDep, error) {
	br := bytes.NewReader(raw)
	count, n := binary.Uvarint(raw)
	if n <= 0 {
		return nil, fmt.Errorf("%w: bad consumed count", ErrCacheCorrupt)
	}
	if _, err := br.Seek(int64(n), io.SeekStart); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCacheCorrupt, err)
	}
	// Each record needs at least two length prefixes, so a count larger
	// than what the remaining bytes could hold can never decode.
	const minRecord = 4 + 4
	if count > uint64(br.Len())/minRecord {
		return nil, fmt.Errorf("%w: implausible consumed count", ErrCacheCorrupt)
	}
	out := make([]ConsumedDep, 0, count)
	for i := uint64(0); i < count; i++ {
		taskField, err := readBytes(br)
		if err != nil {
			return nil, err
		}
		hashField, err := readBytes(br)
		if err != nil {
			return nil, err
		}
		out = append(out, ConsumedDep{
			Task:     TaskID(taskField),
			DataHash: string(hashField),
		})
	}
	if br.Len() != 0 {
		return nil, fmt.Errorf("%w: trailing bytes in consumed blob", ErrCacheCorrupt)
	}
	return out, nil
}

// decodeEntry parses data produced by encodeEntry. It returns
// ErrCacheVersion for an unrecognized format version and
// ErrCacheCorrupt for any bad magic, truncation, length violation,
// checksum mismatch or internal inconsistency.
//
// A successfully decoded entry may still carry an unsupported logical
// version; callers validate it with validateEntryVersion.
func decodeEntry(data []byte) (Entry, error) {
	const minLen = 4 + 1 + sha256.Size
	if len(data) < minLen {
		return Entry{}, fmt.Errorf("%w: truncated entry", ErrCacheCorrupt)
	}
	if !bytes.Equal(data[:len(fileMagic)], fileMagic) {
		return Entry{}, fmt.Errorf("%w: bad magic", ErrCacheCorrupt)
	}
	storedSum := data[len(data)-sha256.Size:]
	body := data[:len(data)-sha256.Size]
	sum := sha256.Sum256(body)
	if !bytes.Equal(storedSum, sum[:]) {
		return Entry{}, fmt.Errorf("%w: checksum mismatch", ErrCacheCorrupt)
	}

	formatVer := body[len(fileMagic)]
	if formatVer != onDiskVersion {
		return Entry{}, fmt.Errorf("%w: format version %d", ErrCacheVersion, formatVer)
	}

	buf := bytes.NewReader(body[len(fileMagic)+1:])
	verField, err := readBytes(buf)
	if err != nil {
		return Entry{}, err
	}
	if len(verField) != 4 {
		return Entry{}, fmt.Errorf("%w: bad version field", ErrCacheCorrupt)
	}
	taskField, err := readBytes(buf)
	if err != nil {
		return Entry{}, err
	}
	fpField, err := readBytes(buf)
	if err != nil {
		return Entry{}, err
	}
	outField, err := readBytes(buf)
	if err != nil {
		return Entry{}, err
	}
	consumedField, err := readBytes(buf)
	if err != nil {
		return Entry{}, err
	}
	// No trailing garbage allowed.
	if buf.Len() != 0 {
		return Entry{}, fmt.Errorf("%w: trailing bytes", ErrCacheCorrupt)
	}
	consumed, err := decodeConsumed(consumedField)
	if err != nil {
		return Entry{}, err
	}
	// The fingerprint and task fields must be structurally sane; these
	// are hex fingerprints emitted by this package.
	if len(fpField) != sha256.Size*2 {
		return Entry{}, fmt.Errorf("%w: bad fingerprint length", ErrCacheCorrupt)
	}
	for _, b := range fpField {
		if !isHex[b] {
			return Entry{}, fmt.Errorf("%w: non-hex fingerprint", ErrCacheCorrupt)
		}
	}
	for _, c := range consumed {
		if len(c.DataHash) != sha256.Size*2 {
			return Entry{}, fmt.Errorf("%w: bad consumed hash length", ErrCacheCorrupt)
		}
		for _, b := range []byte(c.DataHash) {
			if !isHex[b] {
				return Entry{}, fmt.Errorf("%w: non-hex consumed hash", ErrCacheCorrupt)
			}
		}
	}

	entryVer := binary.BigEndian.Uint32(verField)
	return Entry{
		Version:     int(entryVer),
		Task:        TaskID(taskField),
		Fingerprint: string(fpField),
		Consumed:    consumed,
		Output:      Artifact{Data: outField},
	}, nil
}

// validateEntryVersion reports ErrCacheVersion when a structurally
// valid entry carries a logical version this build cannot use. It lets
// callers distinguish "incompatible version" from "corrupt content"
// even though both are handled as a cache miss by the engine.
func validateEntryVersion(e Entry) error {
	if e.Version != Version {
		return fmt.Errorf("%w: entry version %d, want %d",
			ErrCacheVersion, e.Version, Version)
	}
	return nil
}

var isHex [256]bool

func init() {
	for _, b := range []byte("0123456789abcdef") {
		isHex[b] = true
	}
}

// appendBytes appends a uint32 length prefix followed by raw.
func appendBytes(dst, raw []byte) []byte {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(raw)))
	dst = append(dst, l[:]...)
	return append(dst, raw...)
}

// readBytes consumes one length-prefixed field from buf.
func readBytes(buf *bytes.Reader) ([]byte, error) {
	var l [4]byte
	if n, err := buf.Read(l[:]); err != nil || n != 4 {
		return nil, fmt.Errorf("%w: short length prefix", ErrCacheCorrupt)
	}
	size := binary.BigEndian.Uint32(l[:])
	if uint64(size) > uint64(buf.Len()) {
		return nil, fmt.Errorf("%w: declared length %d exceeds remaining %d",
			ErrCacheCorrupt, size, buf.Len())
	}
	out := make([]byte, size)
	if n, err := buf.Read(out); err != nil || n != int(size) {
		return nil, fmt.Errorf("%w: truncated field", ErrCacheCorrupt)
	}
	return out, nil
}
