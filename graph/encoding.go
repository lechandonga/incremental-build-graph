package graph

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// fileMagic prefixes every on-disk cache entry.
var fileMagic = []byte("IBGC")

// onDiskVersion is the format version written to disk. Bumped whenever
// the wire format changes incompatibly.
const onDiskVersion = 1

// encodeEntry serializes an Entry into the versioned, length-prefixed
// on-disk format:
//
//	magic (4) | version (1) | payload | checksum (32)
//
// where payload is a sequence of length-prefixed (uint32 BE) fields:
// version(int), task, fingerprint, output data. The trailing checksum
// is SHA-256 over everything preceding it (magic + version + payload),
// so truncation or bit rot is always detected.
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

	sum := sha256.Sum256(buf.Bytes())
	buf.Write(sum[:])
	return buf.Bytes()
}

// decodeEntry parses data produced by encodeEntry. It returns
// ErrCacheVersion for unsupported versions and ErrCacheCorrupt for any
// truncation or checksum mismatch.
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
	// No trailing garbage allowed.
	if buf.Len() != 0 {
		return Entry{}, fmt.Errorf("%w: trailing bytes", ErrCacheCorrupt)
	}

	entryVer := binary.BigEndian.Uint32(verField)
	return Entry{
		Version:     int(entryVer),
		Task:        TaskID(taskField),
		Fingerprint: string(fpField),
		Output:      Artifact{Data: outField},
	}, nil
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
