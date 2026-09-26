package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
)

const (
	cacheSuffix = ".cache"
	tmpPrefix   = ".tmp-"
	lockName    = ".lock"

	// maxFileNameLen keeps readable file names comfortably below the
	// 255-byte limit shared by common filesystems.
	maxFileNameLen = 240
)

// entryRelPath returns the location of a task's cache file relative to
// the store root. The mapping is derived from the TaskID by an
// injective encoding, so:
//
//   - identifiers containing "/", "\", ":", NUL, "."/"..", Windows
//     device names or other hierarchical fragments never escape the
//     root and never alias another identifier;
//   - every returned path consists only of root-relative components the
//     store itself creates (one optional two-hex-digit shard directory),
//     giving strict isolation between different store directories.
//
// Short identifiers with a conservative character set use a readable
// percent-encoded name; everything else (long names, uppercase letters
// which are unsafe on case-insensitive filesystems, reserved device
// names, ...) falls back to a content-addressed sharded SHA-256 path.
func entryRelPath(id TaskID) string {
	if stem, ok := readableStem(id); ok {
		return stem + cacheSuffix
	}
	sum := sha256.Sum256([]byte(id))
	h := hex.EncodeToString(sum[:])
	return h[:2] + "/" + h + cacheSuffix
}

// readableStem returns the percent-encoded file stem for id and whether
// the readable form is safe to use on every supported filesystem.
func readableStem(id TaskID) (string, bool) {
	if len(id) == 0 {
		return "", false
	}
	var b []byte
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case isLowerAlnum(c) || c == '-' || c == '_':
			b = append(b, c)
		case c >= 'A' && c <= 'Z':
			// Uppercase would make the name depend on filesystem case
			// sensitivity; route such ids through the hash layout.
			return "", false
		default:
			const hex = "0123456789ABCDEF"
			b = append(b, '%', hex[c>>4], hex[c&0x0F])
		}
	}
	if len(b)+len(cacheSuffix) > maxFileNameLen {
		return "", false
	}
	if isReservedName(string(b)) {
		return "", false
	}
	return string(b), true
}

func isLowerAlnum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

// isReservedName reports whether stem is a Windows device name such as
// "nul" or "com1". The readable stem contains only lowercase letters,
// digits, '-', '_' and '%', so a case fold is unnecessary.
func isReservedName(stem string) bool {
	switch stem {
	case "con", "prn", "aux", "nul":
		return true
	}
	if len(stem) == 4 {
		prefix, last := stem[:3], stem[3]
		if (prefix == "com" || prefix == "lpt") && last >= '0' && last <= '9' {
			return true
		}
	}
	return false
}

// isShardDir reports whether name looks like a shard directory created
// by this store (exactly two lowercase hex digits).
func isShardDir(name string) bool {
	if len(name) != 2 {
		return false
	}
	return isHexLower(name[0]) && isHexLower(name[1])
}

func isHexLower(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}

// lockPath is the location of the cross-process advisory lock file.
func lockPath(dir string) string {
	return filepath.Join(dir, lockName)
}
