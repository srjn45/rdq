// SPDX-License-Identifier: Apache-2.0

package envelope

import (
	"fmt"
	"regexp"
	"unicode/utf8"
)

// Validation and truncation helpers for the wire envelope (design 01 §2, §5).
// Names (queue, handler_ref) are constrained to a small ASCII charset and
// length so they are safe as identifiers across storage backends and languages;
// free-text error fields are truncated to fixed byte budgets with a visible
// sentinel so a runaway message or stack can never bloat a stored envelope.

const (
	// MaxNameLen is the maximum length, in bytes, of a queue or handler name
	// (design 01 §2). The charset is ASCII, so bytes == characters here.
	MaxNameLen = 240

	// MaxMessageBytes is the byte budget for error.message (design 01 §2:
	// truncated at 4 KiB).
	MaxMessageBytes = 4 << 10 // 4096

	// MaxStackBytes is the byte budget for error.stack (design 01 §2: truncated
	// at 64 KiB).
	MaxStackBytes = 64 << 10 // 65536

	// TruncationMarker is appended to a value that was shortened, so a reader
	// can tell truncation from a naturally short value (design 01 §2). The
	// leading rune is U+2026 HORIZONTAL ELLIPSIS (three UTF-8 bytes).
	TruncationMarker = "…[truncated]"

	// CurrentVersion is this engine's own envelope_version. It reads every
	// version ≤ its own and writes its own (design 01 §5, rule 2).
	CurrentVersion = 1
)

// nameRE is the queue/handler charset rule from design 01 §2: one or more of
// lowercase ASCII letters, digits, dot, underscore, hyphen. The + anchors an
// implicit non-empty requirement; length is checked separately so an oversized
// name gets a length error rather than a generic charset one.
var nameRE = regexp.MustCompile(`^[a-z0-9._-]+$`)

// ValidateQueue reports whether name is a legal queue name (design 01 §2).
func ValidateQueue(name string) error { return validateName("queue", name) }

// ValidateHandlerRef reports whether name is a legal handler_ref (design 01 §2,
// same charset rule as queue).
func ValidateHandlerRef(name string) error { return validateName("handler_ref", name) }

// validateName enforces the shared charset/length rule, naming the field in any
// error so callers get "invalid queue" vs "invalid handler_ref".
func validateName(field, name string) error {
	if name == "" {
		return fmt.Errorf("envelope: invalid %s: must not be empty", field)
	}
	if len(name) > MaxNameLen {
		return fmt.Errorf("envelope: invalid %s %q: %d bytes exceeds max %d", field, name, len(name), MaxNameLen)
	}
	if !nameRE.MatchString(name) {
		return fmt.Errorf("envelope: invalid %s %q: must match [a-z0-9._-]", field, name)
	}
	return nil
}

// CanRead reports whether an envelope written at version v is readable by this
// engine: any version at or below its own (design 01 §5, rule 2). A version of
// 0 or below never appears on the wire and is not readable.
func CanRead(v int) bool { return v >= 1 && v <= CurrentVersion }

// WriteVersion returns the envelope_version this engine stamps on everything it
// writes — always its own (design 01 §5, rule 2).
func WriteVersion() int { return CurrentVersion }

// ValidateReadVersion is the error-returning form of CanRead: it rejects a
// version this engine is too old to understand (v > CurrentVersion) and a
// nonsensical non-positive version.
func ValidateReadVersion(v int) error {
	if v < 1 {
		return fmt.Errorf("envelope: invalid envelope_version %d: must be >= 1", v)
	}
	if v > CurrentVersion {
		return fmt.Errorf("envelope: envelope_version %d is newer than this engine's %d", v, CurrentVersion)
	}
	return nil
}

// Truncate returns s unchanged when it fits within limit bytes, otherwise a
// prefix of s followed by TruncationMarker, with the whole result never
// exceeding limit bytes. The cut is moved back to a UTF-8 rune boundary so a
// multi-byte character is never split (design 01 §2).
func Truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	budget := limit - len(TruncationMarker)
	if budget < 0 {
		// limit is smaller than the marker itself (never happens for the real
		// 4 KiB / 64 KiB budgets); return as much of the marker as fits so the
		// result still never exceeds limit.
		return runeSafePrefix(TruncationMarker, limit)
	}
	return runeSafePrefix(s, budget) + TruncationMarker
}

// runeSafePrefix returns s[:n], moved back to a UTF-8 rune boundary so a
// multi-byte character is never split. n is assumed to be within [0, len(s)].
func runeSafePrefix(s string, n int) string {
	for n > 0 && n < len(s) && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// TruncateMessage truncates an error message to the 4 KiB budget (design 01 §2).
func TruncateMessage(s string) string { return Truncate(s, MaxMessageBytes) }

// TruncateStack truncates an error stack to the 64 KiB budget (design 01 §2).
func TruncateStack(s string) string { return Truncate(s, MaxStackBytes) }

// TruncateError shortens an Error's Message and Stack in place to their byte
// budgets (design 01 §2). It is nil-safe so writers can call it unconditionally.
func TruncateError(e *Error) {
	if e == nil {
		return
	}
	e.Message = TruncateMessage(e.Message)
	e.Stack = TruncateStack(e.Stack)
}
