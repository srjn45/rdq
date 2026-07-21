// SPDX-License-Identifier: Apache-2.0

package policy

// Glob reports whether pattern matches s using the language-neutral wildcard
// syntax the config-glob classification layer speaks (design 03 §4, layer 4).
// Two metacharacters are recognised: "*" matches any run of characters
// (including the empty run and '.'), and "?" matches exactly one character.
//
// Unlike path.Match there is no separator semantics: error.type strings are
// dotted identifiers (`java.net.SocketTimeoutException`, `TIMEOUT`,
// `com.acme.ValidationException`) and '*' is meant to span the dots, so
// `java.net.*` matches `java.net.SocketTimeoutException` and
// `*.ValidationException` matches `com.acme.ValidationException`. Matching is
// case-sensitive and anchored at both ends — a pattern with no wildcards is an
// exact-equality test, which is how a bare code like `TIMEOUT` is written.
//
// The implementation is the classic two-pointer backtracking matcher: linear
// time, no allocation, and no regexp compilation. Because '*' and '?' are the
// only metacharacters, every other byte — including '.' — is a literal, so no
// escaping is needed for the dotted type names this operates on.
func Glob(pattern, s string) bool {
	// px/sx walk the pattern and subject; star/mark remember the position of the
	// last '*' and the subject offset it was tried against, so a later mismatch
	// backtracks by letting that '*' absorb one more character.
	var (
		px, sx     int
		star, mark = -1, 0
	)
	for sx < len(s) {
		switch {
		case px < len(pattern) && pattern[px] == '*':
			star, mark = px, sx
			px++ // tentatively let '*' match nothing; grow it on backtrack.
		case px < len(pattern) && (pattern[px] == '?' || pattern[px] == s[sx]):
			px++
			sx++
		case star >= 0:
			// Mismatch, but an earlier '*' can swallow one more subject byte.
			px = star + 1
			mark++
			sx = mark
		default:
			return false
		}
	}
	// Subject consumed; any trailing pattern must be all '*' to match the empty
	// remainder.
	for px < len(pattern) && pattern[px] == '*' {
		px++
	}
	return px == len(pattern)
}
