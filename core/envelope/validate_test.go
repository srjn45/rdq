// SPDX-License-Identifier: Apache-2.0

package envelope

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestValidateName(t *testing.T) {
	cases := []struct {
		name  string
		input string
		valid bool
	}{
		{"simple", "orders", true},
		{"digits", "queue123", true},
		{"dot", "orders.priority", true},
		{"underscore", "dead_letter", true},
		{"hyphen", "orders-us-east", true},
		{"all-allowed-punct", "a.b_c-d.0", true},
		{"single-char", "q", true},
		{"max-length", strings.Repeat("a", MaxNameLen), true},

		{"empty", "", false},
		{"uppercase", "Orders", false},
		{"space", "order queue", false},
		{"slash", "orders/priority", false},
		{"colon", "orders:1", false},
		{"unicode", "café", false},
		{"leading-space", " orders", false},
		{"too-long", strings.Repeat("a", MaxNameLen+1), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// queue and handler_ref share the charset rule; exercise both.
			for _, fn := range []struct {
				label    string
				validate func(string) error
			}{
				{"queue", ValidateQueue},
				{"handler_ref", ValidateHandlerRef},
			} {
				err := fn.validate(c.input)
				if c.valid && err != nil {
					t.Errorf("%s(%q) = %v, want nil", fn.label, c.input, err)
				}
				if !c.valid && err == nil {
					t.Errorf("%s(%q) = nil, want error", fn.label, c.input)
				}
			}
		})
	}
}

func TestTruncateBoundary(t *testing.T) {
	markerLen := len(TruncationMarker)

	cases := []struct {
		name      string
		input     string
		limit     int
		truncated bool
	}{
		// Limits sit comfortably above the marker length so a truncated result
		// carries the whole marker; the sub-marker-limit invariant (result never
		// exceeds limit) is covered by TestTruncateNeverSplitsRune.
		{"empty", "", 40, false},
		{"under-limit", "abc", 40, false},
		{"exactly-limit", strings.Repeat("a", 40), 40, false},
		{"one-over-limit", strings.Repeat("a", 41), 40, true},
		{"far-over-limit", strings.Repeat("a", 100), 40, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Truncate(c.input, c.limit)
			if !c.truncated {
				if got != c.input {
					t.Fatalf("Truncate(%q, %d) = %q, want unchanged", c.input, c.limit, got)
				}
				return
			}
			if len(got) > c.limit {
				t.Errorf("Truncate(...) len = %d, exceeds limit %d", len(got), c.limit)
			}
			if !strings.HasSuffix(got, TruncationMarker) {
				t.Errorf("Truncate(...) = %q, want suffix %q", got, TruncationMarker)
			}
			// The surviving prefix must be a genuine prefix of the input.
			prefix := got[:len(got)-markerLen]
			if !strings.HasPrefix(c.input, prefix) {
				t.Errorf("prefix %q is not a prefix of input", prefix)
			}
		})
	}
}

func TestTruncateNeverSplitsRune(t *testing.T) {
	// A run of 3-byte runes (U+2026) so most cut positions land mid-rune; the
	// helper must back up to a boundary and stay valid UTF-8.
	input := strings.Repeat("…", 50)
	for limit := len(TruncationMarker); limit <= len(input); limit++ {
		got := Truncate(input, limit)
		if len(got) > limit {
			t.Fatalf("limit %d: len(got) = %d exceeds limit", limit, len(got))
		}
		if !utf8.ValidString(got) {
			t.Fatalf("limit %d: Truncate produced invalid UTF-8 %q", limit, got)
		}
	}
}

func TestTruncateMessageAndStack(t *testing.T) {
	longMsg := strings.Repeat("x", MaxMessageBytes+100)
	gotMsg := TruncateMessage(longMsg)
	if len(gotMsg) > MaxMessageBytes {
		t.Errorf("TruncateMessage len = %d, exceeds %d", len(gotMsg), MaxMessageBytes)
	}
	if !strings.HasSuffix(gotMsg, TruncationMarker) {
		t.Errorf("TruncateMessage did not mark truncation")
	}

	fitMsg := strings.Repeat("x", MaxMessageBytes)
	if got := TruncateMessage(fitMsg); got != fitMsg {
		t.Errorf("TruncateMessage shortened a message exactly at the limit")
	}

	longStack := strings.Repeat("y", MaxStackBytes+100)
	gotStack := TruncateStack(longStack)
	if len(gotStack) > MaxStackBytes {
		t.Errorf("TruncateStack len = %d, exceeds %d", len(gotStack), MaxStackBytes)
	}
	if !strings.HasSuffix(gotStack, TruncationMarker) {
		t.Errorf("TruncateStack did not mark truncation")
	}
}

func TestTruncateError(t *testing.T) {
	e := &Error{
		Type:    "rdq.SomeError",
		Message: strings.Repeat("m", MaxMessageBytes+50),
		Stack:   strings.Repeat("s", MaxStackBytes+50),
	}
	TruncateError(e)
	if len(e.Message) > MaxMessageBytes {
		t.Errorf("Message not truncated: len %d", len(e.Message))
	}
	if len(e.Stack) > MaxStackBytes {
		t.Errorf("Stack not truncated: len %d", len(e.Stack))
	}
	if e.Type != "rdq.SomeError" {
		t.Errorf("TruncateError altered Type: %q", e.Type)
	}

	// nil-safe.
	TruncateError(nil)
}

func TestVersionSkewRead(t *testing.T) {
	cases := []struct {
		name    string
		version int
		canRead bool
	}{
		{"own-version", CurrentVersion, true},
		{"older-version", CurrentVersion - 1, CurrentVersion-1 >= 1},
		{"newer-version", CurrentVersion + 1, false},
		{"zero", 0, false},
		{"negative", -1, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := CanRead(c.version); got != c.canRead {
				t.Errorf("CanRead(%d) = %v, want %v", c.version, got, c.canRead)
			}
			err := ValidateReadVersion(c.version)
			if c.canRead && err != nil {
				t.Errorf("ValidateReadVersion(%d) = %v, want nil", c.version, err)
			}
			if !c.canRead && err == nil {
				t.Errorf("ValidateReadVersion(%d) = nil, want error", c.version)
			}
		})
	}
}

func TestWriteVersionIsOwn(t *testing.T) {
	if got := WriteVersion(); got != CurrentVersion {
		t.Errorf("WriteVersion() = %d, want own version %d", got, CurrentVersion)
	}
}
