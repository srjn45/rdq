// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/srjn45/rdq/core/envelope"
)

// fixtureDir points at the frozen cross-language contract fixtures produced by
// T1.2. The Postgres mapping is held to the same golden bytes as the Go and Java
// codecs (design 01 §1) — decomposing to rows and back must not perturb them.
const fixtureDir = "../../core/envelope/testdata"

// fixtures are the T1.2 golden envelopes exercised by the round-trip: a full
// envelope, a LEASE_EXPIRED attempt (G7), a Go error.type set (G6), and one
// carrying unknown top-level + per-attempt fields (design 01 §5).
var fixtures = []string{
	"envelope_full.json",
	"lease_expired.json",
	"error_type_go.json",
	"unknown_fields.json",
}

// TestRoundTripFixtures is the T2.2 acceptance: every frozen fixture decomposes
// into task + attempt rows and reassembles into an envelope that re-encodes to
// the exact same canonical bytes, including preserved unknown fields.
func TestRoundTripFixtures(t *testing.T) {
	for _, name := range fixtures {
		t.Run(name, func(t *testing.T) {
			want, err := os.ReadFile(filepath.Join(fixtureDir, name))
			if err != nil {
				t.Fatalf("reading fixture: %v", err)
			}
			orig, err := envelope.Unmarshal(want)
			if err != nil {
				t.Fatalf("unmarshaling fixture: %v", err)
			}

			row, err := taskRowFromEnvelope(orig)
			if err != nil {
				t.Fatalf("taskRowFromEnvelope: %v", err)
			}
			attempts, err := attemptRowsFromEnvelope(orig)
			if err != nil {
				t.Fatalf("attemptRowsFromEnvelope: %v", err)
			}

			got, err := envelopeFromRows(row, attempts)
			if err != nil {
				t.Fatalf("envelopeFromRows: %v", err)
			}
			if !reflect.DeepEqual(orig, got) {
				t.Errorf("envelope not equal after round-trip\n orig = %+v\n got  = %+v", orig, got)
			}

			gotBytes, err := envelope.Marshal(got)
			if err != nil {
				t.Fatalf("re-marshaling: %v", err)
			}
			if string(gotBytes) != string(want) {
				t.Errorf("canonical bytes not stable after round-trip\n want = %s\n got  = %s", want, gotBytes)
			}
		})
	}
}

// TestUnknownFieldsSurvive pins the residual-preservation contract at the row
// boundary: both the top-level and per-attempt unknown fields must land in the
// residual JSONB payloads and reappear intact.
func TestUnknownFieldsSurvive(t *testing.T) {
	want, err := os.ReadFile(filepath.Join(fixtureDir, "unknown_fields.json"))
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	orig, err := envelope.Unmarshal(want)
	if err != nil {
		t.Fatalf("unmarshaling fixture: %v", err)
	}

	row, err := taskRowFromEnvelope(orig)
	if err != nil {
		t.Fatalf("taskRowFromEnvelope: %v", err)
	}
	if len(orig.Residual) == 0 {
		t.Fatal("fixture precondition: expected top-level residual fields")
	}
	// The top-level unknown fields must be carried in the residual JSONB, not
	// silently dropped into "{}".
	if string(row.Residual) == "{}" || len(row.Residual) == 0 {
		t.Fatalf("top-level residual not captured in row: %q", row.Residual)
	}

	attempts, err := attemptRowsFromEnvelope(orig)
	if err != nil {
		t.Fatalf("attemptRowsFromEnvelope: %v", err)
	}
	if len(attempts) == 0 || string(attempts[0].Residual) == "{}" {
		t.Fatalf("per-attempt residual not captured in row: %+v", attempts)
	}

	got, err := envelopeFromRows(row, attempts)
	if err != nil {
		t.Fatalf("envelopeFromRows: %v", err)
	}
	if !reflect.DeepEqual(orig.Residual, got.Residual) {
		t.Errorf("top-level residual lost:\n want %v\n got  %v", orig.Residual, got.Residual)
	}
	if !reflect.DeepEqual(orig.Attempts[0].Residual, got.Attempts[0].Residual) {
		t.Errorf("attempt residual lost:\n want %v\n got  %v", orig.Attempts[0].Residual, got.Attempts[0].Residual)
	}
}

// TestEmptyMapsAreNil checks that the JSONB "{}" sentinel for absent headers and
// residual reassembles to nil maps, so an envelope without headers/unknown fields
// re-encodes with those keys omitted rather than as empty objects.
func TestEmptyMapsAreNil(t *testing.T) {
	e := &envelope.Envelope{
		EnvelopeVersion:    1,
		ID:                 "01J2ZN0000000000000000000B",
		Queue:              "orders.reserve",
		HandlerRef:         "reserve-stock",
		Payload:            []byte("{}"),
		PayloadContentType: "application/json",
		Status:             envelope.StatusPending,
	}
	row, err := taskRowFromEnvelope(e)
	if err != nil {
		t.Fatalf("taskRowFromEnvelope: %v", err)
	}
	if string(row.Headers) != "{}" || string(row.Residual) != "{}" {
		t.Fatalf("empty maps should encode as {}, got headers=%q residual=%q", row.Headers, row.Residual)
	}
	got, err := envelopeFromRows(row, nil)
	if err != nil {
		t.Fatalf("envelopeFromRows: %v", err)
	}
	if got.Headers != nil {
		t.Errorf("empty headers JSONB should decode to nil map, got %v", got.Headers)
	}
	if got.Residual != nil {
		t.Errorf("empty residual JSONB should decode to nil map, got %v", got.Residual)
	}
}

// TestTerminalErrorType checks the DLQ error_type denormalization: the value is
// the last attempt's error type (indexed for DLQFilter pushdown), nil when there
// is no attempt history.
func TestTerminalErrorType(t *testing.T) {
	want, err := os.ReadFile(filepath.Join(fixtureDir, "error_type_go.json"))
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	dead, err := envelope.Unmarshal(want)
	if err != nil {
		t.Fatalf("unmarshaling fixture: %v", err)
	}
	got := terminalErrorType(dead)
	if got == nil || *got != "*errors.errorString" {
		t.Errorf("terminalErrorType = %v, want *errors.errorString", got)
	}

	if terminalErrorType(&envelope.Envelope{}) != nil {
		t.Error("terminalErrorType of an attempt-less envelope should be nil")
	}
}
