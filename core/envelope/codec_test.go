// SPDX-License-Identifier: Apache-2.0

package envelope

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// updateGolden regenerates the testdata/*.json fixtures. These fixtures are the
// frozen cross-language contract (T1.2, frozen at T1.9); regenerate only with a
// deliberate spec change: `go test ./envelope/ -run Golden -update`.
var updateGolden = flag.Bool("update", false, "regenerate golden fixtures")

func ms(y int, mo time.Month, d, h, mi, s, milli int) time.Time {
	return time.Date(y, mo, d, h, mi, s, milli*1_000_000, time.UTC)
}

func tp(t time.Time) *time.Time { return &t }

// goldenFixtures maps each frozen fixture filename to the Envelope it encodes.
// Every fixture round-trips byte-stably through Marshal/Unmarshal.
func goldenFixtures() map[string]Envelope {
	return map[string]Envelope{
		// The design 01 §2 reference envelope: base64 payload, header map, a
		// Java-native error.type, a null lease, one retryable attempt.
		"envelope_full.json": {
			EnvelopeVersion:    1,
			ID:                 "01J2ZK7Q8XW5H3N9G4T6B8RDQ0",
			Queue:              "payments.charge",
			HandlerRef:         "charge-payment",
			HandlerVersion:     "v3",
			Payload:            []byte(`{"order_id": 42}`),
			PayloadContentType: "application/json",
			Headers: map[string]string{
				"traceparent":      "00-4bf9...-01",
				"rdq.source":       "kafka://payments/3/42351",
				"rdq.submitted_by": "checkout-service",
			},
			Status:        StatusPending,
			AttemptCount:  2,
			RedriveCount:  0,
			NextAttemptAt: tp(ms(2026, 7, 20, 14, 5, 22, 117)),
			CreatedAt:     ms(2026, 7, 20, 14, 3, 22, 117),
			Attempts: []Attempt{{
				AttemptNo:  1,
				StartedAt:  ms(2026, 7, 20, 14, 3, 22, 200),
				FinishedAt: tp(ms(2026, 7, 20, 14, 3, 22, 950)),
				Outcome:    OutcomeRetryableFailure,
				Error: &Error{
					Type:    "java.net.SocketTimeoutException",
					Message: "connect timed out after 500ms",
					Stack:   "java.net.SocketTimeoutException: connect timed out\n\tat ...",
				},
			}},
		},

		// G7: a LEASE_EXPIRED attempt. Its error.type is "rdq.LeaseExpired",
		// the message states the lease deadline, and there is no stack. The
		// prior attempt carries a Go %T error.type (G6 fallback branch).
		"lease_expired.json": {
			EnvelopeVersion:    1,
			ID:                 "01J2ZKQP0T4S6V8X0Z2B4D6F8H",
			Queue:              "notifications.send",
			HandlerRef:         "send-email",
			Payload:            []byte("payload-bytes"),
			PayloadContentType: "application/octet-stream",
			Headers:            map[string]string{"rdq.submitted_by": "notify-service"},
			Status:             StatusPending,
			AttemptCount:       2,
			RedriveCount:       0,
			NextAttemptAt:      tp(ms(2026, 7, 20, 14, 12, 22, 117)),
			CreatedAt:          ms(2026, 7, 20, 14, 3, 22, 117),
			Attempts: []Attempt{
				{
					AttemptNo:  1,
					StartedAt:  ms(2026, 7, 20, 14, 3, 22, 200),
					FinishedAt: tp(ms(2026, 7, 20, 14, 3, 22, 950)),
					Outcome:    OutcomeRetryableFailure,
					// G6 fallback: %T of the innermost unwrapped Go error.
					Error: &Error{
						Type:    "*net.OpError",
						Message: "dial tcp 10.0.0.5:25: i/o timeout",
					},
				},
				{
					AttemptNo: 2,
					StartedAt: ms(2026, 7, 20, 14, 5, 22, 200),
					// No finished_at: the worker never reported an outcome.
					FinishedAt: nil,
					Outcome:    OutcomeLeaseExpired,
					Error: &Error{
						Type:    "rdq.LeaseExpired",
						Message: "lease expired at 2026-07-20T14:07:22.117Z",
					},
				},
			},
		},

		// G6: both branches of the Go error.type convention. Attempt 1 uses a
		// classifier/wrapper-supplied name (wins when present) plus a structured
		// detail; attempt 2 uses the %T fallback. Terminal DEAD envelope.
		"error_type_go.json": {
			EnvelopeVersion:    1,
			ID:                 "01J2ZM0000000000000000000A",
			Queue:              "billing.invoice",
			HandlerRef:         "issue-invoice",
			Payload:            []byte("{}"),
			PayloadContentType: "application/json",
			Status:             StatusDead,
			AttemptCount:       2,
			RedriveCount:       1,
			NextAttemptAt:      nil,
			LeaseExpiresAt:     nil,
			CreatedAt:          ms(2026, 7, 20, 14, 0, 0, 0),
			Attempts: []Attempt{
				{
					AttemptNo:  1,
					StartedAt:  ms(2026, 7, 20, 14, 0, 1, 0),
					FinishedAt: tp(ms(2026, 7, 20, 14, 0, 1, 500)),
					Outcome:    OutcomeRetryableFailure,
					// Classifier-supplied name wins over %T.
					Error: &Error{
						Type:    "billing.CardDeclined",
						Message: "card declined",
						Detail:  json.RawMessage(`{"decline_code":"insufficient_funds"}`),
					},
				},
				{
					AttemptNo:  2,
					StartedAt:  ms(2026, 7, 20, 14, 1, 0, 0),
					FinishedAt: tp(ms(2026, 7, 20, 14, 1, 0, 250)),
					Outcome:    OutcomePermanentFailure,
					// %T fallback: innermost unwrapped error is errors.New's type.
					Error: &Error{
						Type:    "*errors.errorString",
						Message: "invoice already finalized",
					},
				},
			},
		},

		// T1.3: a task written by a newer envelope_version carries fields this
		// reader does not know. They must survive the round-trip verbatim
		// (design 01 §5, rule 1) — both extra top-level fields and extra
		// per-attempt fields. Unknown keys are re-emitted sorted, after all
		// known fields of their object.
		"unknown_fields.json": {
			EnvelopeVersion:    1,
			ID:                 "01J2ZN0000000000000000000B",
			Queue:              "orders.reserve",
			HandlerRef:         "reserve-stock",
			Payload:            []byte("{}"),
			PayloadContentType: "application/json",
			Status:             StatusPending,
			AttemptCount:       1,
			RedriveCount:       0,
			NextAttemptAt:      tp(ms(2026, 7, 20, 15, 0, 0, 0)),
			CreatedAt:          ms(2026, 7, 20, 14, 30, 0, 0),
			// Extra top-level fields (a scalar and a nested object).
			Residual: map[string]json.RawMessage{
				"future_priority": json.RawMessage(`7`),
				"x_experimental":  json.RawMessage(`{"canary":true}`),
			},
			Attempts: []Attempt{{
				AttemptNo:  1,
				StartedAt:  ms(2026, 7, 20, 14, 30, 1, 0),
				FinishedAt: tp(ms(2026, 7, 20, 14, 30, 1, 250)),
				Outcome:    OutcomeRetryableFailure,
				Error: &Error{
					Type:    "*errors.errorString",
					Message: "insufficient stock",
				},
				// Extra per-attempt fields (a scalar and a string).
				Residual: map[string]json.RawMessage{
					"future_latency_ms": json.RawMessage(`142`),
					"trace_flags":       json.RawMessage(`"01"`),
				},
			}},
		},
	}
}

func TestGoldenFixtures(t *testing.T) {
	dir := "testdata"
	for name, env := range goldenFixtures() {
		env := env
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name)

			got, err := Marshal(&env)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}

			if *updateGolden {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatalf("MkdirAll: %v", err)
				}
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatalf("WriteFile: %v", err)
				}
				return
			}

			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile (run with -update to create): %v", err)
			}
			// Marshal(value) must equal the frozen fixture byte-for-byte.
			if string(got) != string(want) {
				t.Errorf("Marshal mismatch\n got: %s\nwant: %s", got, want)
			}

			// read(write(x)) == x, byte-stable: decode the fixture, re-encode,
			// and require identical bytes.
			decoded, err := Unmarshal(want)
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			round, err := Marshal(decoded)
			if err != nil {
				t.Fatalf("re-Marshal: %v", err)
			}
			if string(round) != string(want) {
				t.Errorf("round-trip not byte-stable\n got: %s\nwant: %s", round, want)
			}
		})
	}
}

// TestResidualRoundTrip asserts that unknown fields — top-level AND per-attempt
// — captured on decode survive re-encode byte-for-byte (design 01 §5, rule 1).
func TestResidualRoundTrip(t *testing.T) {
	want, err := os.ReadFile(filepath.Join("testdata", "unknown_fields.json"))
	if err != nil {
		t.Fatalf("ReadFile (run TestGoldenFixtures with -update to create): %v", err)
	}

	env, err := Unmarshal(want)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// Top-level unknown fields are captured, values verbatim.
	assertResidual(t, "top-level", env.Residual, map[string]string{
		"future_priority": "7",
		"x_experimental":  `{"canary":true}`,
	})
	// Known keys never leak into the residual.
	if _, ok := env.Residual["attempts"]; ok {
		t.Errorf(`known key "attempts" captured as residual`)
	}

	// Per-attempt unknown fields are captured on the right attempt.
	if len(env.Attempts) != 1 {
		t.Fatalf("attempts = %d, want 1", len(env.Attempts))
	}
	assertResidual(t, "attempt", env.Attempts[0].Residual, map[string]string{
		"future_latency_ms": "142",
		"trace_flags":       `"01"`,
	})

	// read(write(x)) == x, byte-stable, with the residual preserved.
	round, err := Marshal(env)
	if err != nil {
		t.Fatalf("re-Marshal: %v", err)
	}
	if string(round) != string(want) {
		t.Errorf("residual round-trip not byte-stable\n got: %s\nwant: %s", round, want)
	}
}

// TestNoResidualIsNil guards that the residual capture never fires for envelopes
// that carry only known fields — the existing T1.2 fixtures decode to nil maps.
func TestNoResidualIsNil(t *testing.T) {
	for _, name := range []string{"envelope_full.json", "error_type_go.json", "lease_expired.json"} {
		data, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatalf("ReadFile %s: %v", name, err)
		}
		env, err := Unmarshal(data)
		if err != nil {
			t.Fatalf("Unmarshal %s: %v", name, err)
		}
		if env.Residual != nil {
			t.Errorf("%s: envelope residual = %v, want nil", name, env.Residual)
		}
		for i, a := range env.Attempts {
			if a.Residual != nil {
				t.Errorf("%s: attempt %d residual = %v, want nil", name, i, a.Residual)
			}
		}
	}
}

func assertResidual(t *testing.T, where string, got map[string]json.RawMessage, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s residual has %d keys, want %d (%v)", where, len(got), len(want), got)
	}
	for k, v := range want {
		raw, ok := got[k]
		if !ok {
			t.Errorf("%s residual missing key %q", where, k)
			continue
		}
		if string(raw) != v {
			t.Errorf("%s residual[%q] = %s, want %s", where, k, raw, v)
		}
	}
}

func TestTimeCanonicalMillis(t *testing.T) {
	cases := []struct {
		in   time.Time
		want string
	}{
		{ms(2026, 7, 20, 14, 3, 22, 117), `"2026-07-20T14:03:22.117Z"`},
		{ms(2026, 7, 20, 14, 3, 22, 200), `"2026-07-20T14:03:22.200Z"`}, // trailing zeros kept
		{ms(2026, 7, 20, 14, 3, 22, 0), `"2026-07-20T14:03:22.000Z"`},   // always three digits
		// Non-UTC input is normalized to UTC.
		{time.Date(2026, 7, 20, 16, 3, 22, 117_000_000, time.FixedZone("CEST", 2*3600)), `"2026-07-20T14:03:22.117Z"`},
	}
	for _, c := range cases {
		got, err := Time(c.in).MarshalJSON()
		if err != nil {
			t.Fatalf("MarshalJSON: %v", err)
		}
		if string(got) != c.want {
			t.Errorf("Time(%v).MarshalJSON() = %s, want %s", c.in, got, c.want)
		}
		var back Time
		if err := back.UnmarshalJSON([]byte(c.want)); err != nil {
			t.Fatalf("UnmarshalJSON(%s): %v", c.want, err)
		}
		if !time.Time(back).Equal(c.in) {
			t.Errorf("UnmarshalJSON(%s) = %v, want %v", c.want, time.Time(back), c.in)
		}
	}
}

func TestTimeUnmarshalNull(t *testing.T) {
	// A null lease_expires_at decodes to a nil *time.Time on the envelope.
	env, err := Unmarshal([]byte(`{"envelope_version":1,"id":"x","queue":"q","handler_ref":"h","payload":null,"payload_content_type":"","status":"PENDING","attempt_count":0,"redrive_count":0,"next_attempt_at":null,"lease_expires_at":null,"created_at":"2026-07-20T14:03:22.117Z"}`))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if env.NextAttemptAt != nil || env.LeaseExpiresAt != nil {
		t.Errorf("null timestamps decoded to non-nil pointers: next=%v lease=%v", env.NextAttemptAt, env.LeaseExpiresAt)
	}
	if !env.CreatedAt.Equal(ms(2026, 7, 20, 14, 3, 22, 117)) {
		t.Errorf("created_at = %v", env.CreatedAt)
	}
}

func TestDurationMillis(t *testing.T) {
	got, err := Duration(1500 * time.Millisecond).MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if string(got) != "1500" {
		t.Errorf("Duration(1.5s).MarshalJSON() = %s, want 1500", got)
	}
	var d Duration
	if err := d.UnmarshalJSON([]byte("1500")); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if time.Duration(d) != 1500*time.Millisecond {
		t.Errorf("UnmarshalJSON(1500) = %v, want 1.5s", time.Duration(d))
	}
	if _, err := Marshal(&Envelope{}); err != nil { // sanity: codec still builds
		t.Fatalf("Marshal empty: %v", err)
	}
}

func TestULIDParseRoundTrip(t *testing.T) {
	const canonical = "01J2ZK7Q8XW5H3N9G4T6B8RDQ0"
	id, err := ParseULID(canonical)
	if err != nil {
		t.Fatalf("ParseULID: %v", err)
	}
	if id.String() != canonical {
		t.Errorf("round-trip = %q, want %q", id.String(), canonical)
	}

	// MarshalText/UnmarshalText and JSON-string encoding round-trip.
	text, err := id.MarshalText()
	if err != nil || string(text) != canonical {
		t.Fatalf("MarshalText = %q, %v", text, err)
	}
	var back ULID
	if err := back.UnmarshalText([]byte(canonical)); err != nil || back != id {
		t.Fatalf("UnmarshalText = %v, %v", back, err)
	}
}

func TestULIDGenerateEncodesTimestamp(t *testing.T) {
	at := ms(2026, 7, 20, 14, 3, 22, 117)
	id, err := NewULIDAt(at)
	if err != nil {
		t.Fatalf("NewULIDAt: %v", err)
	}
	if !id.Time().Equal(at) {
		t.Errorf("id.Time() = %v, want %v", id.Time(), at)
	}
	// Generated ids parse back to themselves.
	back, err := ParseULID(id.String())
	if err != nil || back != id {
		t.Fatalf("ParseULID(generated) = %v, %v", back, err)
	}
}

func TestULIDParseRejects(t *testing.T) {
	bad := []string{
		"",                            // empty
		"01J2ZK7Q8XW5H3N9G4T6B8RDQ",   // 25 chars
		"01J2ZK7Q8XW5H3N9G4T6B8RDQ00", // 27 chars
		"01J2ZK7Q8XW5H3N9G4T6B8RDQI",  // 'I' not in alphabet
		"81J2ZK7Q8XW5H3N9G4T6B8RDQ0",  // first char > 7 → overflow
	}
	for _, s := range bad {
		if _, err := ParseULID(s); err == nil {
			t.Errorf("ParseULID(%q) = nil error, want rejection", s)
		}
	}
}
