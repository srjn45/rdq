// SPDX-License-Identifier: Apache-2.0

package envelope

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStatusValidAndParse(t *testing.T) {
	valid := []Status{StatusPending, StatusInFlight, StatusSucceeded, StatusDead}
	for _, s := range valid {
		if !s.Valid() {
			t.Errorf("Status(%q).Valid() = false, want true", s)
		}
		got, err := ParseStatus(string(s))
		if err != nil {
			t.Errorf("ParseStatus(%q) returned error: %v", s, err)
		}
		if got != s {
			t.Errorf("ParseStatus(%q) = %q, want round-trip", s, got)
		}
	}

	for _, bad := range []string{"", "pending", "UNKNOWN", "DONE"} {
		if Status(bad).Valid() {
			t.Errorf("Status(%q).Valid() = true, want false", bad)
		}
		if _, err := ParseStatus(bad); err == nil {
			t.Errorf("ParseStatus(%q) = nil error, want rejection", bad)
		}
	}
}

func TestOutcomeValidAndParse(t *testing.T) {
	valid := []Outcome{
		OutcomeSuccess, OutcomeRetryableFailure, OutcomePermanentFailure, OutcomeLeaseExpired,
	}
	for _, o := range valid {
		if !o.Valid() {
			t.Errorf("Outcome(%q).Valid() = false, want true", o)
		}
		got, err := ParseOutcome(string(o))
		if err != nil {
			t.Errorf("ParseOutcome(%q) returned error: %v", o, err)
		}
		if got != o {
			t.Errorf("ParseOutcome(%q) = %q, want round-trip", o, got)
		}
	}

	for _, bad := range []string{"", "success", "FAILURE", "RETRY"} {
		if Outcome(bad).Valid() {
			t.Errorf("Outcome(%q).Valid() = true, want false", bad)
		}
		if _, err := ParseOutcome(bad); err == nil {
			t.Errorf("ParseOutcome(%q) = nil error, want rejection", bad)
		}
	}
}

// sample mirrors the design 01 §2 example envelope.
func sampleEnvelope(t *testing.T) Envelope {
	t.Helper()
	next := time.Date(2026, 7, 20, 14, 5, 22, 117_000_000, time.UTC)
	created := time.Date(2026, 7, 20, 14, 3, 22, 117_000_000, time.UTC)
	started := time.Date(2026, 7, 20, 14, 3, 22, 200_000_000, time.UTC)
	finished := time.Date(2026, 7, 20, 14, 3, 22, 950_000_000, time.UTC)
	return Envelope{
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
		Status:         StatusPending,
		AttemptCount:   2,
		RedriveCount:   0,
		NextAttemptAt:  &next,
		LeaseExpiresAt: nil,
		CreatedAt:      created,
		Attempts: []Attempt{
			{
				AttemptNo:  1,
				StartedAt:  started,
				FinishedAt: &finished,
				Outcome:    OutcomeRetryableFailure,
				Error: &Error{
					Type:    "java.net.SocketTimeoutException",
					Message: "connect timed out after 500ms",
					Stack:   "java.net.SocketTimeoutException: connect timed out\n\tat ...",
				},
			},
		},
	}
}

func TestEnvelopeMarshalsToSnakeCaseShape(t *testing.T) {
	raw, err := json.Marshal(sampleEnvelope(t))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("Unmarshal to map: %v", err)
	}

	// Every design 01 §2 top-level field must be present with its snake_case key.
	for _, key := range []string{
		"envelope_version", "id", "queue", "handler_ref", "handler_version",
		"payload", "payload_content_type", "headers", "status", "attempt_count",
		"redrive_count", "next_attempt_at", "lease_expires_at", "created_at", "attempts",
	} {
		if _, ok := m[key]; !ok {
			t.Errorf("marshaled envelope missing key %q", key)
		}
	}

	// payload_ref is reserved and unused: omitted when nil.
	if _, ok := m["payload_ref"]; ok {
		t.Errorf("payload_ref should be omitted when nil")
	}

	// Opaque payload is base64-encoded (design 01 §1).
	var payload string
	if err := json.Unmarshal(m["payload"], &payload); err != nil {
		t.Fatalf("payload not a JSON string: %v", err)
	}
	if want := "eyJvcmRlcl9pZCI6IDQyfQ=="; payload != want {
		t.Errorf("payload = %q, want base64 %q", payload, want)
	}

	// lease_expires_at is present and null (design 01: null unless IN_FLIGHT).
	if string(m["lease_expires_at"]) != "null" {
		t.Errorf("lease_expires_at = %s, want null", m["lease_expires_at"])
	}

	// Attempt + error sub-shapes.
	var attempts []map[string]json.RawMessage
	if err := json.Unmarshal(m["attempts"], &attempts); err != nil {
		t.Fatalf("attempts not an array of objects: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("attempts len = %d, want 1", len(attempts))
	}
	for _, key := range []string{"attempt_no", "started_at", "finished_at", "outcome", "error"} {
		if _, ok := attempts[0][key]; !ok {
			t.Errorf("attempt missing key %q", key)
		}
	}
	var errObj map[string]json.RawMessage
	if err := json.Unmarshal(attempts[0]["error"], &errObj); err != nil {
		t.Fatalf("error not an object: %v", err)
	}
	for _, key := range []string{"type", "message"} {
		if _, ok := errObj[key]; !ok {
			t.Errorf("error missing key %q", key)
		}
	}
	// detail was unset → omitted; stack was set → present.
	if _, ok := errObj["detail"]; ok {
		t.Errorf("error.detail should be omitted when unset")
	}
	if _, ok := errObj["stack"]; !ok {
		t.Errorf("error.stack should be present when set")
	}
}

func TestEnvelopePayloadRefEmittedWhenSet(t *testing.T) {
	ref := "s3://bucket/key"
	env := Envelope{PayloadRef: &ref}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if _, ok := m["payload_ref"]; !ok {
		t.Errorf("payload_ref should be present when set")
	}
}
