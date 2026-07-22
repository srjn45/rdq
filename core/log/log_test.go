// SPDX-License-Identifier: Apache-2.0

package log_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/srjn45/rdq/core/envelope"
	rdqlog "github.com/srjn45/rdq/core/log"
)

// decodeRecords parses newline-delimited JSON slog output into records.
func decodeRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("record is not valid JSON: %v\n%s", err, line)
		}
		recs = append(recs, m)
	}
	return recs
}

// Acceptance (T6.2): a transition emits a structured record carrying the task id
// and queue.
func TestTransition_EmitsStructuredRecordWithIDAndQueue(t *testing.T) {
	var buf bytes.Buffer
	lg := rdqlog.New(&buf)

	env := envelope.Envelope{
		ID:      "01J2ZK7Q8XW5H3N9G4T6B8RDQ0",
		Queue:   "payments.charge",
		Status:  envelope.StatusInFlight,
		Payload: []byte(`{"order_id":42}`),
	}
	lg.Transition(context.Background(), rdqlog.TransitionClaimed, env)

	recs := decodeRecords(t, &buf)
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	rec := recs[0]
	if got := rec[rdqlog.KeyTransition]; got != string(rdqlog.TransitionClaimed) {
		t.Errorf("transition = %v, want %q", got, rdqlog.TransitionClaimed)
	}
	if got := rec[rdqlog.KeyTaskID]; got != env.ID {
		t.Errorf("task_id = %v, want %q", got, env.ID)
	}
	if got := rec[rdqlog.KeyQueue]; got != env.Queue {
		t.Errorf("queue = %v, want %q", got, env.Queue)
	}
	if got := rec[rdqlog.KeyStatus]; got != string(envelope.StatusInFlight) {
		t.Errorf("status = %v, want %q", got, envelope.StatusInFlight)
	}
}

// Acceptance (FR-25): raw payload bytes NEVER appear in log output; only size and
// a hash are recorded.
func TestTransition_NeverLogsPayloadBytes(t *testing.T) {
	var buf bytes.Buffer
	lg := rdqlog.New(&buf)

	secret := `{"pan":"4111111111111111","cvv":"123","note":"top-secret-payload"}`
	env := envelope.Envelope{
		ID:      "task-redact",
		Queue:   "q",
		Status:  envelope.StatusInFlight,
		Payload: []byte(secret),
	}
	// Every transition kind must be safe.
	for _, tr := range []rdqlog.Transition{
		rdqlog.TransitionClaimed,
		rdqlog.TransitionSucceeded,
		rdqlog.TransitionRetried,
		rdqlog.TransitionDeadLettered,
		rdqlog.TransitionAbandoned,
	} {
		lg.Transition(context.Background(), tr, env)
	}

	out := buf.String()
	for _, needle := range []string{"4111111111111111", "top-secret-payload", "cvv"} {
		if strings.Contains(out, needle) {
			t.Fatalf("log leaked payload substring %q:\n%s", needle, out)
		}
	}
	// Base64 of the raw bytes must not appear either (slog renders []byte as base64
	// if it were ever passed as a value).
	if strings.Contains(out, "eyJwYW4i") { // base64 prefix of the secret JSON
		t.Fatalf("log leaked base64-encoded payload:\n%s", out)
	}

	// But the safe facts ARE present: byte length and a hash.
	rec := decodeRecords(t, &buf)[0]
	if _, ok := rec[rdqlog.KeyPayloadBytes]; !ok {
		t.Errorf("missing %s", rdqlog.KeyPayloadBytes)
	}
	if got := rec[rdqlog.KeyPayloadBytes]; got != float64(len(secret)) {
		t.Errorf("payload_bytes = %v, want %d", got, len(secret))
	}
	if _, ok := rec[rdqlog.KeyPayloadSHA256]; !ok {
		t.Errorf("missing %s", rdqlog.KeyPayloadSHA256)
	}
}

func TestTransition_LogsTraceIDFromHeaders(t *testing.T) {
	var buf bytes.Buffer
	lg := rdqlog.New(&buf)

	const tid = "4bf92f3577b34da6a3ce929d0e0e4736"
	const sid = "00f067aa0ba902b7"
	env := envelope.Envelope{
		ID:     "t1",
		Queue:  "q",
		Status: envelope.StatusInFlight,
		Headers: map[string]string{
			rdqlog.HeaderTraceparent: "00-" + tid + "-" + sid + "-01",
		},
	}
	lg.Transition(context.Background(), rdqlog.TransitionClaimed, env)

	rec := decodeRecords(t, &buf)[0]
	if got := rec[rdqlog.KeyTraceID]; got != tid {
		t.Errorf("trace_id = %v, want %q", got, tid)
	}
	if got := rec[rdqlog.KeySpanID]; got != sid {
		t.Errorf("span_id = %v, want %q", got, sid)
	}
}

// A traceparent set on the context wins over the one in headers (an active span).
func TestTransition_ContextTraceparentWinsOverHeaders(t *testing.T) {
	var buf bytes.Buffer
	lg := rdqlog.New(&buf)

	const ctxTID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	ctx := rdqlog.ContextWithTraceparent(context.Background(),
		"00-"+ctxTID+"-1111111111111111-01")
	env := envelope.Envelope{
		ID:      "t1",
		Queue:   "q",
		Status:  envelope.StatusInFlight,
		Headers: map[string]string{rdqlog.HeaderTraceparent: "00-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-2222222222222222-01"},
	}
	lg.Transition(ctx, rdqlog.TransitionClaimed, env)

	if got := decodeRecords(t, &buf)[0][rdqlog.KeyTraceID]; got != ctxTID {
		t.Errorf("trace_id = %v, want context value %q", got, ctxTID)
	}
}

func TestNilLogger_IsNoOp(t *testing.T) {
	var lg *rdqlog.Logger // nil
	// Must not panic.
	lg.Transition(context.Background(), rdqlog.TransitionClaimed, envelope.Envelope{ID: "x", Queue: "q"})
}

func TestParseTraceparent(t *testing.T) {
	tests := []struct {
		name   string
		tp     string
		tid    string
		sid    string
		wantOK bool
	}{
		{"valid", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", true},
		{"future version parses", "cc-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00", "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", true},
		{"all-zero trace id", "00-00000000000000000000000000000000-00f067aa0ba902b7-01", "", "", false},
		{"all-zero span id", "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", "", "", false},
		{"too few parts", "00-4bf92f3577b34da6a3ce929d0e0e4736-01", "", "", false},
		{"non-hex", "00-ZZf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "", "", false},
		{"wrong length", "00-4bf9-00f067aa0ba902b7-01", "", "", false},
		{"empty", "", "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tid, sid, ok := rdqlog.ParseTraceparent(tc.tp)
			if ok != tc.wantOK || tid != tc.tid || sid != tc.sid {
				t.Errorf("ParseTraceparent(%q) = (%q,%q,%v), want (%q,%q,%v)",
					tc.tp, tid, sid, ok, tc.tid, tc.sid, tc.wantOK)
			}
		})
	}
}

func TestPayloadAttrs_EmptyPayload(t *testing.T) {
	attrs := rdqlog.PayloadAttrs(nil)
	if len(attrs) != 1 {
		t.Fatalf("want 1 attr for empty payload (size only), got %d", len(attrs))
	}
	if attrs[0].Key != rdqlog.KeyPayloadBytes {
		t.Errorf("attr key = %q, want %q", attrs[0].Key, rdqlog.KeyPayloadBytes)
	}
}
