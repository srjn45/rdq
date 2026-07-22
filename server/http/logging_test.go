// SPDX-License-Identifier: Apache-2.0

package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	rdqlog "github.com/srjn45/rdq/core/log"
)

const testTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// TestSubmit_PropagatesInboundTraceparent proves the server end of submit →
// retry → handler: the inbound HTTP `traceparent` header is stamped into the
// stored task's headers so the trace context survives with the task.
func TestSubmit_PropagatesInboundTraceparent(t *testing.T) {
	st := newFakeStorage()
	s := newTestServer(t, st)

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(validSubmit("q")); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/queues/q/tasks", &buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(rdqlog.HeaderTraceparent, testTraceparent)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202\nbody: %s", rec.Code, rec.Body)
	}
	env := decodeEnvelope(t, rec)
	if got := env.Headers[rdqlog.HeaderTraceparent]; got != testTraceparent {
		t.Errorf("stored traceparent = %q, want %q", got, testTraceparent)
	}
}

// A client-supplied traceparent in the body headers wins over the HTTP header.
func TestSubmit_ClientTraceparentWins(t *testing.T) {
	st := newFakeStorage()
	s := newTestServer(t, st)

	const bodyTP = "00-11111111111111111111111111111111-2222222222222222-01"
	body := validSubmit("q")
	body.Headers = map[string]string{rdqlog.HeaderTraceparent: bodyTP}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/queues/q/tasks", &buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(rdqlog.HeaderTraceparent, testTraceparent)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	env := decodeEnvelope(t, rec)
	if got := env.Headers[rdqlog.HeaderTraceparent]; got != bodyTP {
		t.Errorf("stored traceparent = %q, want client value %q", got, bodyTP)
	}
}

// A malformed inbound traceparent is ignored, not stored.
func TestSubmit_IgnoresMalformedTraceparent(t *testing.T) {
	st := newFakeStorage()
	s := newTestServer(t, st)

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(validSubmit("q")); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/queues/q/tasks", &buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(rdqlog.HeaderTraceparent, "not-a-traceparent")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	env := decodeEnvelope(t, rec)
	if _, ok := env.Headers[rdqlog.HeaderTraceparent]; ok {
		t.Errorf("malformed traceparent should not be stored, got %q", env.Headers[rdqlog.HeaderTraceparent])
	}
}

// TestRequestLogging_EmitsTraceIDNoPayload proves the access log records the
// request with its trace_id and never contains the request payload (FR-25).
func TestRequestLogging_EmitsTraceIDNoPayload(t *testing.T) {
	st := newFakeStorage()
	var sink bytes.Buffer
	s := New(WithStorage(st), WithLogger(rdqlog.New(&sink)))

	body := validSubmit("q")
	body.Payload = []byte("super-secret-body")

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/queues/q/tasks", &buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(rdqlog.HeaderTraceparent, testTraceparent)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	out := sink.String()
	if !strings.Contains(out, "http.request") {
		t.Fatalf("no access log record:\n%s", out)
	}
	if !strings.Contains(out, "4bf92f3577b34da6a3ce929d0e0e4736") {
		t.Errorf("access log missing trace_id:\n%s", out)
	}
	// The base64 of "super-secret-body" is the payload wire form; neither the raw
	// text nor its base64 may appear in the access log.
	if strings.Contains(out, "super-secret-body") || strings.Contains(out, "c3VwZXItc2VjcmV0LWJvZHk") {
		t.Fatalf("access log leaked payload:\n%s", out)
	}
}

// A nil logger still propagates trace context and serves requests normally.
func TestRequestLogging_NilLoggerServes(t *testing.T) {
	st := newFakeStorage()
	s := New(WithStorage(st)) // no logger
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/tasks", validSubmit("q"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202\nbody: %s", rec.Code, rec.Body)
	}
}
