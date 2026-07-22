// SPDX-License-Identifier: Apache-2.0

package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// decodeProblem asserts the response is a well-formed problem+json body and
// returns it decoded.
func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder) Problem {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != ProblemContentType {
		t.Fatalf("Content-Type = %q, want %q", ct, ProblemContentType)
	}
	var p Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("body is not valid JSON: %v (body=%q)", err, rec.Body.String())
	}
	return p
}

// TestProblemStatusMatchesRegistry pins every stable code to its status and to a
// self-consistent body — the machine contract clients depend on.
func TestProblemStatusMatchesRegistry(t *testing.T) {
	for code, def := range problemDefs {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/some/path", nil)
		Error(rec, r, code)

		if rec.Code != def.status {
			t.Errorf("%s: HTTP status = %d, want %d", code, rec.Code, def.status)
		}
		p := decodeProblem(t, rec)
		if p.Code != code {
			t.Errorf("%s: body code = %q, want %q", code, p.Code, code)
		}
		if p.Status != def.status {
			t.Errorf("%s: body status = %d, want %d", code, p.Status, def.status)
		}
		if p.Title != def.title {
			t.Errorf("%s: body title = %q, want %q", code, p.Title, def.title)
		}
		if want := problemTypeBase + string(code); p.Type != want {
			t.Errorf("%s: body type = %q, want %q", code, p.Type, want)
		}
		if p.Instance != "/some/path" {
			t.Errorf("%s: body instance = %q, want /some/path", code, p.Instance)
		}
	}
}

// TestUnknownCodeFallsBackToInternal keeps a miswired handler from emitting an
// invalid body.
func TestUnknownCodeFallsBackToInternal(t *testing.T) {
	p := NewProblem(ProblemCode("NOPE_NOT_REAL"), "/x")
	if p.Code != CodeInternal || p.Status != http.StatusInternalServerError {
		t.Fatalf("unknown code fallback = (%s,%d), want (INTERNAL,500)", p.Code, p.Status)
	}
}

// TestRetryableStatusesAlwaysCarryRetryAfter is the T5.1 acceptance invariant:
// 429 and 503 responses always carry a Retry-After header, even when the handler
// supplies no hint.
func TestRetryableStatusesAlwaysCarryRetryAfter(t *testing.T) {
	for _, code := range []ProblemCode{CodeRateLimited, CodeStorageUnavailable} {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/v1/queues/q/tasks", nil)
		Error(rec, r, code) // no WithRetryAfter

		if got := rec.Header().Get("Retry-After"); got != "1" {
			t.Errorf("%s: default Retry-After = %q, want \"1\"", code, got)
		}
	}
}

// TestRetryAfterRoundsUp checks whole-second granularity with an always-positive
// floor: a sub-second hint never rounds down to zero.
func TestRetryAfterRoundsUp(t *testing.T) {
	cases := map[time.Duration]string{
		500 * time.Millisecond: "1",
		1 * time.Second:        "1",
		1500 * time.Millisecond: "2",
		30 * time.Second:       "30",
	}
	for d, want := range cases {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/x", nil)
		Error(rec, r, CodeRateLimited, WithRetryAfter(d))
		if got := rec.Header().Get("Retry-After"); got != want {
			t.Errorf("RetryAfter(%s) = %q, want %q", d, got, want)
		}
	}
}

// TestNonRetryableHasNoRetryAfter guards against leaking the header onto codes
// that are not retryable.
func TestNonRetryableHasNoRetryAfter(t *testing.T) {
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	Error(rec, r, CodeQueueNotFound)
	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Errorf("QUEUE_NOT_FOUND Retry-After = %q, want empty", got)
	}
}
