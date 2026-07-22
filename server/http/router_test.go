// SPDX-License-Identifier: Apache-2.0

package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// do routes a request through a freshly built server and returns the recorder.
func do(t *testing.T, s *Server, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

// TestHealthzLiveness: /healthz is 200 ok and needs no auth.
func TestHealthzLiveness(t *testing.T) {
	rec := do(t, New(), http.MethodGet, "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("healthz body invalid: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("healthz status field = %q, want ok", body["status"])
	}
}

// TestReadyzReflectsStorageReachability: readyz is ready when the probe passes
// and 503 STORAGE_UNAVAILABLE (with Retry-After) when it fails (design 04 §8).
func TestReadyzReflectsStorageReachability(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		s := New(WithReadinessProbe("storage", ProbeFunc(func(context.Context) error { return nil })))
		rec := do(t, s, http.MethodGet, "/readyz")
		if rec.Code != http.StatusOK {
			t.Fatalf("readyz status = %d, want 200", rec.Code)
		}
		var rr readyReport
		if err := json.Unmarshal(rec.Body.Bytes(), &rr); err != nil {
			t.Fatalf("readyz body invalid: %v", err)
		}
		if rr.Status != "ready" || rr.Checks["storage"] != "ok" {
			t.Errorf("readyz report = %+v, want ready/storage:ok", rr)
		}
	})

	t.Run("storage down", func(t *testing.T) {
		s := New(WithReadinessProbe("storage", ProbeFunc(func(context.Context) error {
			return errors.New("dial tcp: connection refused")
		})))
		rec := do(t, s, http.MethodGet, "/readyz")
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("readyz status = %d, want 503", rec.Code)
		}
		if rec.Header().Get("Retry-After") == "" {
			t.Error("readyz 503 missing Retry-After")
		}
		p := decodeProblem(t, rec)
		if p.Code != CodeStorageUnavailable {
			t.Errorf("readyz code = %q, want STORAGE_UNAVAILABLE", p.Code)
		}
	})
}

// TestReadyzNoProbesIsReady: with no dependencies registered, readiness trivially
// holds.
func TestReadyzNoProbesIsReady(t *testing.T) {
	rec := do(t, New(), http.MethodGet, "/readyz")
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz (no probes) status = %d, want 200", rec.Code)
	}
}

// TestHealthEndpointsRejectNonGET: even the unauthenticated health endpoints
// speak the problem+json contract, returning 405 with an Allow header.
func TestHealthEndpointsRejectNonGET(t *testing.T) {
	for _, path := range []string{"/healthz", "/readyz"} {
		rec := do(t, New(), http.MethodPost, path)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s POST status = %d, want 405", path, rec.Code)
		}
		if got := rec.Header().Get("Allow"); got != http.MethodGet {
			t.Errorf("%s Allow = %q, want GET", path, got)
		}
		if p := decodeProblem(t, rec); p.Code != CodeMethodNotAllowed {
			t.Errorf("%s code = %q, want METHOD_NOT_ALLOWED", path, p.Code)
		}
	}
}

// TestUnknownRoutesAreProblemJSON404: both the top-level catch-all and unknown
// paths under /v1 return a problem+json 404, not the net/http plaintext default.
func TestUnknownRoutesAreProblemJSON404(t *testing.T) {
	for _, path := range []string{"/nope", "/v1/", "/v1/anything"} {
		rec := do(t, New(), http.MethodGet, path)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s status = %d, want 404", path, rec.Code)
		}
		if p := decodeProblem(t, rec); p.Code != CodeNotFound {
			t.Errorf("%s code = %q, want NOT_FOUND", path, p.Code)
		}
	}
}

// TestDataPlaneRoutesRejectWrongMethod: data-plane routes are mounted; wrong
// methods return 405 METHOD_NOT_ALLOWED (not 404) with an Allow header.
func TestDataPlaneRoutesRejectWrongMethod(t *testing.T) {
	cases := []struct {
		method, path string
		allow        string
	}{
		{http.MethodGet, "/v1/queues/q/tasks", http.MethodPost},
		{http.MethodGet, "/v1/queues/q/tasks:batch", http.MethodPost},
		{http.MethodPost, "/v1/tasks/01ARZ3NDEKTSV4RRFFQ69G5FAV", http.MethodGet},
	}
	for _, tc := range cases {
		rec := do(t, New(), tc.method, tc.path)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: status = %d, want 405", tc.method, tc.path, rec.Code)
		}
		if got := rec.Header().Get("Allow"); got != tc.allow {
			t.Errorf("%s %s: Allow = %q, want %q", tc.method, tc.path, got, tc.allow)
		}
		if p := decodeProblem(t, rec); p.Code != CodeMethodNotAllowed {
			t.Errorf("%s %s: code = %q, want METHOD_NOT_ALLOWED", tc.method, tc.path, p.Code)
		}
	}
}

// TestHealthIsOutsideV1: the health endpoints are mounted at the root, not under
// /v1 — so probes never cross the auth boundary (G11).
func TestHealthIsOutsideV1(t *testing.T) {
	for _, path := range []string{"/v1/healthz", "/v1/readyz"} {
		rec := do(t, New(), http.MethodGet, path)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s should not exist under /v1 (status %d)", path, rec.Code)
		}
	}
}
