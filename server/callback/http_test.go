// SPDX-License-Identifier: Apache-2.0

package callback

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/config"
	"github.com/srjn45/rdq/core/envelope"
)

// sampleTask is a representative delivery used across the response-status tests.
func sampleTask() Task {
	return Task{
		ID:         "01J2ZK7Q",
		Queue:      "payments.charge",
		HandlerRef: "charge-payment",
		Attempt:    3,
		Payload:    []byte(`{"amount":100,"currency":"USD"}`),
	}
}

func TestDispatch2xxSuccessDeliversPayloadAndHeaders(t *testing.T) {
	task := sampleTask()

	var gotBody []byte
	var gotHeader http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotHeader = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	res, err := New().Dispatch(context.Background(), Target{URL: srv.URL, ContentType: "application/json"}, task)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if !res.Success() || res.Outcome != envelope.OutcomeSuccess {
		t.Fatalf("2xx: got outcome %q success=%v, want SUCCESS", res.Outcome, res.Success())
	}
	if res.Error != nil {
		t.Errorf("2xx should carry no error, got %+v", res.Error)
	}
	if res.Status != http.StatusOK {
		t.Errorf("status = %d, want 200", res.Status)
	}

	// Payload delivered byte-for-byte, no re-encoding.
	if string(gotBody) != string(task.Payload) {
		t.Errorf("body = %q, want %q (verbatim)", gotBody, task.Payload)
	}
	// X-RDQ-* metadata rides in headers.
	checks := map[string]string{
		HeaderTaskID:     task.ID,
		HeaderQueue:      task.Queue,
		HeaderHandlerRef: task.HandlerRef,
		HeaderAttempt:    "3",
		"Content-Type":   "application/json",
	}
	for h, want := range checks {
		if got := gotHeader.Get(h); got != want {
			t.Errorf("header %s = %q, want %q", h, got, want)
		}
	}
}

// TestDispatchHMACVerifiableByReceiver signs a request and has the stub receiver
// independently verify it with Verify — the HMAC round-trip the acceptance
// criteria call for.
func TestDispatchHMACVerifiableByReceiver(t *testing.T) {
	task := sampleTask()
	secret := []byte("webhook-signing-key")

	var verifyErr error
	var signatureSeen string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		signatureSeen = r.Header.Get(SignatureHeader)
		_, verifyErr = Verify(secret, body, signatureSeen)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	target := Target{URL: srv.URL, ContentType: "application/json", Auth: AuthHMAC, Secret: secret}
	if _, err := New().Dispatch(context.Background(), target, task); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if signatureSeen == "" {
		t.Fatal("no X-RDQ-Signature header sent")
	}
	if verifyErr != nil {
		t.Fatalf("receiver could not verify signature: %v", verifyErr)
	}
	// A signature made with a different secret must not verify — guards against a
	// vacuous Verify that accepts anything.
	if _, err := Verify([]byte("attacker"), task.Payload, signatureSeen); err == nil {
		t.Error("signature verified under the wrong secret")
	}
}

func TestDispatchStatusClassification(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		want    envelope.Outcome
		errType string
	}{
		{"2xx ack", http.StatusOK, envelope.OutcomeSuccess, ""},
		{"4xx terminal", http.StatusBadRequest, envelope.OutcomePermanentFailure, "HTTP_400"},
		{"404 terminal", http.StatusNotFound, envelope.OutcomePermanentFailure, "HTTP_404"},
		{"5xx retryable", http.StatusInternalServerError, envelope.OutcomeRetryableFailure, "HTTP_500"},
		{"503 retryable", http.StatusServiceUnavailable, envelope.OutcomeRetryableFailure, "HTTP_503"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			res, err := New().Dispatch(context.Background(), Target{URL: srv.URL}, sampleTask())
			if err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			if res.Outcome != tc.want {
				t.Errorf("status %d: outcome = %q, want %q", tc.status, res.Outcome, tc.want)
			}
			if res.Status != tc.status {
				t.Errorf("status field = %d, want %d", res.Status, tc.status)
			}
			if tc.errType == "" {
				if res.Error != nil {
					t.Errorf("expected no error, got %+v", res.Error)
				}
				return
			}
			if res.Error == nil {
				t.Fatalf("status %d: expected an error, got nil", tc.status)
			}
			if res.Error.Type != tc.errType {
				t.Errorf("error.type = %q, want %q", res.Error.Type, tc.errType)
			}
		})
	}
}

func TestDispatchCapturesResponseBodyIntoDetail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"reason":"downstream refused"}`)
	}))
	defer srv.Close()

	res, err := New().Dispatch(context.Background(), Target{URL: srv.URL}, sampleTask())
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if res.Error == nil || res.Error.Detail == nil {
		t.Fatalf("expected error.detail to be captured, got %+v", res.Error)
	}
	// A JSON body is stored verbatim as structured detail.
	var got map[string]string
	if err := json.Unmarshal(res.Error.Detail, &got); err != nil {
		t.Fatalf("detail is not valid JSON: %v (%s)", err, res.Error.Detail)
	}
	if got["reason"] != "downstream refused" {
		t.Errorf("detail.reason = %q, want %q", got["reason"], "downstream refused")
	}
}

// TestDispatchNonJSONBodyWrappedAsString confirms a plain-text failure body is
// stored as a JSON string, so the envelope stays valid JSON.
func TestDispatchNonJSONBodyWrappedAsString(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "not json, just a sentence")
	}))
	defer srv.Close()

	res, err := New().Dispatch(context.Background(), Target{URL: srv.URL}, sampleTask())
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if res.Error == nil || !json.Valid(res.Error.Detail) {
		t.Fatalf("detail must be valid JSON, got %+v", res.Error)
	}
	var s string
	if err := json.Unmarshal(res.Error.Detail, &s); err != nil {
		t.Fatalf("detail should be a JSON string: %v", err)
	}
	if s != "not json, just a sentence" {
		t.Errorf("detail = %q, want the body text", s)
	}
}

// TestDispatchTruncatesLargeBody caps captured detail at 4 KiB with the sentinel.
func TestDispatchTruncatesLargeBody(t *testing.T) {
	big := strings.Repeat("x", maxDetailBytes*2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, big)
	}))
	defer srv.Close()

	res, err := New().Dispatch(context.Background(), Target{URL: srv.URL}, sampleTask())
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	var s string
	if err := json.Unmarshal(res.Error.Detail, &s); err != nil {
		t.Fatalf("detail should be a JSON string: %v", err)
	}
	if !strings.HasSuffix(s, truncationMarker) {
		t.Errorf("truncated detail should end with %q", truncationMarker)
	}
	body := strings.TrimSuffix(s, truncationMarker)
	if len(body) != maxDetailBytes {
		t.Errorf("captured %d body bytes, want the %d-byte cap", len(body), maxDetailBytes)
	}
}

func TestDispatchTimeoutIsRetryableTIMEOUT(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	res, err := New().Dispatch(context.Background(), Target{URL: srv.URL, Timeout: 50 * time.Millisecond}, sampleTask())
	if err != nil {
		t.Fatalf("Dispatch returned a hard error, want a classified TIMEOUT: %v", err)
	}
	if res.Outcome != envelope.OutcomeRetryableFailure {
		t.Errorf("timeout outcome = %q, want RETRYABLE_FAILURE", res.Outcome)
	}
	if res.Status != 0 {
		t.Errorf("timeout status = %d, want 0 (no response)", res.Status)
	}
	if res.Error == nil || res.Error.Type != "TIMEOUT" {
		t.Fatalf("timeout error.type = %+v, want TIMEOUT", res.Error)
	}
}

// TestDispatchTransportErrorIsRetryable covers an unreachable receiver (connection
// refused): a transient TRANSPORT_ERROR, no response.
func TestDispatchTransportErrorIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // now nothing is listening on url.

	res, err := New().Dispatch(context.Background(), Target{URL: url, Timeout: time.Second}, sampleTask())
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if res.Outcome != envelope.OutcomeRetryableFailure {
		t.Errorf("transport error outcome = %q, want RETRYABLE_FAILURE", res.Outcome)
	}
	if res.Error == nil || res.Error.Type != "TRANSPORT_ERROR" {
		t.Fatalf("transport error.type = %+v, want TRANSPORT_ERROR", res.Error)
	}
}

// TestResponseMappingOverridesDefaults exercises the canonical mapping from
// design 03 §2: exact retryable codes (408, 429) win over the 4xx permanent
// class, while a plain 400 falls to that class, and an exact permanent 500 wins
// over the 5xx retryable class.
func TestResponseMappingOverridesDefaults(t *testing.T) {
	rm := &config.ResponseMapping{
		RetryableStatus: []config.StatusMatcher{{Code: 408}, {Code: 429}, {Class: "5xx"}},
		PermanentStatus: []config.StatusMatcher{{Class: "4xx"}, {Code: 500}},
	}
	cases := []struct {
		status int
		want   envelope.Outcome
	}{
		{http.StatusRequestTimeout, envelope.OutcomeRetryableFailure},      // 408 exact retryable beats 4xx
		{http.StatusTooManyRequests, envelope.OutcomeRetryableFailure},     // 429 exact retryable beats 4xx
		{http.StatusBadRequest, envelope.OutcomePermanentFailure},          // 400 -> 4xx permanent
		{http.StatusInternalServerError, envelope.OutcomePermanentFailure}, // 500 exact permanent beats 5xx
		{http.StatusServiceUnavailable, envelope.OutcomeRetryableFailure},  // 503 -> 5xx retryable
	}
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			res, err := New().Dispatch(context.Background(), Target{URL: srv.URL, ResponseMapping: rm}, sampleTask())
			if err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			if res.Outcome != tc.want {
				t.Errorf("status %d with mapping: outcome = %q, want %q", tc.status, res.Outcome, tc.want)
			}
		})
	}
}

func TestDispatchBearerAndHeaderAuth(t *testing.T) {
	t.Run("bearer", func(t *testing.T) {
		var got string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		target := Target{URL: srv.URL, Auth: AuthBearer, Secret: []byte("tok123")}
		if _, err := New().Dispatch(context.Background(), target, sampleTask()); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if got != "Bearer tok123" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer tok123")
		}
	})

	t.Run("header", func(t *testing.T) {
		var got string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Get("X-Api-Key")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		target := Target{URL: srv.URL, Auth: AuthHeader, HeaderName: "X-Api-Key", Secret: []byte("k3y")}
		if _, err := New().Dispatch(context.Background(), target, sampleTask()); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if got != "k3y" {
			t.Errorf("X-Api-Key = %q, want %q", got, "k3y")
		}
	})
}

// TestDispatchPropagatesTaskHeadersWithoutShadowing checks a propagated header
// (traceparent) reaches the receiver, and that a task cannot spoof an X-RDQ-*
// header the dispatcher controls.
func TestDispatchPropagatesTaskHeadersWithoutShadowing(t *testing.T) {
	task := sampleTask()
	task.Headers = map[string]string{
		"traceparent": "00-abc-def-01",
		HeaderTaskID:  "spoofed",
	}
	var hdr http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if _, err := New().Dispatch(context.Background(), Target{URL: srv.URL}, task); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := hdr.Get("traceparent"); got != "00-abc-def-01" {
		t.Errorf("traceparent = %q, want propagated value", got)
	}
	if got := hdr.Get(HeaderTaskID); got != task.ID {
		t.Errorf("X-RDQ-Task-Id = %q, want %q (task must not shadow it)", got, task.ID)
	}
}
