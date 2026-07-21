// SPDX-License-Identifier: Apache-2.0

package http

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/memstore"
	"github.com/srjn45/rdq/core/metrics"
	"github.com/srjn45/rdq/core/spi"
)

// TestMetricsEndpointNotConfigured: /metrics returns 404 when no handler is wired.
func TestMetricsEndpointNotConfigured(t *testing.T) {
	rec := do(t, New(), http.MethodGet, "/metrics")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("/metrics (unconfigured) status = %d, want 404", rec.Code)
	}
	p := decodeProblem(t, rec)
	if p.Code != CodeNotFound {
		t.Errorf("code = %q, want NOT_FOUND", p.Code)
	}
}

// TestMetricsEndpointServesPrometheus: /metrics returns 200 with text/plain
// prometheus output when a registry is wired.
func TestMetricsEndpointServesPrometheus(t *testing.T) {
	store := memstore.New()
	queues := []string{"emails"}

	// Put a task in the DLQ so rdq_dlq_depth is non-zero.
	due := time.Now().Add(-time.Second)
	task := envelope.Envelope{ID: "m1", Queue: "emails", Status: envelope.StatusPending, NextAttemptAt: &due}
	if err := store.Enqueue(context.Background(), task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := store.ClaimDue(context.Background(), "emails", 1, time.Minute)
	if err != nil || len(claimed) == 0 {
		t.Fatalf("ClaimDue: %v/%d", err, len(claimed))
	}
	att := spi.Attempt{
		AttemptNo: 1,
		StartedAt: time.Now(),
		Outcome:   envelope.OutcomePermanentFailure,
	}
	if err := store.DeadLetter(context.Background(), claimed[0].Task.ID, claimed[0].Token, att); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	reg, err := metrics.New(store, queues)
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	reg.IncrRetry("emails")

	h := promhttp.HandlerFor(reg.PrometheusRegistry(), promhttp.HandlerOpts{})
	s := New(WithMetricsHandler(h))
	rec := do(t, s, http.MethodGet, "/metrics")

	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}

	body := rec.Body.String()
	for _, want := range []string{
		"rdq_dlq_depth",
		"rdq_oldest_pending_age_seconds",
		"rdq_task_retries_total",
		"rdq_task_dlq_arrivals_total",
		"rdq_task_success_after_retry_total",
		"rdq_claim_latency_seconds",
		"rdq_handler_duration_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics body missing %q", want)
		}
	}
}

// TestMetricsIsOutsideV1: /metrics must not appear under /v1 (unauthenticated scrape).
func TestMetricsIsOutsideV1(t *testing.T) {
	rec := do(t, New(), http.MethodGet, "/v1/metrics")
	if rec.Code != http.StatusNotFound {
		t.Errorf("/v1/metrics should not exist (status %d)", rec.Code)
	}
}

// TestMetricsRejectsNonGET: /metrics enforces GET-only via the same pattern as
// health endpoints.
func TestMetricsRejectsNonGET(t *testing.T) {
	h := promhttp.HandlerFor(mustRegistry(t), promhttp.HandlerOpts{})
	s := New(WithMetricsHandler(h))
	rec := do(t, s, http.MethodPost, "/metrics")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("/metrics POST status = %d, want 405", rec.Code)
	}
	if got := rec.Header().Get("Allow"); got != http.MethodGet {
		t.Errorf("Allow = %q, want GET", got)
	}
}

// TestDLQDepthAndOldestAgePresent: the two flagship alert metrics must be
// present and populated from Stats (acceptance criterion).
func TestDLQDepthAndOldestAgePresent(t *testing.T) {
	store := memstore.New()
	queues := []string{"alerts"}

	// Enqueue + dead-letter a task so DLQ depth = 1.
	due2 := time.Now().Add(-time.Second)
	task := envelope.Envelope{ID: "a1", Queue: "alerts", Status: envelope.StatusPending, NextAttemptAt: &due2}
	if err := store.Enqueue(context.Background(), task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := store.ClaimDue(context.Background(), "alerts", 1, time.Minute)
	if err != nil || len(claimed) == 0 {
		t.Fatalf("ClaimDue: %v/%d", err, len(claimed))
	}
	if err := store.DeadLetter(context.Background(), claimed[0].Task.ID, claimed[0].Token, spi.Attempt{
		AttemptNo: 1, StartedAt: time.Now(), Outcome: envelope.OutcomePermanentFailure,
	}); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	reg, err := metrics.New(store, queues)
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	h := promhttp.HandlerFor(reg.PrometheusRegistry(), promhttp.HandlerOpts{})
	s := New(WithMetricsHandler(h))
	body := do(t, s, http.MethodGet, "/metrics").Body.String()

	for _, want := range []string{"rdq_dlq_depth", "rdq_oldest_pending_age_seconds"} {
		if !strings.Contains(body, want) {
			t.Errorf("flagship metric %q missing from /metrics output", want)
		}
	}
	// DLQ depth must be 1.
	if !strings.Contains(body, `rdq_dlq_depth{queue="alerts"} 1`) {
		t.Errorf("rdq_dlq_depth for alerts should be 1; body:\n%s", body)
	}
}

func mustRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	r, err := metrics.New(nil, nil)
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	return r.PrometheusRegistry()
}
