// SPDX-License-Identifier: Apache-2.0

package metrics_test

import (
	"context"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/memstore"
	"github.com/srjn45/rdq/core/metrics"
	"github.com/srjn45/rdq/core/spi"
)

func TestNewRegistersAllInstruments(t *testing.T) {
	r, err := metrics.New(nil, []string{"q"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	mfs, err := r.PrometheusRegistry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	want := map[string]bool{
		"rdq_task_retries_total":             false,
		"rdq_task_success_after_retry_total": false,
		"rdq_task_dlq_arrivals_total":        false,
		"rdq_claim_latency_seconds":          false,
		"rdq_handler_duration_seconds":       false,
	}
	for _, mf := range mfs {
		want[mf.GetName()] = true
	}
	for name, found := range want {
		if !found {
			t.Errorf("metric %q not registered", name)
		}
	}
}

func TestCounterIncrements(t *testing.T) {
	r, err := metrics.New(nil, []string{"orders"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	r.IncrRetry("orders")
	r.IncrRetry("orders")
	r.IncrSuccessAfterRetry("orders")
	r.IncrDLQArrival("orders")

	mfs := gatherMap(t, r)

	if v := counterValue(mfs, "rdq_task_retries_total", "orders"); v != 2 {
		t.Errorf("retries = %v, want 2", v)
	}
	if v := counterValue(mfs, "rdq_task_success_after_retry_total", "orders"); v != 1 {
		t.Errorf("success_after_retry = %v, want 1", v)
	}
	if v := counterValue(mfs, "rdq_task_dlq_arrivals_total", "orders"); v != 1 {
		t.Errorf("dlq_arrivals = %v, want 1", v)
	}
}

func TestHistogramObservations(t *testing.T) {
	r, err := metrics.New(nil, []string{"jobs"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	r.RecordClaimLatency("jobs", 5*time.Millisecond)
	r.RecordHandlerDuration("jobs", 100*time.Millisecond, true)
	r.RecordHandlerDuration("jobs", 200*time.Millisecond, false)

	mfs := gatherMap(t, r)

	if sc := histogramSampleCount(mfs, "rdq_claim_latency_seconds", "jobs"); sc != 1 {
		t.Errorf("claim_latency count = %d, want 1", sc)
	}
	// Two handler observations (one success, one failure).
	totalHandler := histogramSampleCountByLabel(mfs, "rdq_handler_duration_seconds", "jobs", "success") +
		histogramSampleCountByLabel(mfs, "rdq_handler_duration_seconds", "jobs", "failure")
	if totalHandler != 2 {
		t.Errorf("handler_duration total count = %d, want 2", totalHandler)
	}
}

func TestStatsCollectorDLQDepthAndOldestAge(t *testing.T) {
	store := memstore.New()
	queues := []string{"payments"}

	// Enqueue a task and dead-letter it so DLQ depth becomes 1.
	due := time.Now().Add(-time.Second)
	task := envelope.Envelope{
		ID:            "task-1",
		Queue:         "payments",
		Status:        envelope.StatusPending,
		NextAttemptAt: &due,
	}
	if err := store.Enqueue(context.Background(), task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := store.ClaimDue(context.Background(), "payments", 1, time.Minute)
	if err != nil || len(claimed) == 0 {
		t.Fatalf("ClaimDue: %v / %d", err, len(claimed))
	}
	att := spi.Attempt{
		AttemptNo: 1,
		StartedAt: time.Now(),
		Outcome:   envelope.OutcomePermanentFailure,
	}
	if err := store.DeadLetter(context.Background(), claimed[0].Task.ID, claimed[0].Token, att); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	r, err := metrics.New(store, queues)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	mfs := gatherMap(t, r)

	if _, ok := mfs["rdq_dlq_depth"]; !ok {
		t.Fatal("rdq_dlq_depth metric missing")
	}
	if v := gaugeValue(mfs, "rdq_dlq_depth", "payments"); v != 1 {
		t.Errorf("dlq_depth = %v, want 1", v)
	}
	// oldest_pending_age may be 0 (no pending tasks remain after dead-letter).
	if _, ok := mfs["rdq_oldest_pending_age_seconds"]; !ok {
		t.Fatal("rdq_oldest_pending_age_seconds metric missing")
	}
}

func TestNoopRecorder(t *testing.T) {
	var n metrics.Noop
	// Must not panic.
	n.RecordClaimLatency("q", time.Second)
	n.RecordHandlerDuration("q", time.Second, true)
	n.IncrRetry("q")
	n.IncrSuccessAfterRetry("q")
	n.IncrDLQArrival("q")
}

// --- helpers ----------------------------------------------------------------

func gatherMap(t *testing.T, r *metrics.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	mfs, err := r.PrometheusRegistry().Gather()
	if err != nil && !strings.Contains(err.Error(), "no metrics") {
		t.Fatalf("Gather: %v", err)
	}
	m := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		m[mf.GetName()] = mf
	}
	return m
}

func counterValue(mfs map[string]*dto.MetricFamily, name, queue string) float64 {
	mf, ok := mfs[name]
	if !ok {
		return 0
	}
	for _, m := range mf.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "queue" && lp.GetValue() == queue {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func gaugeValue(mfs map[string]*dto.MetricFamily, name, queue string) float64 {
	mf, ok := mfs[name]
	if !ok {
		return -1
	}
	for _, m := range mf.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "queue" && lp.GetValue() == queue {
				return m.GetGauge().GetValue()
			}
		}
	}
	return -1
}

func histogramSampleCount(mfs map[string]*dto.MetricFamily, name, queue string) uint64 {
	return histogramSampleCountByLabel(mfs, name, queue, "")
}

func histogramSampleCountByLabel(mfs map[string]*dto.MetricFamily, name, queue, outcome string) uint64 {
	mf, ok := mfs[name]
	if !ok {
		return 0
	}
	for _, m := range mf.GetMetric() {
		hasQueue, hasOutcome := false, outcome == ""
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "queue" && lp.GetValue() == queue {
				hasQueue = true
			}
			if lp.GetName() == "outcome" && lp.GetValue() == outcome {
				hasOutcome = true
			}
		}
		if hasQueue && hasOutcome {
			return m.GetHistogram().GetSampleCount()
		}
	}
	return 0
}
