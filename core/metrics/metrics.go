// SPDX-License-Identifier: Apache-2.0

// Package metrics defines the Prometheus instruments for rdq: counters and
// histograms the engine pushes to on each event, plus a StatsCollector that
// pulls DLQ-depth and oldest-pending-age from spi.Storage at scrape time
// (the two flagship alert signals from PRD FR-22).
package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/srjn45/rdq/core/spi"
)

const namespace = "rdq"

// Recorder is the event sink the engine writes to. Registry implements it;
// Noop silently discards every observation.
type Recorder interface {
	RecordClaimLatency(queue string, d time.Duration)
	RecordHandlerDuration(queue string, d time.Duration, success bool)
	IncrRetry(queue string)
	IncrSuccessAfterRetry(queue string)
	IncrDLQArrival(queue string)
}

// Registry holds all rdq Prometheus instruments, labelled by queue.
// Construct with New; obtain the underlying prometheus.Registry via
// PrometheusRegistry for use with promhttp.HandlerFor.
type Registry struct {
	reg *prometheus.Registry

	retryTotal        *prometheus.CounterVec
	successAfterRetry *prometheus.CounterVec
	dlqArrivalsTotal  *prometheus.CounterVec
	claimLatency      *prometheus.HistogramVec
	handlerDuration   *prometheus.HistogramVec
}

// New creates a Registry with all instruments pre-registered on a fresh
// prometheus.Registry. queues pre-initialises per-queue label sets so
// metrics appear from the first scrape even before any events fire.
// store drives the embedded StatsCollector that reports DLQ-depth and
// oldest-pending-age; pass nil to omit those gauges (e.g. in unit tests
// that only check counters/histograms).
func New(store spi.Storage, queues []string) (*Registry, error) {
	reg := prometheus.NewRegistry()
	r := &Registry{reg: reg}

	r.retryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "task_retries_total",
		Help:      "Total task retries (reschedule calls), by queue.",
	}, []string{"queue"})

	r.successAfterRetry = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "task_success_after_retry_total",
		Help:      "Tasks that succeeded after at least one prior failure, by queue.",
	}, []string{"queue"})

	r.dlqArrivalsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "task_dlq_arrivals_total",
		Help:      "Tasks moved to the dead-letter queue, by queue.",
	}, []string{"queue"})

	r.claimLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "claim_latency_seconds",
		Help:      "ClaimDue round-trip latency in seconds, by queue.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"queue"})

	r.handlerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "handler_duration_seconds",
		Help:      "Handler execution duration in seconds, by queue and outcome.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"queue", "outcome"})

	for _, c := range []prometheus.Collector{
		r.retryTotal,
		r.successAfterRetry,
		r.dlqArrivalsTotal,
		r.claimLatency,
		r.handlerDuration,
	} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}

	if store != nil {
		if err := reg.Register(&statsCollector{store: store, queues: queues}); err != nil {
			return nil, err
		}
	}

	// Pre-initialise label sets so queues appear on the first scrape.
	for _, q := range queues {
		r.retryTotal.WithLabelValues(q)
		r.successAfterRetry.WithLabelValues(q)
		r.dlqArrivalsTotal.WithLabelValues(q)
		r.claimLatency.WithLabelValues(q)
		r.handlerDuration.WithLabelValues(q, "success")
		r.handlerDuration.WithLabelValues(q, "failure")
	}

	return r, nil
}

// PrometheusRegistry returns the underlying *prometheus.Registry for use
// with promhttp.HandlerFor.
func (r *Registry) PrometheusRegistry() *prometheus.Registry { return r.reg }

// RecordClaimLatency observes the latency of one ClaimDue call.
func (r *Registry) RecordClaimLatency(queue string, d time.Duration) {
	r.claimLatency.WithLabelValues(queue).Observe(d.Seconds())
}

// RecordHandlerDuration observes a handler execution time; success=true on
// nil error.
func (r *Registry) RecordHandlerDuration(queue string, d time.Duration, success bool) {
	outcome := "success"
	if !success {
		outcome = "failure"
	}
	r.handlerDuration.WithLabelValues(queue, outcome).Observe(d.Seconds())
}

// IncrRetry increments the retry counter (task rescheduled after failure).
func (r *Registry) IncrRetry(queue string) {
	r.retryTotal.WithLabelValues(queue).Inc()
}

// IncrSuccessAfterRetry increments the success-after-retry counter (task
// completed after ≥1 prior failure).
func (r *Registry) IncrSuccessAfterRetry(queue string) {
	r.successAfterRetry.WithLabelValues(queue).Inc()
}

// IncrDLQArrival increments the DLQ-arrival counter.
func (r *Registry) IncrDLQArrival(queue string) {
	r.dlqArrivalsTotal.WithLabelValues(queue).Inc()
}

// Noop is a silent no-op Recorder — useful when metrics are not configured.
type Noop struct{}

func (Noop) RecordClaimLatency(_ string, _ time.Duration)            {}
func (Noop) RecordHandlerDuration(_ string, _ time.Duration, _ bool) {}
func (Noop) IncrRetry(_ string)                                      {}
func (Noop) IncrSuccessAfterRetry(_ string)                          {}
func (Noop) IncrDLQArrival(_ string)                                 {}

// statsCollector implements prometheus.Collector and pulls spi.Stats from
// the storage backend at Prometheus scrape time. This is the canonical source
// for the two flagship alert metrics: DLQ depth and oldest-pending-age.
type statsCollector struct {
	store  spi.Storage
	queues []string
}

var (
	dlqDepthDesc = prometheus.NewDesc(
		namespace+"_dlq_depth",
		"Number of tasks currently in the dead-letter queue, by queue.",
		[]string{"queue"}, nil,
	)
	oldestPendingAgeDesc = prometheus.NewDesc(
		namespace+"_oldest_pending_age_seconds",
		"Age in seconds of the oldest pending task (0 when queue is empty), by queue.",
		[]string{"queue"}, nil,
	)
)

func (c *statsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dlqDepthDesc
	ch <- oldestPendingAgeDesc
}

func (c *statsCollector) Collect(ch chan<- prometheus.Metric) {
	ctx := context.Background()
	for _, q := range c.queues {
		s, err := c.store.Stats(ctx, q)
		if err != nil {
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			dlqDepthDesc, prometheus.GaugeValue, float64(s.DLQDepth), q,
		)
		ch <- prometheus.MustNewConstMetric(
			oldestPendingAgeDesc, prometheus.GaugeValue, s.OldestPendingAge.Seconds(), q,
		)
	}
}
