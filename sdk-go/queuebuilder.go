// SPDX-License-Identifier: Apache-2.0

package rdq

import (
	"time"

	"github.com/srjn45/rdq/core/config"
)

// QueueBuilder constructs a core/config.QueueConfig from code, as the
// code-builder alternative to a YAML file (design 03 §1). Start a chain with
// Queue and call Build to obtain the queue name and config:
//
//	name, cfg, err := rdq.Queue("payments.charge").
//	    MaxAttempts(8).
//	    InitialBackoff(2 * time.Second).
//	    SyncRetryAttempts(2).
//	    SyncRetryBackoff(100 * time.Millisecond).
//	    Build()
//
// The produced QueueConfig is structurally identical to loading the equivalent
// YAML block via LoadYAML + config.Resolved. One source per process — use
// either the builder or LoadYAML, not both (design 03 §1).
type QueueBuilder struct {
	name string
	qc   config.QueueConfig
}

// Queue starts a fluent QueueBuilder for name.
func Queue(name string) *QueueBuilder {
	return &QueueBuilder{name: name}
}

// Name returns the queue name supplied to Queue.
func (b *QueueBuilder) Name() string { return b.name }

// --- retry block ---

func (b *QueueBuilder) ensureRetry() *config.RetryConfig {
	if b.qc.Retry == nil {
		b.qc.Retry = &config.RetryConfig{}
	}
	return b.qc.Retry
}

// MaxAttempts sets retry.max_attempts.
func (b *QueueBuilder) MaxAttempts(n int) *QueueBuilder {
	b.ensureRetry().MaxAttempts = &n
	return b
}

// InitialBackoff sets retry.initial_backoff.
func (b *QueueBuilder) InitialBackoff(d time.Duration) *QueueBuilder {
	v := config.Duration(d)
	b.ensureRetry().InitialBackoff = &v
	return b
}

// BackoffMultiplier sets retry.backoff_multiplier.
func (b *QueueBuilder) BackoffMultiplier(f float64) *QueueBuilder {
	b.ensureRetry().BackoffMultiplier = &f
	return b
}

// MaxBackoff sets retry.max_backoff.
func (b *QueueBuilder) MaxBackoff(d time.Duration) *QueueBuilder {
	v := config.Duration(d)
	b.ensureRetry().MaxBackoff = &v
	return b
}

// Jitter sets retry.jitter (fraction of computed backoff, 0..1).
func (b *QueueBuilder) Jitter(f float64) *QueueBuilder {
	b.ensureRetry().Jitter = &f
	return b
}

// --- execution block ---

func (b *QueueBuilder) ensureExecution() *config.ExecutionConfig {
	if b.qc.Execution == nil {
		b.qc.Execution = &config.ExecutionConfig{}
	}
	return b.qc.Execution
}

// Lease sets execution.lease (visibility timeout).
func (b *QueueBuilder) Lease(d time.Duration) *QueueBuilder {
	v := config.Duration(d)
	b.ensureExecution().Lease = &v
	return b
}

// HandlerTimeout sets execution.handler_timeout (must be ≤ Lease).
func (b *QueueBuilder) HandlerTimeout(d time.Duration) *QueueBuilder {
	v := config.Duration(d)
	b.ensureExecution().HandlerTimeout = &v
	return b
}

// Heartbeat sets execution.heartbeat (extend lease while handler runs).
func (b *QueueBuilder) Heartbeat(enabled bool) *QueueBuilder {
	b.ensureExecution().Heartbeat = &enabled
	return b
}

// --- worker block ---

func (b *QueueBuilder) ensureWorker() *config.WorkerConfig {
	if b.qc.Worker == nil {
		b.qc.Worker = &config.WorkerConfig{}
	}
	return b.qc.Worker
}

// BatchSize sets worker.batch_size (ClaimDue limit).
func (b *QueueBuilder) BatchSize(n int) *QueueBuilder {
	b.ensureWorker().BatchSize = &n
	return b
}

// PollInterval sets worker.poll_interval.
func (b *QueueBuilder) PollInterval(d time.Duration) *QueueBuilder {
	v := config.Duration(d)
	b.ensureWorker().PollInterval = &v
	return b
}

// Concurrency sets worker.concurrency (parallel handler invocations per instance).
func (b *QueueBuilder) Concurrency(n int) *QueueBuilder {
	b.ensureWorker().Concurrency = &n
	return b
}

// --- sync_retry block ---

func (b *QueueBuilder) ensureSyncRetry() *config.SyncRetryConfig {
	if b.qc.SyncRetry == nil {
		b.qc.SyncRetry = &config.SyncRetryConfig{}
	}
	return b.qc.SyncRetry
}

// SyncRetryAttempts sets sync_retry.attempts: the number of in-process
// retries before falling through to durable enqueue (design 03 §2).
func (b *QueueBuilder) SyncRetryAttempts(n int) *QueueBuilder {
	b.ensureSyncRetry().Attempts = &n
	return b
}

// SyncRetryBackoff sets sync_retry.backoff: the sleep between in-process
// retry attempts.
func (b *QueueBuilder) SyncRetryBackoff(d time.Duration) *QueueBuilder {
	v := config.Duration(d)
	b.ensureSyncRetry().Backoff = &v
	return b
}

// Build returns the queue name and the QueueConfig assembled by the chain.
// It does not apply defaults merging (that is LoadYAML's responsibility via
// core/config.Load) and does not validate field-level constraints — callers
// that need strict validation should pipe the config through LoadYAML or run
// the worker / submit path, which validates at claim time.
func (b *QueueBuilder) Build() (string, *config.QueueConfig, error) {
	qc := b.qc
	return b.name, &qc, nil
}

// LoadYAML parses and validates a YAML queue-config document using
// core/config.Load, which enforces strict schema validation and deep-merges
// the defaults block into every queue. Use config.Config.Resolved to obtain
// the effective per-queue config after loading.
//
// This is the YAML alternative to QueueBuilder: teams that prefer
// config-as-files use LoadYAML; the output is structurally identical to what
// the builder produces (one source per process, design 03 §1).
func LoadYAML(data []byte) (*config.Config, error) {
	return config.Load(data)
}
