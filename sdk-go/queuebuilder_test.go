// SPDX-License-Identifier: Apache-2.0

package rdq_test

import (
	"encoding/json"
	"testing"
	"time"

	rdq "github.com/srjn45/rdq/sdk-go"
)

// marshalJSON serialises v to a JSON string for comparison. Panics on error
// since test data must always be serialisable.
func marshalJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return string(b)
}

// --- QueueBuilder ---

// TestQueueBuilder_Name verifies that Queue preserves the queue name.
func TestQueueBuilder_Name(t *testing.T) {
	b := rdq.Queue("payments.charge")
	if b.Name() != "payments.charge" {
		t.Fatalf("Name = %q, want %q", b.Name(), "payments.charge")
	}
}

// TestQueueBuilder_Build_RetryFields verifies that the retry block is
// populated correctly from the fluent chain.
func TestQueueBuilder_Build_RetryFields(t *testing.T) {
	name, cfg, err := rdq.Queue("test.retry").
		MaxAttempts(8).
		InitialBackoff(2 * time.Second).
		BackoffMultiplier(2.5).
		MaxBackoff(10 * time.Minute).
		Jitter(0.2).
		Build()

	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if name != "test.retry" {
		t.Fatalf("name = %q, want %q", name, "test.retry")
	}
	if cfg.Retry == nil {
		t.Fatal("Retry block is nil")
	}
	if *cfg.Retry.MaxAttempts != 8 {
		t.Fatalf("MaxAttempts = %d, want 8", *cfg.Retry.MaxAttempts)
	}
	if cfg.Retry.InitialBackoff.Std() != 2*time.Second {
		t.Fatalf("InitialBackoff = %v, want 2s", cfg.Retry.InitialBackoff.Std())
	}
	if *cfg.Retry.BackoffMultiplier != 2.5 {
		t.Fatalf("BackoffMultiplier = %g, want 2.5", *cfg.Retry.BackoffMultiplier)
	}
	if cfg.Retry.MaxBackoff.Std() != 10*time.Minute {
		t.Fatalf("MaxBackoff = %v, want 10m", cfg.Retry.MaxBackoff.Std())
	}
	if *cfg.Retry.Jitter != 0.2 {
		t.Fatalf("Jitter = %g, want 0.2", *cfg.Retry.Jitter)
	}
}

// TestQueueBuilder_Build_ExecutionFields verifies the execution block.
func TestQueueBuilder_Build_ExecutionFields(t *testing.T) {
	_, cfg, err := rdq.Queue("test.exec").
		Lease(60 * time.Second).
		HandlerTimeout(45 * time.Second).
		Heartbeat(true).
		Build()

	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if cfg.Execution == nil {
		t.Fatal("Execution block is nil")
	}
	if cfg.Execution.Lease.Std() != 60*time.Second {
		t.Fatalf("Lease = %v, want 60s", cfg.Execution.Lease.Std())
	}
	if cfg.Execution.HandlerTimeout.Std() != 45*time.Second {
		t.Fatalf("HandlerTimeout = %v, want 45s", cfg.Execution.HandlerTimeout.Std())
	}
	if !*cfg.Execution.Heartbeat {
		t.Fatal("Heartbeat = false, want true")
	}
}

// TestQueueBuilder_Build_WorkerFields verifies the worker block.
func TestQueueBuilder_Build_WorkerFields(t *testing.T) {
	_, cfg, err := rdq.Queue("test.worker").
		BatchSize(32).
		PollInterval(500 * time.Millisecond).
		Concurrency(8).
		Build()

	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if cfg.Worker == nil {
		t.Fatal("Worker block is nil")
	}
	if *cfg.Worker.BatchSize != 32 {
		t.Fatalf("BatchSize = %d, want 32", *cfg.Worker.BatchSize)
	}
	if cfg.Worker.PollInterval.Std() != 500*time.Millisecond {
		t.Fatalf("PollInterval = %v, want 500ms", cfg.Worker.PollInterval.Std())
	}
	if *cfg.Worker.Concurrency != 8 {
		t.Fatalf("Concurrency = %d, want 8", *cfg.Worker.Concurrency)
	}
}

// TestQueueBuilder_Build_SyncRetryFields verifies the sync_retry block.
func TestQueueBuilder_Build_SyncRetryFields(t *testing.T) {
	_, cfg, err := rdq.Queue("test.syncretry").
		SyncRetryAttempts(2).
		SyncRetryBackoff(100 * time.Millisecond).
		Build()

	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if cfg.SyncRetry == nil {
		t.Fatal("SyncRetry block is nil")
	}
	if *cfg.SyncRetry.Attempts != 2 {
		t.Fatalf("Attempts = %d, want 2", *cfg.SyncRetry.Attempts)
	}
	if cfg.SyncRetry.Backoff.Std() != 100*time.Millisecond {
		t.Fatalf("Backoff = %v, want 100ms", cfg.SyncRetry.Backoff.Std())
	}
}

// TestQueueBuilder_UnsetBlocksNil verifies that blocks not touched by the
// chain are nil in the produced QueueConfig (not zero-value structs), so they
// behave correctly in the defaults deep-merge.
func TestQueueBuilder_UnsetBlocksNil(t *testing.T) {
	_, cfg, err := rdq.Queue("test.partial").MaxAttempts(5).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if cfg.Execution != nil {
		t.Fatal("Execution should be nil when not configured")
	}
	if cfg.Worker != nil {
		t.Fatal("Worker should be nil when not configured")
	}
	if cfg.SyncRetry != nil {
		t.Fatal("SyncRetry should be nil when not configured")
	}
}

// --- LoadYAML ---

// TestLoadYAML_Valid verifies that a well-formed YAML document is parsed
// without error and exposes the correct queue names.
func TestLoadYAML_Valid(t *testing.T) {
	yaml := []byte(`
config_version: 1
queues:
  payments.charge:
    retry:
      max_attempts: 8
      initial_backoff: 2s
    sync_retry:
      attempts: 2
      backoff: 100ms
`)
	cfg, err := rdq.LoadYAML(yaml)
	if err != nil {
		t.Fatalf("LoadYAML: %v", err)
	}
	names := cfg.QueueNames()
	if len(names) != 1 || names[0] != "payments.charge" {
		t.Fatalf("QueueNames = %v, want [payments.charge]", names)
	}
}

// TestLoadYAML_InvalidYAML verifies that malformed YAML is rejected.
func TestLoadYAML_InvalidYAML(t *testing.T) {
	_, err := rdq.LoadYAML([]byte(`{not: valid: yaml`))
	if err == nil {
		t.Fatal("LoadYAML should fail on invalid YAML")
	}
}

// TestLoadYAML_UnknownKey verifies that an unknown key is rejected (strict
// schema, design 03 §3).
func TestLoadYAML_UnknownKey(t *testing.T) {
	yaml := []byte(`
config_version: 1
queues:
  my.queue:
    retry:
      max_attempts: 3
      unknown_field: oops
`)
	_, err := rdq.LoadYAML(yaml)
	if err == nil {
		t.Fatal("LoadYAML should reject unknown keys")
	}
}

// --- builder == YAML equivalence ---

// TestBuilderYAML_Equivalence is the core acceptance criterion for T4.3: the
// QueueBuilder and LoadYAML produce structurally identical configs when given
// the same values. We compare by JSON serialisation to sidestep pointer
// address differences while still catching field-value divergence.
func TestBuilderYAML_Equivalence(t *testing.T) {
	const queueName = "payments.charge"

	// Build via code builder.
	_, builderCfg, err := rdq.Queue(queueName).
		MaxAttempts(8).
		InitialBackoff(2 * time.Second).
		SyncRetryAttempts(2).
		SyncRetryBackoff(100 * time.Millisecond).
		Build()
	if err != nil {
		t.Fatalf("Queue.Build: %v", err)
	}

	// Build via YAML loader (no defaults block so merge is a no-op).
	yaml := []byte(`
config_version: 1
queues:
  payments.charge:
    retry:
      max_attempts: 8
      initial_backoff: 2s
    sync_retry:
      attempts: 2
      backoff: 100ms
`)
	yamlConfig, err := rdq.LoadYAML(yaml)
	if err != nil {
		t.Fatalf("LoadYAML: %v", err)
	}
	yamlQueueCfg, err := yamlConfig.Resolved(queueName)
	if err != nil {
		t.Fatalf("Resolved: %v", err)
	}

	builderJSON := marshalJSON(t, builderCfg)
	yamlJSON := marshalJSON(t, yamlQueueCfg)
	if builderJSON != yamlJSON {
		t.Fatalf("builder and YAML configs differ:\n  builder: %s\n  yaml:    %s", builderJSON, yamlJSON)
	}
}

// TestBuilderYAML_Equivalence_SyncRetryOnly verifies equivalence for a config
// that sets only the sync_retry block.
func TestBuilderYAML_Equivalence_SyncRetryOnly(t *testing.T) {
	const queueName = "fast.queue"

	_, builderCfg, err := rdq.Queue(queueName).
		SyncRetryAttempts(3).
		SyncRetryBackoff(50 * time.Millisecond).
		Build()
	if err != nil {
		t.Fatalf("Queue.Build: %v", err)
	}

	yaml := []byte(`
config_version: 1
queues:
  fast.queue:
    sync_retry:
      attempts: 3
      backoff: 50ms
`)
	yamlConfig, err := rdq.LoadYAML(yaml)
	if err != nil {
		t.Fatalf("LoadYAML: %v", err)
	}
	yamlQueueCfg, err := yamlConfig.Resolved(queueName)
	if err != nil {
		t.Fatalf("Resolved: %v", err)
	}

	if marshalJSON(t, builderCfg) != marshalJSON(t, yamlQueueCfg) {
		t.Fatalf("builder and YAML configs differ for sync_retry-only config:\n  builder: %s\n  yaml:    %s",
			marshalJSON(t, builderCfg), marshalJSON(t, yamlQueueCfg))
	}
}

// TestBuilderYAML_DefaultsMerge verifies that LoadYAML correctly inherits
// defaults fields that the queue does not override, while the builder only
// represents the explicit per-queue config (no merged defaults).
func TestBuilderYAML_DefaultsMerge(t *testing.T) {
	yaml := []byte(`
config_version: 1
defaults:
  retry:
    max_attempts: 5
    initial_backoff: 1s
    jitter: 0.2
queues:
  billing.invoice:
    retry:
      max_attempts: 8
`)
	cfg, err := rdq.LoadYAML(yaml)
	if err != nil {
		t.Fatalf("LoadYAML: %v", err)
	}
	resolved, err := cfg.Resolved("billing.invoice")
	if err != nil {
		t.Fatalf("Resolved: %v", err)
	}

	// max_attempts overridden by queue
	if *resolved.Retry.MaxAttempts != 8 {
		t.Fatalf("MaxAttempts = %d, want 8 (overridden by queue)", *resolved.Retry.MaxAttempts)
	}
	// initial_backoff and jitter inherited from defaults
	if resolved.Retry.InitialBackoff.Std() != time.Second {
		t.Fatalf("InitialBackoff = %v, want 1s (inherited from defaults)", resolved.Retry.InitialBackoff.Std())
	}
	if *resolved.Retry.Jitter != 0.2 {
		t.Fatalf("Jitter = %g, want 0.2 (inherited from defaults)", *resolved.Retry.Jitter)
	}
}

// TestQueueBuilder_BuildDistinct verifies that successive Build calls return
// distinct *QueueConfig pointers (each call gets its own outer struct), so
// callers can store configs independently.
func TestQueueBuilder_BuildDistinct(t *testing.T) {
	b := rdq.Queue("iso.queue").MaxAttempts(3)
	_, cfg1, _ := b.Build()
	_, cfg2, _ := b.Build()

	if cfg1 == cfg2 {
		t.Fatal("Build must return distinct *QueueConfig on each call")
	}
}
