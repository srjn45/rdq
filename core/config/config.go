// SPDX-License-Identifier: Apache-2.0

// Package config is the queue-configuration contract (design 03): the third
// rdq contract, holding everything the wire envelope deliberately excludes —
// how a queue is retried, leased, classified, rate-limited, and (in server
// mode) called back. A task names its queue; the queue's config decides its
// fate, resolved by the engine at claim time so every field is live-tunable
// during an incident.
//
// Config is validated strictly: unknown keys are rejected at load time rather
// than silently ignored (design 03 §3). This is the deliberate opposite of the
// envelope, which preserves unknown fields — a config typo must fail fast at
// boot or on an admin write, never at 3am.
//
// A top-level defaults block applies to every queue; each queue overrides it by
// per-key deep-merge, so a queue that sets one field of retry inherits the rest
// of defaults.retry rather than replacing the block wholesale (design 03 §1/§6,
// OI-3). That per-key merge is the only merging rdq does. Load applies it and
// exposes the effective per-queue config through Resolved.
//
// Scalar fields are pointers so that "absent" (nil, inherit from defaults) is
// distinguishable from "set to the zero value"; this is what makes the deep
// merge precise.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/srjn45/rdq/core/envelope"
	"gopkg.in/yaml.v3"
)

// KnownConfigVersion is the highest config_version this build understands. A
// document declaring a newer version is rejected (design 03 §3): config_version
// bumps only on breaking schema changes, and a loader must never guess at a
// schema it does not know.
const KnownConfigVersion = 1

// version_mismatch policies (design 03 §2, PRD FR-12).
const (
	VersionMismatchRunLatest  = "run-latest"
	VersionMismatchDeadLetter = "dead-letter"
)

// Callback protocols (design 03 §2). gRPC callbacks are post-v1 but the value
// is accepted by the schema so a config can be written ahead of the transport.
const (
	ProtocolHTTP = "http"
	ProtocolGRPC = "grpc"
)

// Callback auth types (design 03 §2).
const (
	AuthNone   = "none"
	AuthBearer = "bearer"
	AuthHeader = "header"
)

// secretRefEnvScheme is the only secret_ref indirection scheme in v1: a process
// environment variable (design 03 §3). Vault/SM schemes are post-v1.
const secretRefEnvScheme = "env:"

// Config is a parsed, validated queue-configuration document (design 03 §2).
// Queues holds each queue exactly as written; Resolved returns the effective
// config with defaults deep-merged in.
type Config struct {
	ConfigVersion int                     `yaml:"config_version" json:"config_version"`
	Defaults      *QueueConfig            `yaml:"defaults,omitempty" json:"defaults,omitempty"`
	Queues        map[string]*QueueConfig `yaml:"queues,omitempty" json:"queues,omitempty"`

	// resolved is Queues with Defaults deep-merged into each entry, computed
	// once by Load. Nil for a document that never went through Load.
	resolved map[string]*QueueConfig
}

// QueueConfig is the full per-queue schema (design 03 §2). It doubles as the
// shape of the defaults block. Every block is a pointer so an omitted block
// inherits wholesale, and every scalar within a block is a pointer so an
// omitted scalar inherits field-by-field.
type QueueConfig struct {
	Retry          *RetryConfig          `yaml:"retry,omitempty" json:"retry,omitempty"`
	Execution      *ExecutionConfig      `yaml:"execution,omitempty" json:"execution,omitempty"`
	Limits         *LimitsConfig         `yaml:"limits,omitempty" json:"limits,omitempty"`
	Worker         *WorkerConfig         `yaml:"worker,omitempty" json:"worker,omitempty"`
	Classification *ClassificationConfig `yaml:"classification,omitempty" json:"classification,omitempty"`
	Handler        *HandlerConfig        `yaml:"handler,omitempty" json:"handler,omitempty"`
	SyncRetry      *SyncRetryConfig      `yaml:"sync_retry,omitempty" json:"sync_retry,omitempty"`
	Callback       *CallbackConfig       `yaml:"callback,omitempty" json:"callback,omitempty"`
}

// RetryConfig governs the backoff ladder (design 03 §2/§3). The engine computes
// delay(n) = min(initial_backoff × multiplier^(n-1), max_backoff) × (1 ± jitter·rand).
type RetryConfig struct {
	MaxAttempts       *int      `yaml:"max_attempts,omitempty" json:"max_attempts,omitempty"`
	InitialBackoff    *Duration `yaml:"initial_backoff,omitempty" json:"initial_backoff,omitempty"`
	BackoffMultiplier *float64  `yaml:"backoff_multiplier,omitempty" json:"backoff_multiplier,omitempty"`
	MaxBackoff        *Duration `yaml:"max_backoff,omitempty" json:"max_backoff,omitempty"`
	Jitter            *float64  `yaml:"jitter,omitempty" json:"jitter,omitempty"`
}

// ExecutionConfig governs leasing and handler timeouts (design 03 §2). The
// handler timeout must never exceed the lease, or a handler could still be
// running when the task becomes reclaimable.
type ExecutionConfig struct {
	Lease          *Duration `yaml:"lease,omitempty" json:"lease,omitempty"`
	HandlerTimeout *Duration `yaml:"handler_timeout,omitempty" json:"handler_timeout,omitempty"`
	Heartbeat      *bool     `yaml:"heartbeat,omitempty" json:"heartbeat,omitempty"`
}

// LimitsConfig governs payload size and retention (design 03 §2).
type LimitsConfig struct {
	MaxPayloadSize *Size     `yaml:"max_payload_size,omitempty" json:"max_payload_size,omitempty"`
	TTLSucceeded   *Duration `yaml:"ttl_succeeded,omitempty" json:"ttl_succeeded,omitempty"`
}

// WorkerConfig tunes the claim loop per queue (design 03 §2). RateLimit is a
// per-instance token bucket (G12): the effective global rate across N instances
// is N × rate_limit; omit it for unlimited.
type WorkerConfig struct {
	BatchSize    *int      `yaml:"batch_size,omitempty" json:"batch_size,omitempty"`
	PollInterval *Duration `yaml:"poll_interval,omitempty" json:"poll_interval,omitempty"`
	Concurrency  *int      `yaml:"concurrency,omitempty" json:"concurrency,omitempty"`
	RateLimit    *Rate     `yaml:"rate_limit,omitempty" json:"rate_limit,omitempty"`
}

// ClassificationConfig holds the config-glob layer of outcome classification
// (design 03 §4, layer 4): globs matched against a reported error.type. Code
// classifiers and OutcomeMappers take precedence; this is the only layer
// expressible in YAML.
type ClassificationConfig struct {
	Retryable []string `yaml:"retryable,omitempty" json:"retryable,omitempty"`
	Permanent []string `yaml:"permanent,omitempty" json:"permanent,omitempty"`
}

// HandlerConfig governs handler-version-mismatch policy (design 03 §2, FR-12).
type HandlerConfig struct {
	VersionMismatch *string `yaml:"version_mismatch,omitempty" json:"version_mismatch,omitempty"`
}

// SyncRetryConfig configures in-process retries before durable enqueue
// (design 03 §2). It runs in the embedded SDK's submit path and is ignored by
// worker/server claiming.
type SyncRetryConfig struct {
	Attempts *int      `yaml:"attempts,omitempty" json:"attempts,omitempty"`
	Backoff  *Duration `yaml:"backoff,omitempty" json:"backoff,omitempty"`
}

// CallbackConfig configures the server-mode HTTP callback (design 03 §2);
// ignored by the embedded SDK. callback.timeout must not exceed handler_timeout.
type CallbackConfig struct {
	Protocol        *string          `yaml:"protocol,omitempty" json:"protocol,omitempty"`
	URL             *string          `yaml:"url,omitempty" json:"url,omitempty"`
	Timeout         *Duration        `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	Auth            *CallbackAuth    `yaml:"auth,omitempty" json:"auth,omitempty"`
	ResponseMapping *ResponseMapping `yaml:"response_mapping,omitempty" json:"response_mapping,omitempty"`
}

// CallbackAuth configures callback authentication (design 03 §2). secret_ref is
// indirection only — a raw secret never appears in config.
type CallbackAuth struct {
	Type      *string `yaml:"type,omitempty" json:"type,omitempty"`
	SecretRef *string `yaml:"secret_ref,omitempty" json:"secret_ref,omitempty"`
}

// ResponseMapping overrides the FR-29 default callback-status classification
// per status or class (design 03 §2).
type ResponseMapping struct {
	RetryableStatus []StatusMatcher `yaml:"retryable_status,omitempty" json:"retryable_status,omitempty"`
	PermanentStatus []StatusMatcher `yaml:"permanent_status,omitempty" json:"permanent_status,omitempty"`
}

// ErrUnknownQueue is returned by Resolved for a queue absent from the document.
// A task for an unconfigured queue is rejected — never silently defaulted
// (design 03 §3).
var ErrUnknownQueue = errors.New("config: unknown queue")

// Load parses a YAML config document strictly, deep-merges defaults into every
// queue, and validates the result — returning the first problem it finds so a
// typo or an out-of-range value fails fast at load time (design 03 §3). Unknown
// keys at any level are an error, not a silent no-op.
func Load(data []byte) (*Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // strict: reject unknown keys anywhere in the tree.

	var c Config
	if err := dec.Decode(&c); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("config: empty document")
		}
		return nil, fmt.Errorf("config: %w", err)
	}
	// A second document in the stream is a mistake — one config per file.
	if err := dec.Decode(new(struct{})); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("config: expected a single YAML document")
	}

	if err := validateVersion(c.ConfigVersion); err != nil {
		return nil, err
	}
	// Validate the defaults block on its own so an invalid default is caught
	// even when no queue happens to surface it.
	if c.Defaults != nil {
		if err := validateQueueConfig(c.Defaults, "defaults"); err != nil {
			return nil, err
		}
	}

	c.resolved = make(map[string]*QueueConfig, len(c.Queues))
	for name, qc := range c.Queues {
		// Queue names share the envelope's frozen charset rule (design 03 §3,
		// envelope §2); reuse the contract's validator rather than restating it.
		if err := envelope.ValidateQueue(name); err != nil {
			return nil, fmt.Errorf("config: %w", err)
		}
		merged := mergeQueueConfig(c.Defaults, qc)
		if err := validateQueueConfig(merged, fmt.Sprintf("queues.%s", name)); err != nil {
			return nil, err
		}
		c.resolved[name] = merged
	}
	return &c, nil
}

// Resolved returns the effective config for a queue: its written values with
// defaults deep-merged underneath (design 03 §1/§6). It reports ErrUnknownQueue
// for a queue the document does not define.
func (c *Config) Resolved(queue string) (*QueueConfig, error) {
	qc, ok := c.resolved[queue]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownQueue, queue)
	}
	return qc, nil
}

// QueueNames returns the configured queue names in sorted order.
func (c *Config) QueueNames() []string {
	names := make([]string, 0, len(c.Queues))
	for name := range c.Queues {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// validateVersion enforces the config_version gate (design 03 §3).
func validateVersion(v int) error {
	if v < 1 {
		return fmt.Errorf("config: missing or invalid config_version (want 1..%d)", KnownConfigVersion)
	}
	if v > KnownConfigVersion {
		return fmt.Errorf("config: config_version %d is newer than this build understands (max %d)", v, KnownConfigVersion)
	}
	return nil
}
