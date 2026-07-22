// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"net/url"
	"strings"
)

// Strict field-rule validation (design 03 §3). Unknown keys are already
// rejected by the strict YAML decoder in Load; this pass enforces the value
// rules: numeric bounds, the lease/timeout ordering, enum membership, and the
// secret_ref indirection scheme. It runs on the defaults block and on every
// deep-merged queue, so a cross-field rule such as handler_timeout ≤ lease is
// checked against the effective values even when the two fields come from
// different sources (one from defaults, one from the queue).
//
// Single-field rules fire whenever the field is present; cross-field rules fire
// only when both operands are present, so a partial defaults block is not
// rejected for a comparison it cannot yet make.

// validateQueueConfig validates one block set. path names the location
// ("defaults" or "queues.<name>") for error messages.
func validateQueueConfig(qc *QueueConfig, path string) error {
	if qc == nil {
		return nil
	}
	if err := validateRetry(qc.Retry, path); err != nil {
		return err
	}
	if err := validateExecution(qc.Execution, path); err != nil {
		return err
	}
	if err := validateLimits(qc.Limits, path); err != nil {
		return err
	}
	if err := validateWorker(qc.Worker, path); err != nil {
		return err
	}
	if err := validateHandler(qc.Handler, path); err != nil {
		return err
	}
	if err := validateSyncRetry(qc.SyncRetry, path); err != nil {
		return err
	}
	return validateCallback(qc, path)
}

func validateRetry(r *RetryConfig, path string) error {
	if r == nil {
		return nil
	}
	if r.MaxAttempts != nil && *r.MaxAttempts < 1 {
		return errf(path, "retry.max_attempts must be >= 1, got %d", *r.MaxAttempts)
	}
	if r.BackoffMultiplier != nil && *r.BackoffMultiplier < 1.0 {
		return errf(path, "retry.backoff_multiplier must be >= 1.0, got %g", *r.BackoffMultiplier)
	}
	if r.Jitter != nil && (*r.Jitter < 0 || *r.Jitter > 1) {
		return errf(path, "retry.jitter must be in [0, 1], got %g", *r.Jitter)
	}
	if err := requirePositive(r.InitialBackoff, path, "retry.initial_backoff"); err != nil {
		return err
	}
	return requirePositive(r.MaxBackoff, path, "retry.max_backoff")
}

func validateExecution(e *ExecutionConfig, path string) error {
	if e == nil {
		return nil
	}
	if err := requirePositive(e.Lease, path, "execution.lease"); err != nil {
		return err
	}
	if err := requirePositive(e.HandlerTimeout, path, "execution.handler_timeout"); err != nil {
		return err
	}
	// handler_timeout <= lease (design 03 §3): a handler must finish before its
	// task can be reclaimed.
	if e.Lease != nil && e.HandlerTimeout != nil && e.HandlerTimeout.Std() > e.Lease.Std() {
		return errf(path, "execution.handler_timeout (%s) must be <= execution.lease (%s)",
			e.HandlerTimeout.Std(), e.Lease.Std())
	}
	return nil
}

func validateLimits(l *LimitsConfig, path string) error {
	if l == nil {
		return nil
	}
	if l.MaxPayloadSize != nil && l.MaxPayloadSize.Bytes() <= 0 {
		return errf(path, "limits.max_payload_size must be > 0, got %d bytes", l.MaxPayloadSize.Bytes())
	}
	if l.TTLSucceeded != nil && l.TTLSucceeded.Std() < 0 {
		return errf(path, "limits.ttl_succeeded must be >= 0, got %s", l.TTLSucceeded.Std())
	}
	return nil
}

func validateWorker(w *WorkerConfig, path string) error {
	if w == nil {
		return nil
	}
	if w.BatchSize != nil && *w.BatchSize < 1 {
		return errf(path, "worker.batch_size must be >= 1, got %d", *w.BatchSize)
	}
	if w.Concurrency != nil && *w.Concurrency < 1 {
		return errf(path, "worker.concurrency must be >= 1, got %d", *w.Concurrency)
	}
	if err := requirePositive(w.PollInterval, path, "worker.poll_interval"); err != nil {
		return err
	}
	if w.RateLimit != nil && (w.RateLimit.Count <= 0 || w.RateLimit.Per <= 0) {
		return errf(path, "worker.rate_limit must be a positive count/period, got %q", w.RateLimit)
	}
	return nil
}

func validateHandler(h *HandlerConfig, path string) error {
	if h == nil || h.VersionMismatch == nil {
		return nil
	}
	switch *h.VersionMismatch {
	case VersionMismatchRunLatest, VersionMismatchDeadLetter:
		return nil
	default:
		return errf(path, "handler.version_mismatch must be %q or %q, got %q",
			VersionMismatchRunLatest, VersionMismatchDeadLetter, *h.VersionMismatch)
	}
}

func validateSyncRetry(s *SyncRetryConfig, path string) error {
	if s == nil {
		return nil
	}
	if s.Attempts != nil && *s.Attempts < 0 {
		return errf(path, "sync_retry.attempts must be >= 0, got %d", *s.Attempts)
	}
	if s.Backoff != nil && s.Backoff.Std() < 0 {
		return errf(path, "sync_retry.backoff must be >= 0, got %s", s.Backoff.Std())
	}
	return nil
}

// validateCallback checks the callback block, including the callback.timeout <=
// handler_timeout cross-field rule which reaches into the execution block.
func validateCallback(qc *QueueConfig, path string) error {
	c := qc.Callback
	if c == nil {
		return nil
	}
	if c.Protocol != nil {
		switch *c.Protocol {
		case ProtocolHTTP, ProtocolGRPC:
		default:
			return errf(path, "callback.protocol must be %q or %q, got %q", ProtocolHTTP, ProtocolGRPC, *c.Protocol)
		}
	}
	if c.URL == nil || *c.URL == "" {
		return errf(path, "callback.url is required when a callback is configured")
	}
	if err := validateCallbackURL(*c.URL, c.Protocol, path); err != nil {
		return err
	}
	if err := requirePositive(c.Timeout, path, "callback.timeout"); err != nil {
		return err
	}
	// callback.timeout <= handler_timeout (design 03 §3).
	if c.Timeout != nil && qc.Execution != nil && qc.Execution.HandlerTimeout != nil &&
		c.Timeout.Std() > qc.Execution.HandlerTimeout.Std() {
		return errf(path, "callback.timeout (%s) must be <= execution.handler_timeout (%s)",
			c.Timeout.Std(), qc.Execution.HandlerTimeout.Std())
	}
	return validateCallbackAuth(c.Auth, path)
}

// validateCallbackURL parses the URL and, for the http protocol, requires an
// http/https scheme. The SSRF allowlist is server config, not queue config
// (design 03 §3/§5), so it is intentionally not checked here.
func validateCallbackURL(raw string, protocol *string, path string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return errf(path, "callback.url %q is not a valid URL: %v", raw, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return errf(path, "callback.url %q must be absolute (scheme://host)", raw)
	}
	if (protocol == nil || *protocol == ProtocolHTTP) && u.Scheme != "http" && u.Scheme != "https" {
		return errf(path, "callback.url %q must use http or https", raw)
	}
	return nil
}

func validateCallbackAuth(a *CallbackAuth, path string) error {
	if a == nil {
		return nil
	}
	authType := AuthNone
	if a.Type != nil {
		authType = *a.Type
	}
	switch authType {
	case AuthNone, AuthBearer, AuthHeader:
	default:
		return errf(path, "callback.auth.type must be one of %q, %q, %q, got %q",
			AuthNone, AuthBearer, AuthHeader, authType)
	}
	if a.SecretRef != nil {
		if err := validateSecretRef(*a.SecretRef, path); err != nil {
			return err
		}
	} else if authType == AuthBearer || authType == AuthHeader {
		return errf(path, "callback.auth.secret_ref is required for auth type %q", authType)
	}
	return nil
}

// validateSecretRef enforces the v1 env: indirection scheme (design 03 §3):
// raw secrets never live in config, only a pointer to a process env var.
func validateSecretRef(ref, path string) error {
	if !strings.HasPrefix(ref, secretRefEnvScheme) {
		return errf(path, "callback.auth.secret_ref %q must use the %s scheme (v1 supports env: only)", ref, secretRefEnvScheme)
	}
	if strings.TrimPrefix(ref, secretRefEnvScheme) == "" {
		return errf(path, "callback.auth.secret_ref %q names no environment variable", ref)
	}
	return nil
}

// requirePositive rejects a present duration that is not strictly positive.
func requirePositive(d *Duration, path, field string) error {
	if d != nil && d.Std() <= 0 {
		return errf(path, "%s must be > 0, got %s", field, d.Std())
	}
	return nil
}

// ValidateQueue validates a single QueueConfig block by name. It is called by
// the admin API before persisting config via PUT /admin/queues/{queue}/config
// (design 04 §3). The queue name is used only in error messages.
func ValidateQueue(qc *QueueConfig, queue string) error {
	return validateQueueConfig(qc, "queues."+queue)
}

// errf builds a validation error prefixed with the package name and the block
// path, so a message reads e.g. `config: queues.payments.charge: ...`.
func errf(path, format string, args ...any) error {
	return fmt.Errorf("config: %s: %s", path, fmt.Sprintf(format, args...))
}
