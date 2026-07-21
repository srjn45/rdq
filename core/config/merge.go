// SPDX-License-Identifier: Apache-2.0

package config

// Per-key deep-merge of the defaults block into a queue (design 03 §1/§6,
// OI-3). The rule: a queue overrides defaults field by field, not block by
// block. A queue that sets only retry.max_attempts still inherits
// defaults.retry.initial_backoff and defaults.retry.jitter; only max_attempts
// is replaced. Recursion goes as deep as the schema nests (down into
// callback.auth and callback.response_mapping) so inheritance is consistent at
// every level. This per-key merge is the only merging rdq does.
//
// Because a scalar is a pointer, "set" is exactly "non-nil": pick the queue's
// value when present, else the default's. A slice- or map-valued key (e.g.
// classification.retryable) is treated as a single leaf — set wholesale by the
// queue or inherited wholesale — since globs cannot be meaningfully element-
// merged. The inputs are never mutated; merged blocks alias the originals where
// nothing was overridden, which is safe because a loaded Config is read-only.

// override returns the queue's value when it set one (non-nil), else the
// default's — the leaf rule of the per-key merge.
func override[T any](queue, base *T) *T {
	if queue != nil {
		return queue
	}
	return base
}

// overrideSlice is override for a slice-valued leaf: a nil slice means the queue
// did not set the key (inherit); a non-nil slice — including an explicit empty
// one — is a wholesale override.
func overrideSlice[T any](queue, base []T) []T {
	if queue != nil {
		return queue
	}
	return base
}

// mergeQueueConfig deep-merges base (defaults) under queue, returning the
// effective config. Either argument may be nil.
func mergeQueueConfig(base, queue *QueueConfig) *QueueConfig {
	if base == nil {
		base = &QueueConfig{}
	}
	if queue == nil {
		queue = &QueueConfig{}
	}
	return &QueueConfig{
		Retry:          mergeRetry(base.Retry, queue.Retry),
		Execution:      mergeExecution(base.Execution, queue.Execution),
		Limits:         mergeLimits(base.Limits, queue.Limits),
		Worker:         mergeWorker(base.Worker, queue.Worker),
		Classification: mergeClassification(base.Classification, queue.Classification),
		Handler:        mergeHandler(base.Handler, queue.Handler),
		SyncRetry:      mergeSyncRetry(base.SyncRetry, queue.SyncRetry),
		Callback:       mergeCallback(base.Callback, queue.Callback),
	}
}

func mergeRetry(base, queue *RetryConfig) *RetryConfig {
	if base == nil {
		return queue
	}
	if queue == nil {
		return base
	}
	return &RetryConfig{
		MaxAttempts:       override(queue.MaxAttempts, base.MaxAttempts),
		InitialBackoff:    override(queue.InitialBackoff, base.InitialBackoff),
		BackoffMultiplier: override(queue.BackoffMultiplier, base.BackoffMultiplier),
		MaxBackoff:        override(queue.MaxBackoff, base.MaxBackoff),
		Jitter:            override(queue.Jitter, base.Jitter),
	}
}

func mergeExecution(base, queue *ExecutionConfig) *ExecutionConfig {
	if base == nil {
		return queue
	}
	if queue == nil {
		return base
	}
	return &ExecutionConfig{
		Lease:          override(queue.Lease, base.Lease),
		HandlerTimeout: override(queue.HandlerTimeout, base.HandlerTimeout),
		Heartbeat:      override(queue.Heartbeat, base.Heartbeat),
	}
}

func mergeLimits(base, queue *LimitsConfig) *LimitsConfig {
	if base == nil {
		return queue
	}
	if queue == nil {
		return base
	}
	return &LimitsConfig{
		MaxPayloadSize: override(queue.MaxPayloadSize, base.MaxPayloadSize),
		TTLSucceeded:   override(queue.TTLSucceeded, base.TTLSucceeded),
	}
}

func mergeWorker(base, queue *WorkerConfig) *WorkerConfig {
	if base == nil {
		return queue
	}
	if queue == nil {
		return base
	}
	return &WorkerConfig{
		BatchSize:    override(queue.BatchSize, base.BatchSize),
		PollInterval: override(queue.PollInterval, base.PollInterval),
		Concurrency:  override(queue.Concurrency, base.Concurrency),
		RateLimit:    override(queue.RateLimit, base.RateLimit),
	}
}

func mergeClassification(base, queue *ClassificationConfig) *ClassificationConfig {
	if base == nil {
		return queue
	}
	if queue == nil {
		return base
	}
	return &ClassificationConfig{
		Retryable: overrideSlice(queue.Retryable, base.Retryable),
		Permanent: overrideSlice(queue.Permanent, base.Permanent),
	}
}

func mergeHandler(base, queue *HandlerConfig) *HandlerConfig {
	if base == nil {
		return queue
	}
	if queue == nil {
		return base
	}
	return &HandlerConfig{
		VersionMismatch: override(queue.VersionMismatch, base.VersionMismatch),
	}
}

func mergeSyncRetry(base, queue *SyncRetryConfig) *SyncRetryConfig {
	if base == nil {
		return queue
	}
	if queue == nil {
		return base
	}
	return &SyncRetryConfig{
		Attempts: override(queue.Attempts, base.Attempts),
		Backoff:  override(queue.Backoff, base.Backoff),
	}
}

func mergeCallback(base, queue *CallbackConfig) *CallbackConfig {
	if base == nil {
		return queue
	}
	if queue == nil {
		return base
	}
	return &CallbackConfig{
		Protocol:        override(queue.Protocol, base.Protocol),
		URL:             override(queue.URL, base.URL),
		Timeout:         override(queue.Timeout, base.Timeout),
		Auth:            mergeCallbackAuth(base.Auth, queue.Auth),
		ResponseMapping: mergeResponseMapping(base.ResponseMapping, queue.ResponseMapping),
	}
}

func mergeCallbackAuth(base, queue *CallbackAuth) *CallbackAuth {
	if base == nil {
		return queue
	}
	if queue == nil {
		return base
	}
	return &CallbackAuth{
		Type:      override(queue.Type, base.Type),
		SecretRef: override(queue.SecretRef, base.SecretRef),
	}
}

func mergeResponseMapping(base, queue *ResponseMapping) *ResponseMapping {
	if base == nil {
		return queue
	}
	if queue == nil {
		return base
	}
	return &ResponseMapping{
		RetryableStatus: overrideSlice(queue.RetryableStatus, base.RetryableStatus),
		PermanentStatus: overrideSlice(queue.PermanentStatus, base.PermanentStatus),
	}
}
