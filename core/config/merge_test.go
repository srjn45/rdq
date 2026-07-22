// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"
	"time"
)

// TestDeepMergeOI3 asserts the design 03 §6 (OI-3) example directly: with
// defaults.retry {max_attempts:5, initial_backoff:1s, jitter:0.2}, a queue that
// sets only retry.max_attempts:8 inherits initial_backoff and jitter unchanged.
func TestDeepMergeOI3(t *testing.T) {
	cfg := loadFixture(t, "deep_merge.yaml")

	settle, err := cfg.Resolved("orders.settle")
	if err != nil {
		t.Fatal(err)
	}
	if got := *settle.Retry.MaxAttempts; got != 8 {
		t.Errorf("max_attempts got %d, want 8 (overridden)", got)
	}
	if got := settle.Retry.InitialBackoff.Std(); got != time.Second {
		t.Errorf("initial_backoff got %s, want 1s (inherited)", got)
	}
	if got := *settle.Retry.Jitter; got != 0.2 {
		t.Errorf("jitter got %g, want 0.2 (inherited)", got)
	}
	// The whole execution block is inherited.
	if got := settle.Execution.Lease.Std(); got != 60*time.Second {
		t.Errorf("lease got %s, want 60s (inherited block)", got)
	}
	if got := settle.Execution.HandlerTimeout.Std(); got != 45*time.Second {
		t.Errorf("handler_timeout got %s, want 45s (inherited block)", got)
	}

	// An empty queue ({}) inherits every default block wholesale.
	notify, err := cfg.Resolved("orders.notify")
	if err != nil {
		t.Fatal(err)
	}
	if *notify.Retry.MaxAttempts != 5 || notify.Retry.InitialBackoff.Std() != time.Second {
		t.Errorf("orders.notify did not inherit defaults.retry: %+v", notify.Retry)
	}
	if notify.Execution.Lease.Std() != 60*time.Second {
		t.Errorf("orders.notify did not inherit defaults.execution")
	}
}

// TestMergeDoesNotMutateInputs guards against a merge that aliases and then
// corrupts the defaults block shared across queues.
func TestMergeDoesNotMutateInputs(t *testing.T) {
	cfg := loadFixture(t, "deep_merge.yaml")
	if got := *cfg.Defaults.Retry.MaxAttempts; got != 5 {
		t.Fatalf("defaults.retry.max_attempts mutated to %d, want 5", got)
	}
	// orders.settle overrode to 8; defaults must still read 5.
	settle, _ := cfg.Resolved("orders.settle")
	if *settle.Retry.MaxAttempts != 8 {
		t.Fatal("override did not take")
	}
	if *cfg.Defaults.Retry.MaxAttempts != 5 {
		t.Fatal("override leaked into defaults")
	}
}

func ptr[T any](v T) *T { return &v }

// TestMergeSliceIsWholesale asserts a slice-valued key is replaced, not
// element-merged: a queue that sets classification.retryable replaces the
// default list but still inherits classification.permanent.
func TestMergeSliceIsWholesale(t *testing.T) {
	base := &QueueConfig{
		Classification: &ClassificationConfig{
			Retryable: []string{"a.*", "b.*"},
			Permanent: []string{"c.*"},
		},
	}
	queue := &QueueConfig{
		Classification: &ClassificationConfig{
			Retryable: []string{"z.*"},
		},
	}
	merged := mergeQueueConfig(base, queue)
	if got := merged.Classification.Retryable; len(got) != 1 || got[0] != "z.*" {
		t.Errorf("retryable got %v, want [z.*] (wholesale override)", got)
	}
	if got := merged.Classification.Permanent; len(got) != 1 || got[0] != "c.*" {
		t.Errorf("permanent got %v, want [c.*] (inherited)", got)
	}
}

// TestMergeNestedCallback checks recursion into callback.auth: a queue that sets
// only auth.secret_ref inherits auth.type and the sibling callback fields.
func TestMergeNestedCallback(t *testing.T) {
	base := &QueueConfig{
		Callback: &CallbackConfig{
			Protocol: ptr(ProtocolHTTP),
			URL:      ptr("https://base/cb"),
			Timeout:  ptr(Duration(30 * time.Second)),
			Auth:     &CallbackAuth{Type: ptr(AuthBearer), SecretRef: ptr("env:BASE")},
		},
	}
	queue := &QueueConfig{
		Callback: &CallbackConfig{
			Auth: &CallbackAuth{SecretRef: ptr("env:QUEUE")},
		},
	}
	merged := mergeQueueConfig(base, queue)
	if got := *merged.Callback.Auth.SecretRef; got != "env:QUEUE" {
		t.Errorf("secret_ref got %q, want env:QUEUE (override)", got)
	}
	if got := *merged.Callback.Auth.Type; got != AuthBearer {
		t.Errorf("auth.type got %q, want bearer (inherited within nested block)", got)
	}
	if got := *merged.Callback.URL; got != "https://base/cb" {
		t.Errorf("callback.url got %q, want inherited base value", got)
	}
}

// TestMergeNilInputs covers the degenerate combinations.
func TestMergeNilInputs(t *testing.T) {
	if m := mergeQueueConfig(nil, nil); m == nil {
		t.Fatal("merge(nil, nil) should be an empty config, not nil")
	}
	base := &QueueConfig{Retry: &RetryConfig{MaxAttempts: ptr(3)}}
	if m := mergeQueueConfig(base, nil); *m.Retry.MaxAttempts != 3 {
		t.Fatal("merge(base, nil) should yield the defaults")
	}
	queue := &QueueConfig{Retry: &RetryConfig{MaxAttempts: ptr(9)}}
	if m := mergeQueueConfig(nil, queue); *m.Retry.MaxAttempts != 9 {
		t.Fatal("merge(nil, queue) should yield the queue values")
	}
}
