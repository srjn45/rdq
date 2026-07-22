// SPDX-License-Identifier: Apache-2.0

package rdq_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/config"
	rdq "github.com/srjn45/rdq/sdk-go"
)

// noSleep is an injectable Sleep that returns immediately (zero-delay tests).
func noSleep(_ context.Context, _ time.Duration) error { return nil }

// TestSyncRetrier_SuccessOnFirstAttempt verifies that a handler succeeding on
// the first inline attempt returns nil and never calls fallback.
func TestSyncRetrier_SuccessOnFirstAttempt(t *testing.T) {
	r := &rdq.SyncRetrier{Attempts: 3, Backoff: time.Millisecond, Sleep: noSleep}

	fallbackCalled := false
	err := r.Run(context.Background(),
		func() error { return nil },
		func() error { fallbackCalled = true; return nil },
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if fallbackCalled {
		t.Fatal("fallback must not be called when fn succeeds inline")
	}
}

// TestSyncRetrier_SuccessOnRetry verifies that a handler failing once and then
// succeeding on the second attempt never calls fallback.
func TestSyncRetrier_SuccessOnRetry(t *testing.T) {
	r := &rdq.SyncRetrier{Attempts: 3, Backoff: time.Millisecond, Sleep: noSleep}

	var calls atomic.Int32
	fallbackCalled := false
	err := r.Run(context.Background(),
		func() error {
			if calls.Add(1) < 2 {
				return errors.New("transient")
			}
			return nil
		},
		func() error { fallbackCalled = true; return nil },
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if fallbackCalled {
		t.Fatal("fallback must not be called when fn eventually succeeds inline")
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("fn called %d times, want 2", n)
	}
}

// TestSyncRetrier_ExhaustThenFallback is the core acceptance criterion: all
// inline attempts fail, so Run must call fallback exactly once and return its
// result.
func TestSyncRetrier_ExhaustThenFallback(t *testing.T) {
	const maxAttempts = 3
	r := &rdq.SyncRetrier{Attempts: maxAttempts, Backoff: time.Millisecond, Sleep: noSleep}

	var fnCalls atomic.Int32
	var fallbackCalls atomic.Int32
	sentinel := errors.New("enqueued")

	err := r.Run(context.Background(),
		func() error { fnCalls.Add(1); return errors.New("always fails") },
		func() error { fallbackCalls.Add(1); return sentinel },
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel error from fallback, got %v", err)
	}
	if n := fnCalls.Load(); int(n) != maxAttempts {
		t.Fatalf("fn called %d times, want %d", n, maxAttempts)
	}
	if n := fallbackCalls.Load(); n != 1 {
		t.Fatalf("fallback called %d times, want 1", n)
	}
}

// TestSyncRetrier_ZeroAttempts verifies that Attempts=0 skips the inline path
// and calls fallback immediately without invoking fn.
func TestSyncRetrier_ZeroAttempts(t *testing.T) {
	r := &rdq.SyncRetrier{Attempts: 0, Sleep: noSleep}

	fnCalled := false
	fallbackCalled := false
	err := r.Run(context.Background(),
		func() error { fnCalled = true; return nil },
		func() error { fallbackCalled = true; return nil },
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if fnCalled {
		t.Fatal("fn must not be called when Attempts=0")
	}
	if !fallbackCalled {
		t.Fatal("fallback must be called when Attempts=0")
	}
}

// TestSyncRetrier_ContextCancelled verifies that cancelling the context during
// the inter-attempt sleep aborts without calling fallback and returns the
// context error.
func TestSyncRetrier_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	r := &rdq.SyncRetrier{
		Attempts: 5,
		Backoff:  time.Hour,
		Sleep: func(ctx context.Context, _ time.Duration) error {
			cancel() // cancel on first sleep
			return ctx.Err()
		},
	}

	fallbackCalled := false
	err := r.Run(ctx,
		func() error { return errors.New("fail") },
		func() error { fallbackCalled = true; return nil },
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if fallbackCalled {
		t.Fatal("fallback must not be called on context cancellation")
	}
}

// TestSyncRetrier_ContextAlreadyCancelled verifies that an already-cancelled
// context causes an immediate abort before the first fn call.
func TestSyncRetrier_ContextAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &rdq.SyncRetrier{Attempts: 3, Sleep: noSleep}

	fnCalled := false
	fallbackCalled := false
	err := r.Run(ctx,
		func() error { fnCalled = true; return nil },
		func() error { fallbackCalled = true; return nil },
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if fnCalled {
		t.Fatal("fn must not be called when context is already cancelled")
	}
	if fallbackCalled {
		t.Fatal("fallback must not be called when context is already cancelled")
	}
}

// TestNewSyncRetrier_FromConfig verifies that NewSyncRetrier correctly
// extracts Attempts and Backoff from a SyncRetryConfig.
func TestNewSyncRetrier_FromConfig(t *testing.T) {
	attempts := 2
	backoff := config.Duration(100 * time.Millisecond)
	cfg := &config.SyncRetryConfig{
		Attempts: &attempts,
		Backoff:  &backoff,
	}

	r := rdq.NewSyncRetrier(cfg)
	if r.Attempts != 2 {
		t.Fatalf("Attempts = %d, want 2", r.Attempts)
	}
	if r.Backoff != 100*time.Millisecond {
		t.Fatalf("Backoff = %v, want 100ms", r.Backoff)
	}
}

// TestNewSyncRetrier_NilConfig verifies that a nil SyncRetryConfig produces a
// zero-attempt retrier that falls through to fallback immediately.
func TestNewSyncRetrier_NilConfig(t *testing.T) {
	r := rdq.NewSyncRetrier(nil)
	if r.Attempts != 0 {
		t.Fatalf("Attempts = %d, want 0 for nil config", r.Attempts)
	}

	fallbackCalled := false
	_ = r.Run(context.Background(),
		func() error { return nil },
		func() error { fallbackCalled = true; return nil },
	)
	if !fallbackCalled {
		t.Fatal("fallback must be called when Attempts=0")
	}
}
