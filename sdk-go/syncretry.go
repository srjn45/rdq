// SPDX-License-Identifier: Apache-2.0

package rdq

import (
	"context"
	"time"

	"github.com/srjn45/rdq/core/config"
)

// SyncRetrier runs a function synchronously up to Attempts times before
// falling through to a fallback (typically durable enqueue). It implements
// the embedded-SDK fast path (design 03 §2 sync_retry): if the operation
// usually succeeds immediately, sync retry avoids persisting and reclaiming a
// task through the durable queue.
//
// Attempts ≤ 0 skips the inline path entirely and calls the fallback
// immediately. A nil Sleep uses context-aware time.After.
type SyncRetrier struct {
	Attempts int
	Backoff  time.Duration
	Sleep    func(ctx context.Context, d time.Duration) error
}

// NewSyncRetrier builds a SyncRetrier from a SyncRetryConfig. A nil cfg
// returns a retrier with Attempts=0, which always falls through to the
// fallback.
func NewSyncRetrier(cfg *config.SyncRetryConfig) *SyncRetrier {
	r := &SyncRetrier{}
	if cfg == nil {
		return r
	}
	if cfg.Attempts != nil {
		r.Attempts = *cfg.Attempts
	}
	if cfg.Backoff != nil {
		r.Backoff = cfg.Backoff.Std()
	}
	return r
}

// Run attempts fn synchronously up to r.Attempts times, sleeping r.Backoff
// between consecutive failures. If any attempt returns nil, Run returns nil
// without calling fallback. If context is cancelled (before or during an
// inter-attempt sleep), Run returns the context error without calling
// fallback. After all attempts are exhausted, Run calls fallback and returns
// its result.
//
// r.Attempts ≤ 0 causes Run to call fallback immediately, bypassing fn.
func (r *SyncRetrier) Run(ctx context.Context, fn func() error, fallback func() error) error {
	sleep := r.Sleep
	if sleep == nil {
		sleep = contextSleep
	}
	for i := 0; i < r.Attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(); err == nil {
			return nil
		}
		if i < r.Attempts-1 {
			if err := sleep(ctx, r.Backoff); err != nil {
				return err
			}
		}
	}
	return fallback()
}

// contextSleep sleeps for d, honouring context cancellation.
func contextSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
