// SPDX-License-Identifier: Apache-2.0

// Package engine holds the worker-side execution machinery: the pieces that turn
// a resolved queue config into paced, leased, retried handler invocations. This
// file is the rate-limiting gate (design 03 §0/§3, T3.5).
package engine

import (
	"sync"
	"time"

	"github.com/srjn45/rdq/core/config"
)

// rateClock is the minimal time source the limiter depends on: just "what time
// is it now". It is deliberately tiny and unexported so it stays self-contained
// — the worker runtime (T3.6) may introduce its own richer clock abstraction in
// this package, and this interface must not collide with it. A nil clock passed
// to NewLimiter falls back to the real wall clock.
//
// Note this is a *local, per-instance* clock for pacing invocations, distinct
// from the storage backend's clock, which remains the authority for due-ness and
// lease expiry (G9, design 02). Rate limiting is coordination-free and per
// instance (G12), so a plain in-process clock is exactly right here.
type rateClock interface {
	Now() time.Time
}

// systemClock is the default rateClock: the real monotonic wall clock.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Limiter is a per-queue token-bucket gate over handler/callback invocations,
// configured from worker.rate_limit (design 03 §2, G12). It is the safety valve
// for bulk redrive after an outage — the thundering-herd case (FR-17) — capping
// the rate at which a single instance hammers a struggling downstream.
//
// The bucket refills continuously at the configured rate and holds at most one
// period's worth of tokens (the burst), so a caller may spend an idle-accrued
// burst immediately but its sustained throughput is pinned to the rate. Semantics
// are per instance: N instances yield an effective global rate of N × rate_limit
// (G12, documented loudly in design 03 §3 as a deliberate footgun).
//
// A Limiter with no configured rate (nil, or a non-positive rate) is unlimited:
// every Allow succeeds. A Limiter is safe for concurrent use.
type Limiter struct {
	// ratePerSec is the sustained refill rate in tokens per second. Zero means
	// unlimited — the bucket machinery is bypassed entirely.
	ratePerSec float64
	// burst is the bucket capacity: the most tokens that can accrue while idle.
	burst float64
	clock rateClock

	mu     sync.Mutex
	tokens float64   // current tokens, in [0, burst]
	last   time.Time // clock time at which tokens was last recomputed
}

// NewLimiter builds a per-queue Limiter from a resolved worker.rate_limit value.
//
// A nil rate — the config field was omitted — or any non-positive rate yields an
// unlimited limiter whose Allow always succeeds; this matches the config
// contract, where an absent rate_limit means "no cap" (units.go, Rate doc). The
// bucket starts full so a freshly started, previously idle worker may burst up to
// one period's worth of tokens before settling to the sustained rate.
//
// clk is the injectable time source; pass nil in production to use the real wall
// clock, or a fake in tests for deterministic timing.
func NewLimiter(rate *config.Rate, clk rateClock) *Limiter {
	if clk == nil {
		clk = systemClock{}
	}
	l := &Limiter{clock: clk, last: clk.Now()}
	if rate == nil {
		return l // unlimited
	}
	perSec := rate.PerSecond()
	if perSec <= 0 {
		return l // unlimited: a zero/degenerate rate is treated as no cap
	}
	l.ratePerSec = perSec
	// Burst = one period's worth of tokens (the written count), never below 1 so
	// a rate like 1/h can still admit a single task. Start full.
	l.burst = float64(rate.Count)
	if l.burst < 1 {
		l.burst = 1
	}
	l.tokens = l.burst
	return l
}

// Unlimited reports whether this limiter imposes no cap (no rate configured).
func (l *Limiter) Unlimited() bool { return l.ratePerSec == 0 }

// Allow reports whether one invocation may proceed now, consuming a token if so.
// It is the non-blocking primitive the worker loop polls before dispatching a
// task; a false result means the caller should back off and retry later. An
// unlimited limiter always returns true.
func (l *Limiter) Allow() bool { return l.AllowN(1) }

// AllowN reports whether n invocations may proceed now, consuming n tokens if so.
// It is all-or-nothing: if fewer than n tokens are available no tokens are spent
// and it returns false. n <= 0 is a no-op that returns true. An unlimited limiter
// always returns true.
func (l *Limiter) AllowN(n int) bool {
	if n <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ratePerSec == 0 {
		return true // unlimited
	}
	l.refillLocked()
	need := float64(n)
	if l.tokens < need {
		return false
	}
	l.tokens -= need
	return true
}

// refillLocked advances the bucket to the current clock time, crediting tokens
// for elapsed time up to the burst ceiling. The caller must hold l.mu.
//
// A non-monotonic clock reading (now before last, e.g. a fake clock rewound in a
// test) credits nothing and does not rewind the bucket; last only ever moves
// forward.
func (l *Limiter) refillLocked() {
	now := l.clock.Now()
	elapsed := now.Sub(l.last)
	if elapsed <= 0 {
		return
	}
	l.last = now
	l.tokens += elapsed.Seconds() * l.ratePerSec
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
}
