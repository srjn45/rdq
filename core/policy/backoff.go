// SPDX-License-Identifier: Apache-2.0

// Package policy holds the engine's pure decision functions — the retry
// backoff ladder here, outcome classification alongside it (design 03 §2/§4).
// Everything in this package is deterministic given its inputs: the backoff
// takes an injected random source rather than reaching for a global RNG, so the
// engine's retry timing is fully reproducible under test (design 05, "backoff
// and classification are deterministic pure functions with heavy unit
// coverage").
package policy

import (
	"math"
	"time"

	"github.com/srjn45/rdq/core/config"
)

// RNG is the injected source of randomness for backoff jitter. It is satisfied
// by *math/rand.Rand and *math/rand/v2.Rand out of the box, and by a stub in
// tests — the whole point of injecting it is that a test can pin Float64 and
// assert an exact delay (design 03 §2, backlog T3.2).
//
// Float64 must return a value in [0, 1), the contract math/rand already
// guarantees.
type RNG interface {
	Float64() float64
}

// Backoff is the resolved retry-backoff ladder for a queue: concrete values,
// already merged and validated by the config loader (design 03 §2/§3). It is
// deliberately decoupled from *config.RetryConfig's inherit-or-set pointers so
// that Delay stays a pure numeric function; BackoffFromConfig bridges the two.
type Backoff struct {
	// Initial is the base delay before the first retry (n = 1).
	Initial time.Duration
	// Multiplier grows the delay per attempt; 1.0 is linear (constant). Config
	// validation guarantees it is ≥ 1.0.
	Multiplier float64
	// Max caps the pre-jitter delay. A non-positive Max means uncapped.
	Max time.Duration
	// Jitter is the symmetric spread as a fraction of the delay, in [0, 1]: the
	// jittered result lands in base·[1−Jitter, 1+Jitter). Zero disables jitter,
	// making Delay independent of the RNG.
	Jitter float64
}

// Delay computes the wait before retry attempt n (1-based), implementing the
// design's formula (design 03 §2):
//
//	delay(n) = min(initial × multiplier^(n−1), max) × (1 ± jitter·rand)
//
// The cap is applied to the exponential term before jitter, exactly as written,
// so a jittered delay may exceed max by up to the jitter fraction. The
// exponential is evaluated in float64 so a large n saturates to max instead of
// overflowing int64. When Jitter is zero, rng is never consulted and may be nil.
func (b Backoff) Delay(n int, rng RNG) time.Duration {
	base := b.baseDelay(n)
	if b.Jitter <= 0 {
		return base
	}
	// rng.Float64() ∈ [0, 1) maps to a factor in [1−Jitter, 1+Jitter).
	factor := 1 + b.Jitter*(2*rng.Float64()-1)
	d := time.Duration(float64(base) * factor)
	if d < 0 {
		// Only reachable with jitter > 1, which config validation forbids; guard
		// anyway so the engine never schedules a retry in the past.
		return 0
	}
	return d
}

// baseDelay is the pre-jitter min(initial × multiplier^(n−1), max). Attempt
// numbers below 1 are clamped to 1 so the exponent never goes negative.
func (b Backoff) baseDelay(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	scaled := float64(b.Initial) * math.Pow(b.Multiplier, float64(n-1))
	if b.Max > 0 && scaled >= float64(b.Max) {
		return b.Max
	}
	if scaled >= float64(math.MaxInt64) {
		// Uncapped and overflowing (e.g. multiplier^n → +Inf); saturate rather
		// than wrap to a negative duration.
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(scaled)
}

// BackoffFromConfig resolves a RetryConfig's backoff fields into a Backoff. It
// requires the four backoff fields to be present: the config loader's
// deep-merge fills them from defaults, and a queue whose effective retry block
// omits one has no defined ladder, so the engine — not this function — must
// decide that policy. ok is false when any required field is nil.
func BackoffFromConfig(rc *config.RetryConfig) (b Backoff, ok bool) {
	if rc == nil ||
		rc.InitialBackoff == nil ||
		rc.BackoffMultiplier == nil ||
		rc.MaxBackoff == nil ||
		rc.Jitter == nil {
		return Backoff{}, false
	}
	return Backoff{
		Initial:    rc.InitialBackoff.Std(),
		Multiplier: *rc.BackoffMultiplier,
		Max:        rc.MaxBackoff.Std(),
		Jitter:     *rc.Jitter,
	}, true
}
