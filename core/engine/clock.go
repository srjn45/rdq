// SPDX-License-Identifier: Apache-2.0

package engine

import "time"

// Clock is the package's single injectable time source for the worker runtime
// (T3.6). It is the authority the worker uses for *local* scheduling — poll
// cadence, handler timeouts, heartbeat intervals, the sweeper tick — and for the
// logical "now" it stamps on attempt records and feeds into backoff. It is
// deliberately a superset of the limiter's minimal rateClock (ratelimit.go): a
// Clock satisfies rateClock, so the worker hands its Clock straight to NewLimiter
// and the whole package shares one clock rather than two competing ones.
//
// Note this local clock is distinct from the storage backend's clock, which
// remains the sole authority for due-ness and lease expiry (G9, design 02). In
// production both are the real wall clock; a deterministic test injects one fake
// clock into both the worker (here) and the store (memstore.WithClock) so logical
// time advances in lockstep.
type Clock interface {
	// Now reports the current logical time.
	Now() time.Time
	// NewTimer returns a Timer that fires once after d.
	NewTimer(d time.Duration) Timer
	// NewTicker returns a Ticker that fires every d until stopped.
	NewTicker(d time.Duration) Ticker
}

// Timer is a one-shot timer, mirroring the slice of *time.Timer the worker uses.
type Timer interface {
	// C is the channel on which the tick is delivered.
	C() <-chan time.Time
	// Stop halts the timer, reporting whether it did so before it fired.
	Stop() bool
}

// Ticker is a repeating timer, mirroring the slice of *time.Ticker the worker
// uses.
type Ticker interface {
	// C is the channel on which ticks are delivered.
	C() <-chan time.Time
	// Stop halts the ticker; its channel is not drained or closed.
	Stop()
}

// systemClock — declared in ratelimit.go as the default rateClock — is extended
// here into the full production Clock. Keeping the type in one place means the
// package has exactly one real-clock implementation backing both the limiter and
// the worker.
var _ Clock = systemClock{}

// NewTimer returns a real time.Timer wrapper.
func (systemClock) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

// NewTicker returns a real time.Ticker wrapper.
func (systemClock) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

// realTimer adapts *time.Timer to Timer (exposing its C field as a method).
type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }
func (r realTimer) Stop() bool          { return r.t.Stop() }

// realTicker adapts *time.Ticker to Ticker.
type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }
