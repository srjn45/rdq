// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/config"
)

// fixedRNG returns a preset sequence of Float64 values, one per call, so a test
// can pin jitter to an exact factor and assert an exact delay. It panics if the
// sequence is exhausted — a drained stub means the test under-specified its
// randomness, which should fail loudly rather than silently reuse a value.
type fixedRNG struct {
	vals []float64
	i    int
}

func (r *fixedRNG) Float64() float64 {
	if r.i >= len(r.vals) {
		panic("fixedRNG: sequence exhausted")
	}
	v := r.vals[r.i]
	r.i++
	return v
}

// TestBackoffBaseLadder checks the pre-jitter exponential and its cap with
// jitter disabled, so the delay is fully deterministic without an RNG.
func TestBackoffBaseLadder(t *testing.T) {
	b := Backoff{
		Initial:    time.Second,
		Multiplier: 2.0,
		Max:        10 * time.Minute,
		Jitter:     0,
	}
	cases := []struct {
		n    int
		want time.Duration
	}{
		{n: 0, want: time.Second},     // clamped to n=1, exponent 0
		{n: 1, want: time.Second},     // initial × 2^0
		{n: 2, want: 2 * time.Second}, // initial × 2^1
		{n: 3, want: 4 * time.Second},
		{n: 4, want: 8 * time.Second},
		{n: 10, want: 512 * time.Second},
		{n: 11, want: 10 * time.Minute}, // 1024s > 600s cap → capped
		{n: 50, want: 10 * time.Minute}, // deep in the cap
	}
	for _, c := range cases {
		if got := b.Delay(c.n, nil); got != c.want {
			t.Errorf("Delay(%d) = %s, want %s", c.n, got, c.want)
		}
	}
}

// TestBackoffLinear verifies that multiplier 1.0 is a constant (linear) ladder.
func TestBackoffLinear(t *testing.T) {
	b := Backoff{Initial: 100 * time.Millisecond, Multiplier: 1.0, Max: time.Minute}
	for n := 1; n <= 5; n++ {
		if got := b.Delay(n, nil); got != 100*time.Millisecond {
			t.Errorf("Delay(%d) = %s, want 100ms", n, got)
		}
	}
}

// TestBackoffJitterBounds injects the RNG extremes (0 and just below 1) plus the
// midpoint and asserts the jittered delay hits exactly base·(1−jitter),
// base·(1+jitter·(2r−1)), and base at r=0.5.
func TestBackoffJitterBounds(t *testing.T) {
	b := Backoff{
		Initial:    time.Second,
		Multiplier: 2.0,
		Max:        time.Hour,
		Jitter:     0.2,
	}
	// n = 3 → base = 1s × 2^2 = 4s.
	const base = 4 * time.Second
	cases := []struct {
		r    float64
		want time.Duration
	}{
		{r: 0.0, want: time.Duration(float64(base) * 0.8)}, // factor 1 − 0.2
		{r: 0.5, want: base}, // factor 1.0
		{r: 1.0, want: time.Duration(float64(base) * 1.2)}, // factor 1 + 0.2 (upper bound)
	}
	for _, c := range cases {
		rng := &fixedRNG{vals: []float64{c.r}}
		if got := b.Delay(3, rng); got != c.want {
			t.Errorf("Delay(3) with r=%.2f = %s, want %s", c.r, got, c.want)
		}
	}
}

// TestBackoffJitterStaysInBounds is a property check: across a full RNG range,
// every jittered delay stays within base·[1−jitter, 1+jitter].
func TestBackoffJitterStaysInBounds(t *testing.T) {
	b := Backoff{Initial: 250 * time.Millisecond, Multiplier: 1.5, Max: time.Hour, Jitter: 0.3}
	rng := rand.New(rand.NewSource(1)) // seeded → reproducible
	for n := 1; n <= 8; n++ {
		base := b.baseDelay(n)
		lo := time.Duration(float64(base) * (1 - b.Jitter))
		hi := time.Duration(float64(base) * (1 + b.Jitter))
		for i := 0; i < 1000; i++ {
			d := b.Delay(n, rng)
			if d < lo || d > hi {
				t.Fatalf("Delay(%d) = %s out of bounds [%s, %s]", n, d, lo, hi)
			}
		}
	}
}

// TestBackoffJitterSkipsRNG confirms that zero jitter never consults the RNG:
// passing a stub that would panic on use must not panic.
func TestBackoffJitterSkipsRNG(t *testing.T) {
	b := Backoff{Initial: time.Second, Multiplier: 2.0, Max: time.Minute, Jitter: 0}
	rng := &fixedRNG{} // empty — any Float64 call panics
	if got := b.Delay(4, rng); got != 8*time.Second {
		t.Errorf("Delay(4) = %s, want 8s", got)
	}
}

// TestBackoffUncapped checks that a non-positive Max means no cap, and that a
// runaway exponent saturates to MaxInt64 rather than overflowing negative.
func TestBackoffUncapped(t *testing.T) {
	b := Backoff{Initial: time.Second, Multiplier: 2.0, Max: 0}
	if got := b.Delay(4, nil); got != 8*time.Second {
		t.Errorf("Delay(4) uncapped = %s, want 8s", got)
	}
	// n large enough that initial × 2^(n−1) overflows int64 nanoseconds.
	if got := b.Delay(200, nil); got != time.Duration(math.MaxInt64) {
		t.Errorf("Delay(200) uncapped = %s, want saturated MaxInt64", got)
	}
}

// TestBackoffFromConfig bridges a resolved RetryConfig into a Backoff and checks
// the ok gate on missing fields.
func TestBackoffFromConfig(t *testing.T) {
	initial := config.Duration(time.Second)
	maxb := config.Duration(10 * time.Minute)
	mult := 2.0
	jit := 0.2
	rc := &config.RetryConfig{
		InitialBackoff:    &initial,
		BackoffMultiplier: &mult,
		MaxBackoff:        &maxb,
		Jitter:            &jit,
	}
	b, ok := BackoffFromConfig(rc)
	if !ok {
		t.Fatal("BackoffFromConfig: ok = false for a complete config")
	}
	want := Backoff{Initial: time.Second, Multiplier: 2.0, Max: 10 * time.Minute, Jitter: 0.2}
	if b != want {
		t.Errorf("BackoffFromConfig = %+v, want %+v", b, want)
	}
	// A resolved delay through the bridge matches a hand-built ladder; r=0.5
	// pins jitter to the identity factor so base (1s × 2^2 = 4s) shows through.
	if got := b.Delay(3, &fixedRNG{vals: []float64{0.5}}); got != 4*time.Second {
		t.Errorf("bridged Delay(3) = %s, want 4s", got)
	}

	// Missing any backoff field → not ok.
	for _, drop := range []func(*config.RetryConfig){
		func(c *config.RetryConfig) { c.InitialBackoff = nil },
		func(c *config.RetryConfig) { c.BackoffMultiplier = nil },
		func(c *config.RetryConfig) { c.MaxBackoff = nil },
		func(c *config.RetryConfig) { c.Jitter = nil },
	} {
		i2, m2, x2, j2 := initial, mult, maxb, jit
		partial := &config.RetryConfig{
			InitialBackoff:    &i2,
			BackoffMultiplier: &m2,
			MaxBackoff:        &x2,
			Jitter:            &j2,
		}
		drop(partial)
		if _, ok := BackoffFromConfig(partial); ok {
			t.Error("BackoffFromConfig: ok = true with a missing field")
		}
	}
	if _, ok := BackoffFromConfig(nil); ok {
		t.Error("BackoffFromConfig(nil): ok = true")
	}
}
