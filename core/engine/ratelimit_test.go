// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/config"
)

// fakeClock is a deterministic, manually-advanced rateClock. Its zero value is
// unusable; use newFakeClock. It is concurrency-safe so the throughput test can
// advance it while (in principle) other goroutines read it.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	// A fixed, arbitrary epoch — never time.Now(), so tests are reproducible.
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func rate(t *testing.T, s string) *config.Rate {
	t.Helper()
	var r config.Rate
	if err := r.UnmarshalJSON([]byte(`"` + s + `"`)); err != nil {
		t.Fatalf("parse rate %q: %v", s, err)
	}
	return &r
}

// TestLimiterCapsThroughput is the acceptance test: with an injected clock, a
// limiter's sustained throughput is capped to the configured rate. We drain the
// initial burst, then greedily poll Allow while advancing the clock across a
// window and assert the grant count matches rate × window (within one token of
// fractional-refill slack).
func TestLimiterCapsThroughput(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(rate(t, "100/s"), clk) // 100 tokens/sec, burst 100

	// Drain the full initial burst so we measure the refill rate in isolation.
	for i := 0; i < 100; i++ {
		if !l.Allow() {
			t.Fatalf("initial burst: Allow returned false at token %d, want true", i)
		}
	}
	if l.Allow() {
		t.Fatal("bucket should be empty after draining the burst, but Allow returned true")
	}

	// Simulate 10 seconds, stepping 1ms at a time and greedily taking every
	// available token. At 100/s the bucket yields exactly one token per 10ms, so
	// over 10s we expect ~1000 grants.
	const (
		window = 10 * time.Second
		step   = time.Millisecond
	)
	granted := 0
	for elapsed := time.Duration(0); elapsed < window; elapsed += step {
		clk.advance(step)
		for l.Allow() { // drain whatever accrued this step
			granted++
		}
	}

	want := 100 * int(window/time.Second) // 1000
	// Allow one token of slack for boundary fractional accumulation.
	if granted < want-1 || granted > want+1 {
		t.Fatalf("granted %d over %s at 100/s, want ~%d (rate not capped correctly)", granted, window, want)
	}

	// Cross-check the effective rate.
	effective := float64(granted) / window.Seconds()
	if effective < 99 || effective > 101 {
		t.Fatalf("effective rate %.2f/s, want ~100/s", effective)
	}
}

// TestLimiterInitialBurst verifies the bucket starts full: a previously-idle
// worker may spend one period's worth of tokens immediately, then is throttled.
func TestLimiterInitialBurst(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(rate(t, "50/s"), clk)

	for i := 0; i < 50; i++ {
		if !l.Allow() {
			t.Fatalf("burst token %d denied, want allowed", i)
		}
	}
	if l.Allow() {
		t.Fatal("51st immediate token allowed, want denied (burst exhausted)")
	}
}

// TestLimiterRefillIsProportional checks that tokens accrue proportionally to
// elapsed time and never exceed the burst ceiling.
func TestLimiterRefillIsProportional(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(rate(t, "10/s"), clk) // burst 10

	// Drain the burst.
	for i := 0; i < 10; i++ {
		if !l.Allow() {
			t.Fatalf("drain token %d denied", i)
		}
	}

	// After 500ms at 10/s, exactly 5 tokens should be available.
	clk.advance(500 * time.Millisecond)
	got := 0
	for l.Allow() {
		got++
	}
	if got != 5 {
		t.Fatalf("after 500ms at 10/s got %d tokens, want 5", got)
	}

	// Idle far longer than one period: tokens must cap at the burst (10), not
	// accumulate unbounded.
	clk.advance(1 * time.Hour)
	got = 0
	for l.Allow() {
		got++
	}
	if got != 10 {
		t.Fatalf("after long idle got %d tokens, want burst cap 10", got)
	}
}

// TestLimiterAllowN exercises the batch primitive: all-or-nothing consumption.
func TestLimiterAllowN(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(rate(t, "10/s"), clk) // burst 10, starts full

	if !l.AllowN(7) {
		t.Fatal("AllowN(7) from full bucket denied, want allowed")
	}
	// 3 tokens left; asking for 4 must fail without spending anything.
	if l.AllowN(4) {
		t.Fatal("AllowN(4) with 3 tokens allowed, want denied")
	}
	if !l.AllowN(3) {
		t.Fatal("AllowN(3) with 3 tokens denied, want allowed (nothing should have been spent by the failed call)")
	}
	if l.Allow() {
		t.Fatal("bucket should be empty, Allow returned true")
	}
	// n <= 0 is always a no-op success.
	if !l.AllowN(0) || !l.AllowN(-1) {
		t.Fatal("AllowN with n<=0 should return true")
	}
}

// TestLimiterUnlimited verifies that an omitted or degenerate rate imposes no cap.
func TestLimiterUnlimited(t *testing.T) {
	clk := newFakeClock()

	cases := map[string]*Limiter{
		"nil rate":  NewLimiter(nil, clk),
		"zero rate": NewLimiter(&config.Rate{Count: 0, Per: time.Second}, clk),
		"zero per":  NewLimiter(&config.Rate{Count: 100, Per: 0}, clk),
	}
	for name, l := range cases {
		t.Run(name, func(t *testing.T) {
			if !l.Unlimited() {
				t.Fatal("limiter should report Unlimited")
			}
			// No clock advance: an unlimited limiter still admits everything.
			for i := 0; i < 10_000; i++ {
				if !l.Allow() {
					t.Fatalf("unlimited limiter denied at call %d", i)
				}
			}
			if !l.AllowN(1_000_000) {
				t.Fatal("unlimited limiter denied a large AllowN")
			}
		})
	}
}

// TestLimiterSubSecondRate covers a rate expressed over a longer period (60/m),
// exercising the fractional refill path (1 token/sec, burst 60).
func TestLimiterSubSecondRate(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(rate(t, "60/m"), clk) // 1 token/sec, burst 60

	for i := 0; i < 60; i++ {
		if !l.Allow() {
			t.Fatalf("burst token %d denied", i)
		}
	}
	if l.Allow() {
		t.Fatal("token past burst allowed, want denied")
	}
	// One second yields exactly one token.
	clk.advance(time.Second)
	if !l.Allow() {
		t.Fatal("after 1s at 60/m no token, want one")
	}
	if l.Allow() {
		t.Fatal("second token within the same second allowed, want denied")
	}
}

// TestLimiterNilClockUsesWallClock verifies a nil clock is accepted and the
// limiter is usable (it defaults to the real wall clock).
func TestLimiterNilClockUsesWallClock(t *testing.T) {
	l := NewLimiter(rate(t, "5/s"), nil)
	// The bucket starts full, so the first few grants succeed regardless of the
	// (real) clock — we only assert construction and basic operation, not timing.
	if !l.Allow() {
		t.Fatal("nil-clock limiter denied first token from a full bucket")
	}
}

// TestLimiterConcurrentAllow is a race-detector smoke test: concurrent Allow
// calls must not corrupt the token count. Run with -race.
func TestLimiterConcurrentAllow(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(rate(t, "1000/s"), clk) // burst 1000, starts full

	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := 0
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := 0
			for i := 0; i < 1000; i++ {
				if l.Allow() {
					local++
				}
			}
			mu.Lock()
			granted += local
			mu.Unlock()
		}()
	}
	wg.Wait()

	// The clock never advanced, so no refill occurred: total grants must equal
	// exactly the initial burst, proving no tokens were double-spent.
	if granted != 1000 {
		t.Fatalf("concurrent grants totalled %d, want exactly the burst of 1000", granted)
	}
}
