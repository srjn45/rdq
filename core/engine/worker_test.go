// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/memstore"
	"github.com/srjn45/rdq/core/policy"
	"github.com/srjn45/rdq/core/registry"
	"github.com/srjn45/rdq/core/spi"
)

// --- test doubles -----------------------------------------------------------

// workerClock is the worker's injected clock for tests: a logical Now() that the
// test advances by hand (shared with the store so due-ness and backoff move in
// lockstep), backed by REAL timers/tickers so the poll loop keeps cycling on a
// fast wall-clock cadence regardless of logical time. This cleanly separates
// "what time is it" (deterministic, test-controlled) from "how often does the
// loop wake" (real, fast) — so assertions are deterministic while the loop stays
// responsive.
type workerClock struct {
	mu  sync.Mutex
	now time.Time
}

func newWorkerClock() *workerClock {
	return &workerClock{now: time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)}
}

func (c *workerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *workerClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *workerClock) NewTimer(d time.Duration) Timer   { return realTimer{time.NewTimer(d)} }
func (c *workerClock) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

// fixedRNG is a deterministic RNG. Returning 0.5 makes the jitter factor exactly
// 1.0 (no net jitter), so backoff delays are the pure base ladder.
type fixedRNG struct{}

func (fixedRNG) Float64() float64 { return 0.5 }

// blockingHandler runs a per-invocation function. started is signalled once per
// call so a test can wait until a handler is actually executing.
type testHandler struct {
	version string
	fn      func(ctx context.Context, task envelope.Envelope) error
}

func (h *testHandler) Version() string { return h.version }
func (h *testHandler) Handle(ctx context.Context, task envelope.Envelope) error {
	return h.fn(ctx, task)
}

// --- helpers ----------------------------------------------------------------

const testQueue = "orders.process"

// eventually polls cond until it is true or the deadline elapses. It bridges the
// real-time gap between advancing the logical clock and a handler goroutine
// observing the change; the logical *results* remain deterministic.
func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// submit enqueues a fresh PENDING task due at the clock's current logical now.
func submit(t *testing.T, store *memstore.Store, clk *workerClock, id string) {
	t.Helper()
	now := clk.Now()
	err := store.Enqueue(context.Background(), envelope.Envelope{
		EnvelopeVersion:    1,
		ID:                 id,
		Queue:              testQueue,
		HandlerRef:         "h",
		Payload:            []byte("{}"),
		PayloadContentType: "application/json",
		Status:             envelope.StatusPending,
		NextAttemptAt:      &now,
		CreatedAt:          now,
	})
	if err != nil {
		t.Fatalf("enqueue %s: %v", id, err)
	}
}

func getTask(t *testing.T, store *memstore.Store, id string) envelope.Envelope {
	t.Helper()
	env, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return env
}

// baseSpec is a fast, deterministic QueueSpec: short logical lease, a 2ms poll so
// the loop re-polls promptly, and a simple 1s×2 backoff ladder.
func baseSpec() QueueSpec {
	return QueueSpec{
		Queue:          testQueue,
		MaxAttempts:    3,
		Backoff:        policy.Backoff{Initial: time.Second, Multiplier: 2, Max: time.Hour, Jitter: 0},
		Classifier:     policy.Classifier{},
		Lease:          200 * time.Millisecond,
		HandlerTimeout: 200 * time.Millisecond,
		BatchSize:      8,
		Concurrency:    4,
		PollInterval:   2 * time.Millisecond,
		VersionPolicy:  registry.PolicyRunLatest,
	}
}

// runWorker starts a worker in the background and returns a cancel func plus a
// channel that closes when Run returns.
func runWorker(t *testing.T, w *Worker) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()
	return cancel, done
}

func newTestWorker(t *testing.T, store *memstore.Store, reg *registry.Registry, clk *workerClock, specs []QueueSpec, opts ...Option) *Worker {
	t.Helper()
	all := append([]Option{WithClock(clk), WithRNG(fixedRNG{}), WithSweepInterval(0)}, opts...)
	w, err := NewWorker(store, reg, specs, all...)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	return w
}

// --- tests ------------------------------------------------------------------

// TestSubmitRetrySucceed is the full happy-path loop: a handler that fails twice
// then succeeds should drive the task through two backoff-scheduled retries to
// SUCCEEDED, with the attempt history to match.
func TestSubmitRetrySucceed(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New()

	var calls int
	var mu sync.Mutex
	if err := reg.Register("h", &testHandler{fn: func(ctx context.Context, task envelope.Envelope) error {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n < 3 {
			return errors.New("transient")
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	w := newTestWorker(t, store, reg, clk, []QueueSpec{baseSpec()})
	cancel, done := runWorker(t, w)
	defer func() { cancel(); <-done }()

	submit(t, store, clk, "t1")

	// Attempt 1 fails → rescheduled +1s (initial backoff).
	eventually(t, func() bool { return getTask(t, store, "t1").AttemptCount == 1 })
	if got := getTask(t, store, "t1"); got.Status != envelope.StatusPending {
		t.Fatalf("after attempt 1: status %s, want PENDING", got.Status)
	}

	// Advance past the first backoff → attempt 2 fails → rescheduled +2s.
	clk.advance(time.Second)
	eventually(t, func() bool { return getTask(t, store, "t1").AttemptCount == 2 })

	// Advance past the second backoff → attempt 3 succeeds.
	clk.advance(2 * time.Second)
	eventually(t, func() bool { return getTask(t, store, "t1").Status == envelope.StatusSucceeded })

	got := getTask(t, store, "t1")
	if got.AttemptCount != 3 {
		t.Fatalf("attempt_count = %d, want 3", got.AttemptCount)
	}
	wantOutcomes := []envelope.Outcome{
		envelope.OutcomeRetryableFailure,
		envelope.OutcomeRetryableFailure,
		envelope.OutcomeSuccess,
	}
	if len(got.Attempts) != len(wantOutcomes) {
		t.Fatalf("recorded %d attempts, want %d", len(got.Attempts), len(wantOutcomes))
	}
	for i, o := range wantOutcomes {
		if got.Attempts[i].Outcome != o {
			t.Errorf("attempt %d outcome = %s, want %s", i+1, got.Attempts[i].Outcome, o)
		}
	}
}

// TestSubmitExhaustDeadLetter is the exhaustion loop: a handler that always fails
// (retryably) should be dead-lettered once it hits max_attempts.
func TestSubmitExhaustDeadLetter(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New()
	if err := reg.Register("h", &testHandler{fn: func(ctx context.Context, task envelope.Envelope) error {
		return errors.New("always fails")
	}}); err != nil {
		t.Fatal(err)
	}

	spec := baseSpec()
	spec.MaxAttempts = 2
	w := newTestWorker(t, store, reg, clk, []QueueSpec{spec})
	cancel, done := runWorker(t, w)
	defer func() { cancel(); <-done }()

	submit(t, store, clk, "t1")

	// Attempt 1 fails → rescheduled.
	eventually(t, func() bool { return getTask(t, store, "t1").AttemptCount == 1 })

	// Advance past the backoff → attempt 2 fails → exhausted → DLQ.
	clk.advance(time.Second)
	eventually(t, func() bool { return getTask(t, store, "t1").Status == envelope.StatusDead })

	got := getTask(t, store, "t1")
	if got.AttemptCount != 2 {
		t.Fatalf("attempt_count = %d, want 2", got.AttemptCount)
	}
	for i, a := range got.Attempts {
		if a.Outcome != envelope.OutcomeRetryableFailure {
			t.Errorf("attempt %d outcome = %s, want RETRYABLE_FAILURE", i+1, a.Outcome)
		}
	}
}

// TestRedriveContinuesAttemptHistory pins the attempt-numbering split (the T5.7
// regression): a redrive resets attempt_count (the retry BUDGET) to 0 but keeps
// the attempt history (invariant 7), so a task re-run after a redrive must number
// its new attempt PAST the preserved ones — attempt_no is a per-task monotonic
// sequence under a UNIQUE(task_id, attempt_no) constraint in real storage.
// Numbering it from the reset budget (attempt_count+1) would re-emit an existing
// attempt_no and, against Postgres, fail the outcome write and wedge the task
// IN_FLIGHT. Here we assert the engine records the *continued* history number
// while the budget still resets (a fresh, non-exhausted attempt).
func TestRedriveContinuesAttemptHistory(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New()

	var mu sync.Mutex
	failing := true
	if err := reg.Register("h", &testHandler{fn: func(ctx context.Context, task envelope.Envelope) error {
		mu.Lock()
		defer mu.Unlock()
		if failing {
			return errors.New("transient")
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	spec := baseSpec()
	spec.MaxAttempts = 2
	w := newTestWorker(t, store, reg, clk, []QueueSpec{spec})
	cancel, done := runWorker(t, w)
	defer func() { cancel(); <-done }()

	// Drive the task to the DLQ: attempt 1 fails, attempt 2 exhausts → DEAD with
	// attempts numbered 1 and 2.
	submit(t, store, clk, "t1")
	eventually(t, func() bool { return getTask(t, store, "t1").AttemptCount == 1 })
	clk.advance(time.Second)
	eventually(t, func() bool { return getTask(t, store, "t1").Status == envelope.StatusDead })
	if got := getTask(t, store, "t1"); len(got.Attempts) != 2 {
		t.Fatalf("pre-redrive attempts = %d, want 2", len(got.Attempts))
	}

	// Redrive resets the budget to 0 but keeps attempts 1 and 2, then let the
	// handler succeed on the re-run.
	mu.Lock()
	failing = false
	mu.Unlock()
	if n, err := store.Redrive(context.Background(), testQueue, spi.Selector{IDs: []string{"t1"}}); err != nil || n != 1 {
		t.Fatalf("Redrive = (%d, %v), want (1, nil)", n, err)
	}

	eventually(t, func() bool { return getTask(t, store, "t1").Status == envelope.StatusSucceeded })

	got := getTask(t, store, "t1")
	// Budget reset: a single successful attempt since the redrive.
	if got.AttemptCount != 1 {
		t.Errorf("post-redrive attempt_count = %d, want 1 (budget reset)", got.AttemptCount)
	}
	// History continued: the new attempt is numbered 3, not a duplicate 1.
	if len(got.Attempts) != 3 {
		t.Fatalf("total attempts = %d, want 3 (2 preserved + 1 new)", len(got.Attempts))
	}
	for i, want := range []int{1, 2, 3} {
		if got.Attempts[i].AttemptNo != want {
			t.Errorf("attempts[%d].attempt_no = %d, want %d (monotonic, no collision)", i, got.Attempts[i].AttemptNo, want)
		}
	}
	if last := got.Attempts[2]; last.Outcome != envelope.OutcomeSuccess {
		t.Errorf("final attempt outcome = %s, want SUCCESS", last.Outcome)
	}
}

// TestPermanentFailureDeadLetters verifies the classification ladder short-circuits
// retries: a permanently-classified error dead-letters on the first attempt.
func TestPermanentFailureDeadLetters(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New()
	permErr := errors.New("bad input")
	if err := reg.Register("h", &testHandler{fn: func(ctx context.Context, task envelope.Envelope) error {
		return policy.Permanent(permErr)
	}}); err != nil {
		t.Fatal(err)
	}

	w := newTestWorker(t, store, reg, clk, []QueueSpec{baseSpec()})
	cancel, done := runWorker(t, w)
	defer func() { cancel(); <-done }()

	submit(t, store, clk, "t1")
	eventually(t, func() bool { return getTask(t, store, "t1").Status == envelope.StatusDead })

	got := getTask(t, store, "t1")
	if got.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1 (no retries for a permanent failure)", got.AttemptCount)
	}
	if got.Attempts[0].Outcome != envelope.OutcomePermanentFailure {
		t.Fatalf("attempt outcome = %s, want PERMANENT_FAILURE", got.Attempts[0].Outcome)
	}
}

// TestUnroutableDeadLetters verifies a task with no registered handler is
// dead-lettered immediately (never rescheduled — no hot loop).
func TestUnroutableDeadLetters(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New() // no handler registered for "h"

	w := newTestWorker(t, store, reg, clk, []QueueSpec{baseSpec()})
	cancel, done := runWorker(t, w)
	defer func() { cancel(); <-done }()

	submit(t, store, clk, "t1")
	eventually(t, func() bool { return getTask(t, store, "t1").Status == envelope.StatusDead })

	got := getTask(t, store, "t1")
	if got.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1", got.AttemptCount)
	}
	if got.Attempts[0].Error == nil || got.Attempts[0].Error.Type != registry.ErrorTypeUnroutable {
		t.Fatalf("dead-letter error type = %v, want %s", got.Attempts[0].Error, registry.ErrorTypeUnroutable)
	}
}

// TestLeaseOverrunReclaimAndAbandon exercises the fencing path: while a handler
// runs past its lease, the store reclaims the task (recording LEASE_EXPIRED) under
// a fresh token; when the original handler finally reports its outcome, the write
// is rejected with ErrStaleClaim and the worker abandons the item.
func TestLeaseOverrunReclaimAndAbandon(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New()

	release := make(chan struct{})
	handlerRunning := make(chan struct{}, 1)
	if err := reg.Register("h", &testHandler{fn: func(ctx context.Context, task envelope.Envelope) error {
		select {
		case handlerRunning <- struct{}{}:
		default:
		}
		<-release // ignore ctx: run past the lease, the poison-pill case
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	var abandoned []error
	var mu sync.Mutex
	spec := baseSpec()
	spec.Concurrency = 1 // the single slot stays occupied by the runaway handler
	w := newTestWorker(t, store, reg, clk, []QueueSpec{spec},
		WithAbandonHook(func(task envelope.Envelope, err error) {
			mu.Lock()
			abandoned = append(abandoned, err)
			mu.Unlock()
		}))
	cancel, done := runWorker(t, w)
	defer func() { cancel(); <-done }()

	submit(t, store, clk, "t1")
	<-handlerRunning // the worker has claimed t1 and the handler is executing

	// Expire the lease and reclaim from "another worker" (a direct store claim):
	// this invalidates the running handler's token and records LEASE_EXPIRED.
	clk.advance(spec.Lease + time.Millisecond)
	reclaimed, err := store.ClaimDue(context.Background(), testQueue, 1, spec.Lease)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("reclaim returned %d tasks, want 1", len(reclaimed))
	}

	// Let the original handler finish; its Complete must be fenced off.
	close(release)
	eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(abandoned) == 1
	})

	mu.Lock()
	gotErr := abandoned[0]
	mu.Unlock()
	if !errors.Is(gotErr, spi.ErrStaleClaim) {
		t.Fatalf("abandon error = %v, want ErrStaleClaim", gotErr)
	}

	got := getTask(t, store, "t1")
	if got.Status == envelope.StatusSucceeded {
		t.Fatal("task marked SUCCEEDED by a stale claim — fencing failed")
	}
	var sawLeaseExpired bool
	for _, a := range got.Attempts {
		if a.Outcome == envelope.OutcomeLeaseExpired {
			sawLeaseExpired = true
		}
	}
	if !sawLeaseExpired {
		t.Fatal("no LEASE_EXPIRED attempt recorded on reclaim")
	}
}

// TestGracefulDrain verifies G10: after stop, the worker claims no new tasks but
// lets the in-flight handler finish (its outcome is still persisted).
func TestGracefulDrain(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New()

	release := make(chan struct{})
	handlerRunning := make(chan struct{}, 1)
	if err := reg.Register("h", &testHandler{fn: func(ctx context.Context, task envelope.Envelope) error {
		select {
		case handlerRunning <- struct{}{}:
		default:
		}
		<-release
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	spec := baseSpec()
	spec.Concurrency = 1
	spec.Lease = 5 * time.Second // long enough that drain waits on the handler, not the timeout
	spec.HandlerTimeout = 5 * time.Second
	w := newTestWorker(t, store, reg, clk, []QueueSpec{spec})

	ctx, cancel := context.WithCancel(context.Background())
	runReturned := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(runReturned)
	}()

	submit(t, store, clk, "inflight")
	<-handlerRunning // "inflight" is claimed and executing

	// Stop the worker, then submit a second task it must NOT claim.
	cancel()
	submit(t, store, clk, "after-stop")

	// Run must not return while the in-flight handler is still blocked.
	select {
	case <-runReturned:
		t.Fatal("Run returned before the in-flight handler finished (drain did not wait)")
	case <-time.After(50 * time.Millisecond):
	}

	// Let the in-flight handler finish; drain completes and Run returns.
	close(release)
	select {
	case <-runReturned:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the in-flight handler finished")
	}

	if got := getTask(t, store, "inflight"); got.Status != envelope.StatusSucceeded {
		t.Fatalf("in-flight task status = %s, want SUCCEEDED (drain must let it finish)", got.Status)
	}
	if got := getTask(t, store, "after-stop"); got.Status != envelope.StatusPending {
		t.Fatalf("post-stop task status = %s, want PENDING (no new claims after stop)", got.Status)
	}
}

// TestHeartbeatKeepsLease verifies that with heartbeat enabled, a long-running
// handler's lease is extended so the task is not reclaimed mid-run.
func TestHeartbeatKeepsLease(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New()

	release := make(chan struct{})
	handlerRunning := make(chan struct{}, 1)
	if err := reg.Register("h", &testHandler{fn: func(ctx context.Context, task envelope.Envelope) error {
		select {
		case handlerRunning <- struct{}{}:
		default:
		}
		<-release
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	spec := baseSpec()
	spec.Concurrency = 1
	spec.Lease = 30 * time.Millisecond // short lease so heartbeats (lease/3) fire fast in real time
	spec.HandlerTimeout = 30 * time.Millisecond
	spec.Heartbeat = true
	w := newTestWorker(t, store, reg, clk, []QueueSpec{spec})
	cancel, done := runWorker(t, w)
	defer func() { cancel(); <-done }()

	submit(t, store, clk, "t1")
	<-handlerRunning

	// The lease-expiry timestamp at claim time. With heartbeat OFF this never
	// moves; with it ON, each ExtendLease sets it to (logical now + lease).
	initial := getTask(t, store, "t1").LeaseExpiresAt
	if initial == nil {
		t.Fatal("claimed task has no lease_expires_at")
	}
	l0 := *initial

	// Advance logical time well past the original lease. The heartbeat (lease/3,
	// firing on the real clock) must push lease_expires_at into the new future —
	// the direct signal that the lease is being kept alive.
	clk.advance(3 * spec.Lease)
	eventually(t, func() bool {
		le := getTask(t, store, "t1").LeaseExpiresAt
		return le != nil && le.After(l0)
	})

	close(release)
	eventually(t, func() bool { return getTask(t, store, "t1").Status == envelope.StatusSucceeded })

	// A kept-alive lease is never reclaimed, so no LEASE_EXPIRED is recorded.
	for _, a := range getTask(t, store, "t1").Attempts {
		if a.Outcome == envelope.OutcomeLeaseExpired {
			t.Fatal("LEASE_EXPIRED recorded despite an active heartbeat")
		}
	}
}

// TestSweeperPurgesSucceeded verifies G19: the jittered sweeper removes SUCCEEDED
// tasks older than the queue's retention window.
func TestSweeperPurgesSucceeded(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New()
	if err := reg.Register("h", &testHandler{fn: func(ctx context.Context, task envelope.Envelope) error {
		return nil // succeed immediately
	}}); err != nil {
		t.Fatal(err)
	}

	spec := baseSpec()
	spec.TTLSucceeded = time.Minute
	// Enable the sweeper on a fast real cadence; its logical age check uses the
	// fake clock.
	w := newTestWorker(t, store, reg, clk, []QueueSpec{spec},
		WithSweepInterval(2*time.Millisecond), WithSweepJitter(0))
	cancel, done := runWorker(t, w)
	defer func() { cancel(); <-done }()

	submit(t, store, clk, "t1")
	eventually(t, func() bool { return getTask(t, store, "t1").Status == envelope.StatusSucceeded })

	// Within the retention window: the sweeper must NOT purge it.
	time.Sleep(20 * time.Millisecond)
	if _, err := store.Get(context.Background(), "t1"); err != nil {
		t.Fatalf("task purged while still within retention: %v", err)
	}

	// Advance logical time past the TTL; the next sweep tick purges it.
	clk.advance(2 * time.Minute)
	eventually(t, func() bool {
		_, err := store.Get(context.Background(), "t1")
		return errors.Is(err, spi.ErrNotFound)
	})
}

// TestVersionMismatchDeadLetters verifies the dead-letter version policy: a task
// pinned to a version the registered handler does not provide is dead-lettered
// with the version-mismatch error class.
func TestVersionMismatchDeadLetters(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New()
	if err := reg.Register("h", &testHandler{version: "v2", fn: func(ctx context.Context, task envelope.Envelope) error {
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	spec := baseSpec()
	spec.VersionPolicy = registry.PolicyDeadLetter
	w := newTestWorker(t, store, reg, clk, []QueueSpec{spec})
	cancel, done := runWorker(t, w)
	defer func() { cancel(); <-done }()

	now := clk.Now()
	if err := store.Enqueue(context.Background(), envelope.Envelope{
		EnvelopeVersion: 1, ID: "t1", Queue: testQueue, HandlerRef: "h", HandlerVersion: "v1",
		Payload: []byte("{}"), PayloadContentType: "application/json",
		Status: envelope.StatusPending, NextAttemptAt: &now, CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	eventually(t, func() bool { return getTask(t, store, "t1").Status == envelope.StatusDead })
	got := getTask(t, store, "t1")
	if got.Attempts[0].Error == nil || got.Attempts[0].Error.Type != registry.ErrorTypeVersionMismatch {
		t.Fatalf("dead-letter error type = %v, want %s", got.Attempts[0].Error, registry.ErrorTypeVersionMismatch)
	}
}

// TestNewWorkerValidation covers the constructor's guard rails.
func TestNewWorkerValidation(t *testing.T) {
	reg := registry.New()
	store := memstore.New()
	if _, err := NewWorker(nil, reg, nil); err == nil {
		t.Error("nil store accepted, want error")
	}
	if _, err := NewWorker(store, nil, nil); err == nil {
		t.Error("nil registry accepted, want error")
	}
	if _, err := NewWorker(store, reg, []QueueSpec{{Queue: ""}}); err == nil {
		t.Error("empty queue name accepted, want error")
	}
	if _, err := NewWorker(store, reg, []QueueSpec{{Queue: "q", Lease: 0}}); err == nil {
		t.Error("non-positive lease accepted, want error")
	}
}
