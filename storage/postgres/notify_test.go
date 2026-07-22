// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// listenSettle is a short pause giving the WaitDue goroutine time to establish its
// LISTEN before the test fires the NOTIFY. Without it the notification could be
// sent before anyone is listening and then missed (WaitDue would block until its
// own deadline). It only needs to cover round-tripping one LISTEN to the backend.
const listenSettle = 300 * time.Millisecond

// TestWaitDue_UnblocksOnEnqueue is the T2.5 acceptance: a WaitDue blocked on a
// queue returns (nil) as soon as an Enqueue into that queue fires its NOTIFY,
// rather than waiting out the caller's poll interval.
func TestWaitDue_UnblocksOnEnqueue(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.WaitDue(waitCtx, "orders.reserve") }()

	time.Sleep(listenSettle)
	if err := s.Enqueue(ctx, pendingTask("01J2ZN0000000000000000000A", "orders.reserve", time.Now())); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitDue = %v, want nil (woken by enqueue notify)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitDue did not unblock on enqueue within 5s")
	}
}

// TestWaitDue_UnblocksOnReschedule proves the reschedule path also notifies: a
// worker blocked in WaitDue wakes when a claimed task is rescheduled back to
// PENDING.
func TestWaitDue_UnblocksOnReschedule(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	id := "01J2ZN0000000000000000000A"
	if err := s.Enqueue(ctx, pendingTask(id, "q", time.Now().Add(-time.Minute))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	c := mustClaimOne(ctx, t, s, "q", time.Hour)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.WaitDue(waitCtx, "q") }()

	time.Sleep(listenSettle)
	if err := s.Reschedule(ctx, spi.TaskID(id), c.Token,
		spi.Attempt{AttemptNo: 1, StartedAt: time.Now(), Outcome: envelope.OutcomeRetryableFailure,
			Error: &envelope.Error{Type: "boom", Message: "retry"}}, time.Now()); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitDue = %v, want nil (woken by reschedule notify)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitDue did not unblock on reschedule within 5s")
	}
}

// TestWaitDue_IgnoresOtherQueue confirms the payload filter: a notify for another
// queue must not wake a worker waiting on a different one; the matching queue's
// notify then does.
func TestWaitDue_IgnoresOtherQueue(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.WaitDue(waitCtx, "q1") }()

	time.Sleep(listenSettle)
	// A notify for q2 must NOT wake the q1 waiter.
	if err := s.Enqueue(ctx, pendingTask("01J2ZN0000000000000000000B", "q2", time.Now())); err != nil {
		t.Fatalf("Enqueue q2: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("WaitDue woke on a foreign queue's notify (err=%v)", err)
	case <-time.After(1500 * time.Millisecond):
		// Still blocked, as required.
	}

	// The matching queue's notify does wake it.
	if err := s.Enqueue(ctx, pendingTask("01J2ZN0000000000000000000A", "q1", time.Now())); err != nil {
		t.Fatalf("Enqueue q1: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitDue = %v, want nil after matching-queue notify", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitDue did not unblock on its own queue within 5s")
	}
}

// TestWaitDue_RespectsContext checks WaitDue honors ctx: with no notification it
// returns the context error at the deadline instead of blocking forever, so the
// engine's poll interval bounds the wait.
func TestWaitDue_RespectsContext(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	waitCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := s.WaitDue(waitCtx, "q")
	if err == nil {
		t.Fatal("WaitDue = nil, want a context error on deadline with no notify")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("WaitDue blocked %v past its deadline, want prompt return", elapsed)
	}
}
