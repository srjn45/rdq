// SPDX-License-Identifier: Apache-2.0

// Package compliance is the storage compliance kit: a single exported entry
// point, Run, that verifies any spi.Storage implementation against the eight
// correctness invariants of design 02 §3. The in-memory reference store passes
// it (T1.7); the Postgres binding (M2) and the Java binding (M7) run the same
// kit against real backends via testcontainers, which is what freezes the
// storage contract as a cross-backend guarantee rather than a per-plugin hope.
//
// The kit is an ordinary (non-_test.go) package so a plugin in a different
// module can import it and call Run from its own test:
//
//	func TestCompliance(t *testing.T) {
//	    compliance.Run(t, func() spi.Storage { return mystore.New(...) })
//	}
//
// Every invariant is exercised as a named subtest, so a failure names the exact
// contract clause a backend violates. The kit assumes only the SPI floor: it
// never injects a clock (the backend owns the clock, G9), so lease-expiry
// invariants use short real leases and wait past them — the same technique that
// works against Postgres now(). Each subtest builds a fresh store from factory,
// so invariants never share state.
package compliance

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// shortLease is the lease handed out when a test needs a claim to expire. It is
// deliberately tiny; expireWait sleeps a comfortable multiple past it so lease
// reclaim is exercised without a controllable clock (the backend owns time, G9).
const shortLease = 40 * time.Millisecond

// expireWait is slept to guarantee a shortLease has lapsed by the backend's
// clock. Sleeping longer than needed only makes a task more due, so a loaded CI
// node makes these waits more reliable, never less.
const expireWait = 250 * time.Millisecond

// longLease outlives every test that must NOT see a lease expire mid-run.
const longLease = 10 * time.Second

// Run verifies factory's Storage against design 02 §3 invariants 1–8. Each
// invariant is a named subtest given its own fresh store from factory; a
// backend is compliant when Run reports no failures.
func Run(t *testing.T, factory func() spi.Storage) {
	t.Helper()
	if factory == nil {
		t.Fatal("compliance.Run: factory is nil")
	}
	t.Run("NoDoubleClaim", func(t *testing.T) { testNoDoubleClaim(t, factory) })
	t.Run("Fencing", func(t *testing.T) { testFencing(t, factory) })
	t.Run("LeaseRecoveryCounts", func(t *testing.T) { testLeaseRecoveryCounts(t, factory) })
	t.Run("AtomicTransition", func(t *testing.T) { testAtomicTransition(t, factory) })
	t.Run("IdempotentEnqueue", func(t *testing.T) { testIdempotentEnqueue(t, factory) })
	t.Run("LosslessRoundTrip", func(t *testing.T) { testLosslessRoundTrip(t, factory) })
	t.Run("RedriveReset", func(t *testing.T) { testRedriveReset(t, factory) })
	t.Run("StablePagination", func(t *testing.T) { testStablePagination(t, factory) })
}

// --- shared builders -------------------------------------------------------

// pastDue is a timestamp one second in the past, so a task scheduled for it is
// immediately due by any backend's clock without the test having to wait.
func pastDue() time.Time { return time.Now().Add(-time.Second) }

// newPendingTask builds a PENDING envelope due one second in the past, so it is
// immediately claimable by any backend's clock without waiting.
func newPendingTask(id, queue string) envelope.Envelope {
	due := pastDue()
	return envelope.Envelope{
		EnvelopeVersion:    1,
		ID:                 id,
		Queue:              queue,
		HandlerRef:         "h.process",
		Payload:            []byte("payload-" + id),
		PayloadContentType: "application/octet-stream",
		Status:             envelope.StatusPending,
		NextAttemptAt:      &due,
		CreatedAt:          due,
	}
}

// retryAttempt builds a retryable-failure attempt record carrying errType.
func retryAttempt(no int, errType string) spi.Attempt {
	now := time.Now()
	fin := now
	return envelope.Attempt{
		AttemptNo:  no,
		StartedAt:  now,
		FinishedAt: &fin,
		Outcome:    envelope.OutcomeRetryableFailure,
		Error:      &envelope.Error{Type: errType, Message: "boom"},
	}
}

// --- shared drivers --------------------------------------------------------

// mustEnqueue admits e, failing the test on any error.
func mustEnqueue(t *testing.T, s spi.Storage, e envelope.Envelope) {
	t.Helper()
	if err := s.Enqueue(context.Background(), e); err != nil {
		t.Fatalf("Enqueue(%s): %v", e.ID, err)
	}
}

// mustClaimOne claims exactly one due task from queue with the given lease,
// failing if the backend returns any other count.
func mustClaimOne(t *testing.T, s spi.Storage, queue string, lease time.Duration) spi.Claimed {
	t.Helper()
	claimed, err := s.ClaimDue(context.Background(), queue, 10, lease)
	if err != nil {
		t.Fatalf("ClaimDue(%s): %v", queue, err)
	}
	if len(claimed) != 1 {
		t.Fatalf("ClaimDue(%s) returned %d tasks, want 1", queue, len(claimed))
	}
	return claimed[0]
}

// driveTaskToDLQ enqueues a fresh PENDING task and dead-letters it in one claim,
// leaving exactly one attempt (carrying errType) in its history.
func driveTaskToDLQ(t *testing.T, s spi.Storage, id, queue, errType string) {
	t.Helper()
	ctx := context.Background()
	mustEnqueue(t, s, newPendingTask(id, queue))
	c := mustClaimOne(t, s, queue, longLease)
	if err := s.DeadLetter(ctx, id, c.Token, retryAttempt(1, errType)); err != nil {
		t.Fatalf("DeadLetter(%s): %v", id, err)
	}
}

// assertStaleToken asserts that every post-claim mutation rejects token with
// ErrStaleClaim (the fencing invariant, design 02 §3 #2). The caller separately
// checks that the task's state is unchanged.
func assertStaleToken(t *testing.T, s spi.Storage, id spi.TaskID, token spi.ClaimToken) {
	t.Helper()
	ctx := context.Background()
	att := retryAttempt(99, "stale.attempt")
	ops := []struct {
		name string
		call func() error
	}{
		{"Reschedule", func() error { return s.Reschedule(ctx, id, token, att, time.Now()) }},
		{"Complete", func() error { return s.Complete(ctx, id, token, att) }},
		{"DeadLetter", func() error { return s.DeadLetter(ctx, id, token, att) }},
		{"ExtendLease", func() error { return s.ExtendLease(ctx, id, token, time.Minute) }},
	}
	for _, op := range ops {
		if err := op.call(); !errors.Is(err, spi.ErrStaleClaim) {
			t.Fatalf("%s with stale token %q = %v, want ErrStaleClaim", op.name, token, err)
		}
	}
}

// fingerprint is a compact, comparable summary of the mutable fields a stale
// mutation must never touch — used to assert "changes nothing" (design 02 §3
// #2) without depending on any backend-internal representation.
func fingerprint(e envelope.Envelope) string {
	return fmt.Sprintf("status=%s attempts=%d attempt_count=%d redrive_count=%d next=%v lease=%v",
		e.Status, len(e.Attempts), e.AttemptCount, e.RedriveCount,
		formatTimePtr(e.NextAttemptAt), formatTimePtr(e.LeaseExpiresAt))
}

func formatTimePtr(t *time.Time) string {
	if t == nil {
		return "<nil>"
	}
	return t.UTC().Format(time.RFC3339Nano)
}
