// SPDX-License-Identifier: Apache-2.0

// This file implements design 02 §3 invariants 3 (lease recovery counts) and 4
// (atomic transitions). See claims.go for why the bodies live in regular .go
// files rather than lease_test.go.
package compliance

import (
	"context"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// testLeaseRecoveryCounts verifies invariant 3 (design 02 §3): reclaiming an
// expired lease appends a LEASE_EXPIRED attempt to the history atomically with
// the reclaim, so a poison-pill task that keeps crashing its worker still counts
// attempts toward exhaustion (envelope §2).
func testLeaseRecoveryCounts(t *testing.T, factory func() spi.Storage) {
	const queue = "q.lease"
	s := factory()
	ctx := context.Background()
	mustEnqueue(t, s, newPendingTask("t", queue))

	first := mustClaimOne(t, s, queue, shortLease)
	if len(first.Task.Attempts) != 0 {
		t.Fatalf("fresh claim already has %d attempts, want 0", len(first.Task.Attempts))
	}

	time.Sleep(expireWait)
	reclaimed := mustClaimOne(t, s, queue, longLease)

	// The reclaimed envelope carries exactly one new LEASE_EXPIRED record.
	if got := len(reclaimed.Task.Attempts); got != 1 {
		t.Fatalf("attempts after reclaim = %d, want 1 (LEASE_EXPIRED appended)", got)
	}
	a := reclaimed.Task.Attempts[0]
	if a.Outcome != envelope.OutcomeLeaseExpired {
		t.Fatalf("reclaim attempt outcome = %s, want LEASE_EXPIRED", a.Outcome)
	}
	if a.Error == nil || a.Error.Type != "rdq.LeaseExpired" {
		t.Fatalf("reclaim attempt error = %+v, want type rdq.LeaseExpired", a.Error)
	}
	if reclaimed.Task.AttemptCount != 1 {
		t.Fatalf("attempt_count after reclaim = %d, want 1 (lease expiry counts)", reclaimed.Task.AttemptCount)
	}

	// The count is durable, not just present on the returned Claimed.
	got, err := s.Get(ctx, "t")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.AttemptCount != 1 || len(got.Attempts) != 1 {
		t.Fatalf("post-reclaim Get attempt_count=%d attempts=%d, want 1/1", got.AttemptCount, len(got.Attempts))
	}
}

// testAtomicTransition verifies invariant 4 (design 02 §3): each SPI mutation is
// all-or-nothing, so a task is only ever observed in a self-consistent state. The
// kit cannot crash a backend mid-call, but it can assert the observable face of
// atomicity — after every transition the status and the lease/next-attempt/
// history fields agree, and the lease-reclaim append never appears half-applied.
func testAtomicTransition(t *testing.T, factory func() spi.Storage) {
	const queue = "q.atomic"
	s := factory()
	ctx := context.Background()
	mustEnqueue(t, s, newPendingTask("t", queue))

	// PENDING: due, no lease.
	assertConsistent(t, s, "t", envelope.StatusPending)

	// claim → IN_FLIGHT with a lease.
	c := mustClaimOne(t, s, queue, longLease)
	assertConsistent(t, s, "t", envelope.StatusInFlight)

	// Reschedule → PENDING again, attempt appended, lease cleared — all together.
	nextAt := time.Now().Add(-time.Second)
	if err := s.Reschedule(ctx, "t", c.Token, retryAttempt(1, "boom"), nextAt); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}
	got := assertConsistent(t, s, "t", envelope.StatusPending)
	if got.AttemptCount != 1 || len(got.Attempts) != 1 {
		t.Fatalf("post-Reschedule attempt_count=%d attempts=%d, want 1/1 (append is atomic with the transition)", got.AttemptCount, len(got.Attempts))
	}

	// claim → Complete → SUCCEEDED terminal.
	c = mustClaimOne(t, s, queue, longLease)
	if err := s.Complete(ctx, "t", c.Token, retryAttempt(2, "ok")); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	got = assertConsistent(t, s, "t", envelope.StatusSucceeded)
	if len(got.Attempts) != 2 {
		t.Fatalf("post-Complete attempts = %d, want 2", len(got.Attempts))
	}

	// The lease-reclaim append (invariant 3) is atomic with the re-lease: a
	// second task is never seen IN_FLIGHT-without-the-record or vice versa.
	mustEnqueue(t, s, newPendingTask("u", queue))
	mustClaimOne(t, s, queue, shortLease)
	time.Sleep(expireWait)
	reclaimed := mustClaimOne(t, s, queue, longLease)
	if reclaimed.Task.Status != envelope.StatusInFlight {
		t.Fatalf("reclaimed status = %s, want IN_FLIGHT", reclaimed.Task.Status)
	}
	if len(reclaimed.Task.Attempts) != 1 {
		t.Fatalf("reclaimed attempts = %d, want the LEASE_EXPIRED record present with the re-lease", len(reclaimed.Task.Attempts))
	}
}

// assertConsistent fetches id and checks that the field set matches want,
// returning the fetched envelope for further assertions.
func assertConsistent(t *testing.T, s spi.Storage, id spi.TaskID, want envelope.Status) envelope.Envelope {
	t.Helper()
	got, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	if got.Status != want {
		t.Fatalf("status = %s, want %s", got.Status, want)
	}
	switch want {
	case envelope.StatusPending:
		if got.NextAttemptAt == nil {
			t.Fatalf("PENDING task has nil next_attempt_at")
		}
		if got.LeaseExpiresAt != nil {
			t.Fatalf("PENDING task still carries a lease")
		}
	case envelope.StatusInFlight:
		if got.LeaseExpiresAt == nil {
			t.Fatalf("IN_FLIGHT task has nil lease_expires_at")
		}
	case envelope.StatusSucceeded, envelope.StatusDead:
		if got.LeaseExpiresAt != nil {
			t.Fatalf("terminal task (%s) still carries a lease", want)
		}
		if got.NextAttemptAt != nil {
			t.Fatalf("terminal task (%s) still has next_attempt_at", want)
		}
	}
	return got
}
