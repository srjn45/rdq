// SPDX-License-Identifier: Apache-2.0

// This file implements design 02 §3 invariant 5 (idempotent enqueue), including
// the cross-queue conflict clause (G8). See claims.go for why the bodies live in
// regular .go files rather than enqueue_test.go.
package compliance

import (
	"context"
	"errors"
	"testing"

	"github.com/srjn45/rdq/core/spi"
)

// testIdempotentEnqueue verifies invariant 5 (design 02 §3): re-enqueue of an
// existing id in the SAME queue is a no-op that never clobbers accumulated state,
// while the same id in a DIFFERENT queue is rejected with ErrIDConflict (G8)
// rather than silently returning a foreign envelope.
func testIdempotentEnqueue(t *testing.T, factory func() spi.Storage) {
	s := factory()
	ctx := context.Background()
	task := newPendingTask("dup", "q1")

	// First admit, then a duplicate admit — both succeed.
	mustEnqueue(t, s, task)
	if err := s.Enqueue(ctx, task); err != nil {
		t.Fatalf("duplicate Enqueue (same queue) = %v, want nil no-op", err)
	}

	// Advance the task's state, then re-enqueue: the no-op must not reset it.
	c := mustClaimOne(t, s, "q1", longLease)
	if err := s.Reschedule(ctx, "dup", c.Token, retryAttempt(1, "boom"), pastDue()); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}
	if err := s.Enqueue(ctx, task); err != nil {
		t.Fatalf("re-Enqueue after state change = %v, want nil no-op", err)
	}
	got, err := s.Get(ctx, "dup")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.AttemptCount != 1 || len(got.Attempts) != 1 {
		t.Fatalf("re-enqueue clobbered state: attempt_count=%d attempts=%d, want 1/1", got.AttemptCount, len(got.Attempts))
	}

	// The same id in a different queue is a conflict, not a silent no-op.
	if err := s.Enqueue(ctx, newPendingTask("dup", "q2")); !errors.Is(err, spi.ErrIDConflict) {
		t.Fatalf("cross-queue Enqueue = %v, want ErrIDConflict", err)
	}
}
