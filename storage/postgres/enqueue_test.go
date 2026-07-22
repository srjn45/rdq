// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// pendingTask builds a minimal PENDING envelope due at nextAt, the admission-path
// counterpart to the seedPending helper (which bypasses Enqueue).
func pendingTask(id, queue string, nextAt time.Time) envelope.Envelope {
	return envelope.Envelope{
		EnvelopeVersion:    1,
		ID:                 id,
		Queue:              queue,
		HandlerRef:         "reserve-stock",
		Payload:            []byte(`{"n":1}`),
		PayloadContentType: "application/json",
		Status:             envelope.StatusPending,
		NextAttemptAt:      &nextAt,
		CreatedAt:          nextAt,
	}
}

// TestEnqueue_ThenClaim proves an enqueued task is a real, claimable row: Enqueue
// admits a due task and the next ClaimDue hands it out IN_FLIGHT with a token.
func TestEnqueue_ThenClaim(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	task := pendingTask("01J2ZN0000000000000000000A", "orders.reserve", time.Now().Add(-time.Minute))
	if err := s.Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	claimed := mustClaimOne(ctx, t, s, "orders.reserve", 30*time.Second)
	if claimed.Task.ID != task.ID {
		t.Errorf("claimed id = %s, want %s", claimed.Task.ID, task.ID)
	}
	if claimed.Task.Status != envelope.StatusInFlight {
		t.Errorf("claimed status = %s, want IN_FLIGHT", claimed.Task.Status)
	}
	if claimed.Token == "" {
		t.Error("claim did not mint a fencing token")
	}
}

// TestEnqueue_IdempotentSameQueue mirrors the compliance invariant 5 check: a
// re-enqueue of an existing id in the same queue is a no-op that must not clobber
// accumulated state (attempt history / counts).
func TestEnqueue_IdempotentSameQueue(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	task := pendingTask("01J2ZN0000000000000000000A", "q1", time.Now().Add(-time.Minute))
	if err := s.Enqueue(ctx, task); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	// Duplicate admit before any state change: a clean no-op.
	if err := s.Enqueue(ctx, task); err != nil {
		t.Fatalf("duplicate Enqueue (same queue) = %v, want nil no-op", err)
	}

	// Advance the task's state, then re-enqueue: the no-op must not reset it.
	c := mustClaimOne(ctx, t, s, "q1", time.Hour)
	nextAt := time.Now().Add(-time.Second)
	if err := s.Reschedule(ctx, spi.TaskID(task.ID), c.Token,
		spi.Attempt{AttemptNo: 1, StartedAt: time.Now(), Outcome: envelope.OutcomeRetryableFailure,
			Error: &envelope.Error{Type: "boom", Message: "kaboom"}}, nextAt); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}
	if err := s.Enqueue(ctx, task); err != nil {
		t.Fatalf("re-Enqueue after state change = %v, want nil no-op", err)
	}

	got, err := s.Get(ctx, spi.TaskID(task.ID))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.AttemptCount != 1 || len(got.Attempts) != 1 {
		t.Fatalf("re-enqueue clobbered state: attempt_count=%d attempts=%d, want 1/1",
			got.AttemptCount, len(got.Attempts))
	}
}

// TestEnqueue_CrossQueueConflict verifies G8: the same id in a different queue is
// rejected with ErrIDConflict rather than silently returning a foreign envelope.
func TestEnqueue_CrossQueueConflict(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	if err := s.Enqueue(ctx, pendingTask("01J2ZN0000000000000000000A", "q1", time.Now())); err != nil {
		t.Fatalf("Enqueue q1: %v", err)
	}
	err := s.Enqueue(ctx, pendingTask("01J2ZN0000000000000000000A", "q2", time.Now()))
	if !errors.Is(err, spi.ErrIDConflict) {
		t.Fatalf("cross-queue Enqueue = %v, want ErrIDConflict", err)
	}
}

// TestEnqueue_ConflictWithDeadLetteredID checks the conflict/idempotency check
// consults BOTH task tables: an id that has moved to the DLQ still owns its id, so
// a cross-queue re-enqueue of it is a conflict, not a fresh insert.
func TestEnqueue_ConflictWithDeadLetteredID(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	id := "01J2ZN0000000000000000000A"
	if err := s.Enqueue(ctx, pendingTask(id, "q1", time.Now().Add(-time.Minute))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	c := mustClaimOne(ctx, t, s, "q1", time.Hour)
	if err := s.DeadLetter(ctx, spi.TaskID(id), c.Token,
		spi.Attempt{AttemptNo: 1, StartedAt: time.Now(), Outcome: envelope.OutcomePermanentFailure,
			Error: &envelope.Error{Type: "fatal", Message: "done"}}); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	// Same id, different queue, now living in the DLQ: still a conflict.
	if err := s.Enqueue(ctx, pendingTask(id, "q2", time.Now())); !errors.Is(err, spi.ErrIDConflict) {
		t.Fatalf("re-enqueue of dead-lettered id in another queue = %v, want ErrIDConflict", err)
	}
	// Same id, same queue: an idempotent no-op even from the DLQ.
	if err := s.Enqueue(ctx, pendingTask(id, "q1", time.Now())); err != nil {
		t.Fatalf("re-enqueue of dead-lettered id in same queue = %v, want nil no-op", err)
	}
}

// TestCapabilities asserts the advertised optional accelerations. It needs no
// database, so it runs even without Docker.
func TestCapabilities(t *testing.T) {
	got := New(nil).Capabilities()
	want := spi.Capabilities{Notify: true, FilterPushdown: true}
	if got != want {
		t.Fatalf("Capabilities() = %+v, want %+v", got, want)
	}
}
