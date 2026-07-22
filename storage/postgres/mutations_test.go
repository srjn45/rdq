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

// failedAttempt builds an engine-supplied Attempt with the given number and a
// non-nil error, the shape Reschedule/DeadLetter receive from the worker.
func failedAttempt(no int, outcome envelope.Outcome, errType string) spi.Attempt {
	now := time.Now()
	return envelope.Attempt{
		AttemptNo:  no,
		StartedAt:  now.Add(-time.Second),
		FinishedAt: &now,
		Outcome:    outcome,
		Error: &envelope.Error{
			Type:    errType,
			Message: "boom",
		},
	}
}

// successAttempt builds a SUCCESS attempt (no error), the shape Complete receives.
func successAttempt(no int) spi.Attempt {
	now := time.Now()
	return envelope.Attempt{
		AttemptNo:  no,
		StartedAt:  now.Add(-time.Second),
		FinishedAt: &now,
		Outcome:    envelope.OutcomeSuccess,
	}
}

// attemptCount returns the number of rdq_attempt rows recorded for a task.
func attemptCount(ctx context.Context, t *testing.T, s *Store, id string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRowContext(ctx,
		"SELECT count(*) FROM rdq_attempt WHERE task_id = $1", id).Scan(&n); err != nil {
		t.Fatalf("counting attempts for %s: %v", id, err)
	}
	return n
}

// dlqRow returns the status, denormalized error_type, and existence of a DLQ row.
func dlqRow(ctx context.Context, t *testing.T, s *Store, id string) (status string, errType *string, exists bool) {
	t.Helper()
	err := s.db.QueryRowContext(ctx,
		"SELECT status, error_type FROM rdq_dlq_task WHERE id = $1", id).Scan(&status, &errType)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return "", nil, false
		}
		t.Fatalf("reading dlq row %s: %v", id, err)
	}
	return status, errType, true
}

// TestReschedule_Fenced covers the failure path and its fencing: a valid token
// returns the task to PENDING and records the attempt; the now-stale token is
// rejected with ErrStaleClaim and changes nothing.
func TestReschedule_Fenced(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)
	seedPending(ctx, t, s, "01J2ZN0000000000000000000A", "q", time.Now().Add(-time.Minute))

	c := mustClaimOne(ctx, t, s, "q", time.Hour)
	nextAt := time.Now().Add(5 * time.Minute)
	if err := s.Reschedule(ctx, c.Task.ID, c.Token, failedAttempt(1, envelope.OutcomeRetryableFailure, "net.Timeout"), nextAt); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}

	status, ac, token, gotNext, lease, exists := liveTask(ctx, t, s, c.Task.ID)
	if !exists || status != "PENDING" {
		t.Fatalf("after reschedule: status=%s exists=%v, want PENDING", status, exists)
	}
	if ac != 1 {
		t.Errorf("attempt_count = %d, want 1", ac)
	}
	if token != nil || lease != nil {
		t.Errorf("claim not cleared: token=%v lease=%v", token, lease)
	}
	if gotNext == nil || gotNext.Sub(nextAt).Abs() > time.Second {
		t.Errorf("next_attempt_at = %v, want ~%v", gotNext, nextAt)
	}
	if n := attemptCount(ctx, t, s, c.Task.ID); n != 1 {
		t.Errorf("recorded %d attempts, want 1", n)
	}

	// The spent token is now stale: rescheduling again must not change state.
	err := s.Reschedule(ctx, c.Task.ID, c.Token, failedAttempt(2, envelope.OutcomeRetryableFailure, "net.Timeout"), nextAt)
	if !errors.Is(err, spi.ErrStaleClaim) {
		t.Fatalf("stale Reschedule: got %v, want ErrStaleClaim", err)
	}
	if n := attemptCount(ctx, t, s, c.Task.ID); n != 1 {
		t.Errorf("stale Reschedule changed history: %d attempts, want 1", n)
	}
}

// TestComplete_Fenced covers the success path and its fencing.
func TestComplete_Fenced(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)
	seedPending(ctx, t, s, "01J2ZN0000000000000000000A", "q", time.Now().Add(-time.Minute))

	c := mustClaimOne(ctx, t, s, "q", time.Hour)
	if err := s.Complete(ctx, c.Task.ID, c.Token, successAttempt(1)); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	status, ac, token, _, lease, exists := liveTask(ctx, t, s, c.Task.ID)
	if !exists || status != "SUCCEEDED" {
		t.Fatalf("after complete: status=%s exists=%v, want SUCCEEDED", status, exists)
	}
	if ac != 1 || token != nil || lease != nil {
		t.Errorf("unexpected state: attempt_count=%d token=%v lease=%v", ac, token, lease)
	}
	if n := attemptCount(ctx, t, s, c.Task.ID); n != 1 {
		t.Errorf("recorded %d attempts, want 1", n)
	}

	err := s.Complete(ctx, c.Task.ID, c.Token, successAttempt(2))
	if !errors.Is(err, spi.ErrStaleClaim) {
		t.Fatalf("stale Complete: got %v, want ErrStaleClaim", err)
	}
}

// TestDeadLetter_Fenced covers exhaustion: the task moves to the DLQ with the
// terminal error_type denormalized and the final attempt recorded; the stale
// token is rejected.
func TestDeadLetter_Fenced(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)
	seedPending(ctx, t, s, "01J2ZN0000000000000000000A", "q", time.Now().Add(-time.Minute))

	c := mustClaimOne(ctx, t, s, "q", time.Hour)
	if err := s.DeadLetter(ctx, c.Task.ID, c.Token, failedAttempt(1, envelope.OutcomePermanentFailure, "app.Fatal")); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	// Gone from the live table, present in the DLQ.
	if _, _, _, _, _, exists := liveTask(ctx, t, s, c.Task.ID); exists {
		t.Error("dead-lettered task still present in rdq_task")
	}
	status, errType, exists := dlqRow(ctx, t, s, c.Task.ID)
	if !exists || status != "DEAD" {
		t.Fatalf("dlq row: status=%s exists=%v, want DEAD", status, exists)
	}
	if errType == nil || *errType != "app.Fatal" {
		t.Errorf("denormalized error_type = %v, want app.Fatal", errType)
	}
	if n := attemptCount(ctx, t, s, c.Task.ID); n != 1 {
		t.Errorf("recorded %d attempts, want 1", n)
	}

	// The spent token no longer matches any live row.
	err := s.DeadLetter(ctx, c.Task.ID, c.Token, failedAttempt(2, envelope.OutcomePermanentFailure, "app.Fatal"))
	if !errors.Is(err, spi.ErrStaleClaim) {
		t.Fatalf("stale DeadLetter: got %v, want ErrStaleClaim", err)
	}
}

// TestExtendLease_Fenced renews a lease with the valid token and rejects a stale
// one.
func TestExtendLease_Fenced(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)
	seedPending(ctx, t, s, "01J2ZN0000000000000000000A", "q", time.Now().Add(-time.Minute))

	c := mustClaimOne(ctx, t, s, "q", time.Minute)
	_, _, _, _, leaseBefore, _ := liveTask(ctx, t, s, c.Task.ID)

	if err := s.ExtendLease(ctx, c.Task.ID, c.Token, time.Hour); err != nil {
		t.Fatalf("ExtendLease: %v", err)
	}
	_, _, _, _, leaseAfter, _ := liveTask(ctx, t, s, c.Task.ID)
	if leaseBefore == nil || leaseAfter == nil || !leaseAfter.After(*leaseBefore) {
		t.Errorf("lease not extended: before=%v after=%v", leaseBefore, leaseAfter)
	}

	err := s.ExtendLease(ctx, c.Task.ID, spi.ClaimToken("00000000-0000-0000-0000-000000000000"), time.Hour)
	if !errors.Is(err, spi.ErrStaleClaim) {
		t.Fatalf("stale ExtendLease: got %v, want ErrStaleClaim", err)
	}
}

// TestMutations_MalformedTokenIsStale checks the fencing is robust to garbage:
// a non-UUID token is a clean ErrStaleClaim, not a SQL error, and leaves the
// live claim untouched.
func TestMutations_MalformedTokenIsStale(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)
	seedPending(ctx, t, s, "01J2ZN0000000000000000000A", "q", time.Now().Add(-time.Minute))

	c := mustClaimOne(ctx, t, s, "q", time.Hour)
	garbage := spi.ClaimToken("not-a-uuid")

	if err := s.ExtendLease(ctx, c.Task.ID, garbage, time.Hour); !errors.Is(err, spi.ErrStaleClaim) {
		t.Errorf("ExtendLease(garbage): got %v, want ErrStaleClaim", err)
	}
	if err := s.Complete(ctx, c.Task.ID, garbage, successAttempt(1)); !errors.Is(err, spi.ErrStaleClaim) {
		t.Errorf("Complete(garbage): got %v, want ErrStaleClaim", err)
	}
	if err := s.DeadLetter(ctx, c.Task.ID, garbage, failedAttempt(1, envelope.OutcomePermanentFailure, "x")); !errors.Is(err, spi.ErrStaleClaim) {
		t.Errorf("DeadLetter(garbage): got %v, want ErrStaleClaim", err)
	}

	// The real claim still holds: the valid token completes normally.
	if err := s.Complete(ctx, c.Task.ID, c.Token, successAttempt(1)); err != nil {
		t.Fatalf("Complete with valid token after garbage attempts: %v", err)
	}
}
