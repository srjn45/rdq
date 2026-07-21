// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// migratedStore brings up a testcontainers Postgres, applies the migrations, and
// returns a Store ready for claim/mutation tests. It skips (not fails) when
// Docker is unavailable, matching the T2.1 harness.
func migratedStore(ctx context.Context, t *testing.T) *Store {
	t.Helper()
	db := startPostgres(ctx, t)
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return New(db)
}

// seedPending inserts a PENDING task due at nextAt directly into rdq_task (a
// stand-in for Enqueue, which lands in a later task). It reuses the T2.2 mapping
// so the seeded row is a faithful envelope decomposition.
func seedPending(ctx context.Context, t *testing.T, s *Store, id, queue string, nextAt time.Time) {
	t.Helper()
	e := &envelope.Envelope{
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
	row, err := taskRowFromEnvelope(e)
	if err != nil {
		t.Fatalf("taskRowFromEnvelope: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO rdq_task
		(id, queue, envelope_version, handler_ref, handler_version, payload,
		 payload_content_type, payload_ref, headers, status, attempt_count,
		 redrive_count, next_attempt_at, lease_expires_at, claim_token, created_at, residual)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NULL,NULL,$14,$15)`,
		row.ID, row.Queue, row.EnvelopeVersion, row.HandlerRef, row.HandlerVersion,
		row.Payload, row.PayloadContentType, row.PayloadRef, row.Headers, row.Status,
		row.AttemptCount, row.RedriveCount, row.NextAttemptAt, row.CreatedAt, row.Residual,
	); err != nil {
		t.Fatalf("seeding task %s: %v", id, err)
	}
}

// liveTask reads back key columns of a task from rdq_task. exists is false when
// the row is gone (e.g. dead-lettered).
func liveTask(ctx context.Context, t *testing.T, s *Store, id string) (status string, attemptCount int, token *string, nextAt, lease *time.Time, exists bool) {
	t.Helper()
	err := s.db.QueryRowContext(ctx, `SELECT status, attempt_count, claim_token::text,
		next_attempt_at, lease_expires_at FROM rdq_task WHERE id = $1`, id).
		Scan(&status, &attemptCount, &token, &nextAt, &lease)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return "", 0, nil, nil, nil, false
		}
		t.Fatalf("reading task %s: %v", id, err)
	}
	return status, attemptCount, token, nextAt, lease, true
}

// TestClaimDue_ClaimsOnlyDue claims a queue with a due and a not-yet-due task and
// asserts only the due one is handed out, IN_FLIGHT, with a token and a lease.
func TestClaimDue_ClaimsOnlyDue(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)
	seedPending(ctx, t, s, "01J2ZN0000000000000000000A", "orders.reserve", past)
	seedPending(ctx, t, s, "01J2ZN0000000000000000000B", "orders.reserve", future)

	claimed, err := s.ClaimDue(ctx, "orders.reserve", 10, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d tasks, want 1", len(claimed))
	}
	c := claimed[0]
	if c.Task.ID != "01J2ZN0000000000000000000A" {
		t.Errorf("claimed id = %s, want the due task", c.Task.ID)
	}
	if c.Task.Status != envelope.StatusInFlight {
		t.Errorf("claimed status = %s, want IN_FLIGHT", c.Task.Status)
	}
	if c.Token == "" {
		t.Error("claim did not mint a fencing token")
	}
	if c.Task.LeaseExpiresAt == nil || !c.Task.LeaseExpiresAt.After(time.Now()) {
		t.Errorf("claimed lease_expires_at = %v, want a future lease", c.Task.LeaseExpiresAt)
	}

	// The not-yet-due task is untouched.
	status, _, token, _, _, exists := liveTask(ctx, t, s, "01J2ZN0000000000000000000B")
	if !exists || status != "PENDING" || token != nil {
		t.Errorf("future task = (%s, token=%v, exists=%v), want PENDING/no-token", status, token, exists)
	}
}

// TestClaimDue_NoDoubleClaim proves the lease holds: a second claim within the
// lease window returns nothing (the task is IN_FLIGHT with a live lease, so it is
// not due). SKIP LOCKED + the due predicate together prevent a double claim.
func TestClaimDue_NoDoubleClaim(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	seedPending(ctx, t, s, "01J2ZN0000000000000000000A", "q", time.Now().Add(-time.Minute))

	first, err := s.ClaimDue(ctx, "q", 10, time.Hour)
	if err != nil {
		t.Fatalf("first ClaimDue: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first claim got %d, want 1", len(first))
	}
	second, err := s.ClaimDue(ctx, "q", 10, time.Hour)
	if err != nil {
		t.Fatalf("second ClaimDue: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second claim got %d, want 0 (task still leased)", len(second))
	}
}

// TestClaimDue_ReclaimsExpiredLease drives the crash-recovery path: a claim with
// a lease already in the past (negative duration) leaves an expired lease, and
// the next claim reclaims it, appending a LEASE_EXPIRED attempt (G7) and minting
// a fresh token distinct from the dead one.
func TestClaimDue_ReclaimsExpiredLease(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	seedPending(ctx, t, s, "01J2ZN0000000000000000000A", "q", time.Now().Add(-time.Minute))

	// Claim with a lease that expires one minute in the PAST: the row is left
	// IN_FLIGHT but immediately reclaimable.
	dead, err := s.ClaimDue(ctx, "q", 10, -time.Minute)
	if err != nil {
		t.Fatalf("first ClaimDue: %v", err)
	}
	if len(dead) != 1 {
		t.Fatalf("first claim got %d, want 1", len(dead))
	}
	deadToken := dead[0].Token

	reclaimed, err := s.ClaimDue(ctx, "q", 10, time.Hour)
	if err != nil {
		t.Fatalf("reclaim ClaimDue: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("reclaim got %d, want 1", len(reclaimed))
	}
	c := reclaimed[0]
	if c.Token == deadToken {
		t.Error("reclaim reused the dead fencing token; want a fresh one")
	}
	if c.Task.AttemptCount != 1 {
		t.Errorf("attempt_count = %d, want 1 after lease-expired record", c.Task.AttemptCount)
	}
	if len(c.Task.Attempts) != 1 {
		t.Fatalf("claimed history has %d attempts, want 1 LEASE_EXPIRED", len(c.Task.Attempts))
	}
	a := c.Task.Attempts[0]
	if a.Outcome != envelope.OutcomeLeaseExpired {
		t.Errorf("attempt outcome = %s, want LEASE_EXPIRED", a.Outcome)
	}
	if a.Error == nil || a.Error.Type != leaseExpiredType {
		t.Errorf("attempt error = %v, want type %q", a.Error, leaseExpiredType)
	}
}

// TestClaimDue_RespectsLimitAndQueue checks the limit cap and queue scoping: two
// due tasks in two queues, a limit of 1, claims exactly one from the named queue.
func TestClaimDue_RespectsLimitAndQueue(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(ctx, t)

	past := time.Now().Add(-time.Minute)
	seedPending(ctx, t, s, "01J2ZN0000000000000000000A", "q1", past)
	seedPending(ctx, t, s, "01J2ZN0000000000000000000B", "q1", past)
	seedPending(ctx, t, s, "01J2ZN0000000000000000000C", "q2", past)

	claimed, err := s.ClaimDue(ctx, "q1", 1, time.Hour)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d, want 1 (limit)", len(claimed))
	}
	if claimed[0].Task.Queue != "q1" {
		t.Errorf("claimed from queue %s, want q1", claimed[0].Task.Queue)
	}

	// q2's task is untouched by the q1 claim.
	if _, _, _, _, _, exists := liveTask(ctx, t, s, "01J2ZN0000000000000000000C"); !exists {
		t.Error("q2 task should be untouched")
	}
	if status, _, _, _, _, _ := liveTask(ctx, t, s, "01J2ZN0000000000000000000C"); status != "PENDING" {
		t.Errorf("q2 task status = %s, want PENDING", status)
	}
}

// mustClaimOne claims exactly one task from queue and returns it, failing the
// test otherwise. Shared by the mutation tests.
func mustClaimOne(ctx context.Context, t *testing.T, s *Store, queue string, lease time.Duration) spi.Claimed {
	t.Helper()
	claimed, err := s.ClaimDue(ctx, queue, 10, lease)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d tasks, want 1", len(claimed))
	}
	return claimed[0]
}
