// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/compliance"
	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
	"github.com/testcontainers/testcontainers-go"
	pgmod "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestPostgresCompliance runs the full compliance kit against the Postgres
// spi.Storage backend (T2.6). One container is shared across all invariant
// subtests; each factory() call truncates the data tables so every invariant
// starts from a clean slate.
func TestPostgresCompliance(t *testing.T) {
	ctx := context.Background()
	db := startPostgres(ctx, t)
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	compliance.Run(t, func() spi.Storage {
		if _, err := db.ExecContext(ctx,
			"TRUNCATE rdq_task, rdq_dlq_task, rdq_attempt RESTART IDENTITY"); err != nil {
			t.Fatalf("truncate tables for fresh factory state: %v", err)
		}
		return New(db)
	})
}

// TestPostgresCompliance_KillMidClaim is the chaos acceptance test (T2.6):
// it simulates a worker kill-9 by closing its database connection before
// reporting an outcome, then verifies the orphaned task is reclaimable via
// the LEASE_EXPIRED path on a fresh connection.
func TestPostgresCompliance_KillMidClaim(t *testing.T) {
	ctx := context.Background()
	// Inline container setup so we retain the DSN to open a second connection.
	dsn := startPostgresDSN(ctx, t)

	// Worker 1: claims a task, then gets kill-9'd (connection closed without
	// reporting an outcome).
	db1, err := Open(dsn)
	if err != nil {
		t.Fatalf("open worker db: %v", err)
	}
	if err := Migrate(ctx, db1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	s1 := New(db1)

	due := time.Now().Add(-time.Minute)
	if err := s1.Enqueue(ctx, pendingTask("chaos-01", "q.chaos", due)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	const shortLease = 100 * time.Millisecond
	first, err := s1.ClaimDue(ctx, "q.chaos", 1, shortLease)
	if err != nil {
		t.Fatalf("first ClaimDue: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("want 1 claimed, got %d", len(first))
	}
	deadToken := first[0].Token

	// Simulate kill -9: close the worker's connection without reporting an outcome.
	_ = db1.Close()

	// Wait past the lease so Postgres considers it expired.
	time.Sleep(300 * time.Millisecond)

	// Worker 2: fresh connection to the same Postgres (recovery node).
	db2, err := Open(dsn)
	if err != nil {
		t.Fatalf("open recovery db: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	s2 := New(db2)

	reclaimed, err := s2.ClaimDue(ctx, "q.chaos", 1, time.Hour)
	if err != nil {
		t.Fatalf("reclaim ClaimDue: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("want 1 reclaimed after kill-9, got %d", len(reclaimed))
	}
	r := reclaimed[0]

	if r.Token == deadToken {
		t.Error("reclaim reused the dead worker's fencing token")
	}
	if r.Task.AttemptCount != 1 {
		t.Errorf("attempt_count = %d, want 1 (LEASE_EXPIRED appended by reclaim)", r.Task.AttemptCount)
	}
	if len(r.Task.Attempts) != 1 {
		t.Fatalf("attempts after reclaim = %d, want 1 LEASE_EXPIRED record", len(r.Task.Attempts))
	}
	if a := r.Task.Attempts[0]; a.Outcome != envelope.OutcomeLeaseExpired {
		t.Errorf("reclaim attempt outcome = %s, want LEASE_EXPIRED", a.Outcome)
	}

	// Dead-worker's token is fenced: every mutation must return ErrStaleClaim.
	now := time.Now()
	zombie := envelope.Attempt{
		AttemptNo: 99, StartedAt: now, FinishedAt: &now,
		Outcome: envelope.OutcomeRetryableFailure,
		Error:   &envelope.Error{Type: "zombie", Message: "dead worker attempt"},
	}
	for _, op := range []struct {
		name string
		fn   func() error
	}{
		{"Complete", func() error { return s2.Complete(ctx, "chaos-01", deadToken, zombie) }},
		{"Reschedule", func() error { return s2.Reschedule(ctx, "chaos-01", deadToken, zombie, time.Now()) }},
		{"DeadLetter", func() error { return s2.DeadLetter(ctx, "chaos-01", deadToken, zombie) }},
		{"ExtendLease", func() error { return s2.ExtendLease(ctx, "chaos-01", deadToken, time.Minute) }},
	} {
		if err := op.fn(); !errors.Is(err, spi.ErrStaleClaim) {
			t.Errorf("%s with dead token = %v, want ErrStaleClaim", op.name, err)
		}
	}

	// Recovery worker's token resolves the task successfully.
	live := envelope.Attempt{
		AttemptNo: 1, StartedAt: now, FinishedAt: &now,
		Outcome: envelope.OutcomeRetryableFailure,
		Error:   &envelope.Error{Type: "recovered", Message: "task recovered from kill-9"},
	}
	if err := s2.Complete(ctx, "chaos-01", r.Token, live); err != nil {
		t.Fatalf("recovery Complete: %v", err)
	}
}

// startPostgresDSN starts a throwaway Postgres container, registers cleanup
// with t, and returns the DSN. It is used when a test needs to open multiple
// independent *sql.DB connections to the same instance (e.g. the kill-9 chaos
// test); for single-connection tests prefer startPostgres.
func startPostgresDSN(ctx context.Context, t *testing.T) string {
	t.Helper()
	if os.Getenv("RDQ_SKIP_DOCKER") != "" {
		t.Skip("RDQ_SKIP_DOCKER set; skipping testcontainers Postgres test")
	}

	ctr, err := pgmod.Run(ctx, "postgres:16-alpine",
		pgmod.WithDatabase("rdq"),
		pgmod.WithUsername("rdq"),
		pgmod.WithPassword("rdq"),
		pgmod.BasicWaitStrategies(),
	)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("Docker unavailable, skipping Postgres integration test: %v", err)
		}
		t.Fatalf("starting postgres container: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = testcontainers.TerminateContainer(ctr, testcontainers.StopContext(stopCtx))
	})

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}
