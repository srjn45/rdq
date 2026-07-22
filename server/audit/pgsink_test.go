// SPDX-License-Identifier: Apache-2.0

package audit_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	pgmod "github.com/testcontainers/testcontainers-go/modules/postgres"

	coreaudit "github.com/srjn45/rdq/core/audit"
	srvaudit "github.com/srjn45/rdq/server/audit"
	pgstore "github.com/srjn45/rdq/storage/postgres"
)

// startPostgres brings up a throwaway Postgres via testcontainers, applies all
// migrations (including 0003_audit), and returns an open *sql.DB. Skips when
// Docker is unavailable so local runs without a container runtime stay green.
func startPostgres(ctx context.Context, t *testing.T) *sql.DB {
	t.Helper()
	if os.Getenv("RDQ_SKIP_DOCKER") != "" {
		t.Skip("RDQ_SKIP_DOCKER set; skipping testcontainers Postgres test")
	}

	container, err := pgmod.Run(ctx, "postgres:16-alpine",
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
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = testcontainers.TerminateContainer(container, testcontainers.StopContext(ctx))
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping db: %v", err)
	}
	if err := pgstore.Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return db
}

func isDockerUnavailable(err error) bool {
	msg := err.Error()
	for _, needle := range []string{
		"Cannot connect to the Docker daemon",
		"docker daemon",
		"failed to find a viable Docker",
		"failed to create Docker provider",
		"get provider",
		"rootless Docker not found",
		"Docker host",
		"dial unix",
		"connect: connection refused",
		"no such file or directory",
		"executable file not found",
	} {
		if containsFold(msg, needle) {
			return true
		}
	}
	return false
}

func containsFold(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub ||
		func() bool {
			for i := 0; i <= len(s)-len(sub); i++ {
				if equalFold(s[i:i+len(sub)], sub) {
					return true
				}
			}
			return false
		}())
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

func TestPGSinkEmit(t *testing.T) {
	ctx := context.Background()
	db := startPostgres(ctx, t)
	sink := srvaudit.NewPGSink(db)

	ts := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	r := coreaudit.Record{
		Timestamp: ts,
		Principal: "ops-bot",
		Action:    coreaudit.ActionRedrive,
		Queue:     "payments",
		Selector:  "filter:{error_type:timeout}",
		Count:     7,
		Outcome:   coreaudit.OutcomeSuccess,
	}

	if err := sink.Emit(r); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	var (
		principal, action, queue, selector, outcome, errMsg string
		count                                                int
		dbTS                                                 time.Time
	)
	err := db.QueryRowContext(ctx,
		`SELECT timestamp, principal, action, queue, selector, count, outcome, error_message
		   FROM rdq_audit WHERE principal = $1`, "ops-bot").
		Scan(&dbTS, &principal, &action, &queue, &selector, &count, &outcome, &errMsg)
	if errors.Is(err, sql.ErrNoRows) {
		t.Fatal("Emit wrote no row to rdq_audit")
	}
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if principal != "ops-bot" {
		t.Errorf("principal: got %q, want %q", principal, "ops-bot")
	}
	if action != "redrive" {
		t.Errorf("action: got %q, want %q", action, "redrive")
	}
	if queue != "payments" {
		t.Errorf("queue: got %q, want %q", queue, "payments")
	}
	if selector != "filter:{error_type:timeout}" {
		t.Errorf("selector: got %q", selector)
	}
	if count != 7 {
		t.Errorf("count: got %d, want 7", count)
	}
	if outcome != "success" {
		t.Errorf("outcome: got %q, want success", outcome)
	}
	if errMsg != "" {
		t.Errorf("error_message: got %q, want empty", errMsg)
	}
}

func TestPGSinkAllActions(t *testing.T) {
	ctx := context.Background()
	db := startPostgres(ctx, t)
	sink := srvaudit.NewPGSink(db)

	actions := []coreaudit.Action{
		coreaudit.ActionRedrive,
		coreaudit.ActionPurge,
		coreaudit.ActionPause,
		coreaudit.ActionResume,
		coreaudit.ActionConfigWrite,
	}
	for _, action := range actions {
		r := coreaudit.Record{
			Timestamp: time.Now().UTC(),
			Principal: "tester",
			Action:    action,
			Queue:     "q1",
			Count:     -1,
			Outcome:   coreaudit.OutcomeSuccess,
		}
		if err := sink.Emit(r); err != nil {
			t.Errorf("Emit(%s): %v", action, err)
		}
	}

	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM rdq_audit WHERE principal = 'tester'`).Scan(&n); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if n != len(actions) {
		t.Errorf("expected %d rows, got %d", len(actions), n)
	}
}

func TestPGSinkFailureRecord(t *testing.T) {
	ctx := context.Background()
	db := startPostgres(ctx, t)
	sink := srvaudit.NewPGSink(db)

	r := coreaudit.Record{
		Timestamp:    time.Now().UTC(),
		Principal:    "anonymous",
		Action:       coreaudit.ActionPurge,
		Queue:        "q-fail",
		Selector:     "all",
		Count:        -1,
		Outcome:      coreaudit.OutcomeFailure,
		ErrorMessage: "storage unavailable",
	}
	if err := sink.Emit(r); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	var outcome, errMsg string
	if err := db.QueryRowContext(ctx,
		`SELECT outcome, error_message FROM rdq_audit WHERE queue = 'q-fail'`).
		Scan(&outcome, &errMsg); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if outcome != "failure" {
		t.Errorf("outcome: got %q, want failure", outcome)
	}
	if errMsg != "storage unavailable" {
		t.Errorf("error_message: got %q, want 'storage unavailable'", errMsg)
	}
}
