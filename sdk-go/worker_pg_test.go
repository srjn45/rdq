// SPDX-License-Identifier: Apache-2.0

// Postgres integration tests for the sdk-go Worker (T4.2). Skipped when
// Docker is unavailable (RDQ_SKIP_DOCKER env) to keep `go test ./...` green
// on machines without a container runtime.

package rdq_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"

	pgmod "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/policy"
	"github.com/srjn45/rdq/core/registry"
	pgstore "github.com/srjn45/rdq/storage/postgres"
	rdq "github.com/srjn45/rdq/sdk-go"
	"github.com/srjn45/rdq/sdk-go/submit"
)

// Handler names for the postgres test suite, distinct from the memstore names.
const (
	hdlPGOK   = "rdq.t.pg.ok"
	hdlPGPerm = "rdq.t.pg.perm"
)

// startPostgresForSDK brings up a throwaway Postgres container, registers
// cleanup with t, and returns an open *sql.DB. Mirrors the helper in
// storage/postgres/migrations_test.go so the harness pattern stays consistent.
func startPostgresForSDK(ctx context.Context, t *testing.T) *sql.DB {
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
		if isPGDockerUnavailable(err) {
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
	db, err := pgstore.Open(dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping db: %v", err)
	}
	return db
}

// isPGDockerUnavailable reports whether err looks like "no container runtime
// reachable" rather than a real provisioning failure.
func isPGDockerUnavailable(err error) bool {
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
		if containsFoldPG(msg, needle) {
			return true
		}
	}
	return false
}

func containsFoldPG(s, sub string) bool {
	return strings.Contains(strings.Map(unicode.ToLower, s), strings.Map(unicode.ToLower, sub))
}

// pgSpec returns a QueueSpec suitable for Postgres integration tests. Uses
// longer timeouts than memstore tests to tolerate container cold-start latency.
func pgSpec(queue string) rdq.QueueSpec {
	return rdq.QueueSpec{
		Queue:          queue,
		MaxAttempts:    3,
		Backoff:        policy.Backoff{Initial: 50 * time.Millisecond, Multiplier: 1, Max: time.Hour},
		Classifier:     policy.Classifier{},
		Lease:          10 * time.Second,
		HandlerTimeout: 9 * time.Second,
		BatchSize:      4,
		Concurrency:    2,
		PollInterval:   20 * time.Millisecond,
	}
}

// enqueuePG submits a task to store and returns the task ID.
func enqueuePG(t *testing.T, store *pgstore.Store, queue, handlerRef string) string {
	t.Helper()
	env, err := submit.Submit(queue, handlerRef, []byte("{}"))
	if err != nil {
		t.Fatalf("submit.Submit: %v", err)
	}
	if err := store.Enqueue(context.Background(), *env); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return env.ID
}

// waitTerminalPG polls store until the task with id reaches a terminal state.
func waitTerminalPG(t *testing.T, store *pgstore.Store, id string) envelope.Envelope {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		env, err := store.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if env.Status == envelope.StatusSucceeded || env.Status == envelope.StatusDead {
			return env
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not reach terminal state within 30s", id)
	panic("unreachable")
}

// TestWorker_Postgres_Success verifies the full claim-handle-complete path
// against a real Postgres backend: a handler returning nil drives the task
// to SUCCEEDED.
func TestWorker_Postgres_Success(t *testing.T) {
	ctx := context.Background()
	db := startPostgresForSDK(ctx, t)
	if err := pgstore.Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	store := pgstore.New(db)

	if err := rdq.Register(hdlPGOK, func(_ context.Context, _ envelope.Envelope) error {
		return nil
	}); err != nil && !errors.Is(err, registry.ErrDuplicateHandler) {
		t.Fatalf("Register: %v", err)
	}

	id := enqueuePG(t, store, "pg.ok", hdlPGOK)

	w, err := rdq.NewWorker(store, []rdq.QueueSpec{pgSpec("pg.ok")})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	ctx2, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx2) }()

	env := waitTerminalPG(t, store, id)
	cancel()
	if env.Status != envelope.StatusSucceeded {
		t.Fatalf("status = %s, want SUCCEEDED", env.Status)
	}
}

// TestWorker_Postgres_Permanent verifies that rdq.Permanent(err) dead-letters
// the task on the first attempt against a real Postgres backend.
func TestWorker_Postgres_Permanent(t *testing.T) {
	ctx := context.Background()
	db := startPostgresForSDK(ctx, t)
	if err := pgstore.Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	store := pgstore.New(db)

	if err := rdq.Register(hdlPGPerm, func(_ context.Context, _ envelope.Envelope) error {
		return rdq.Permanent(errors.New("cannot fix"))
	}); err != nil && !errors.Is(err, registry.ErrDuplicateHandler) {
		t.Fatalf("Register: %v", err)
	}

	id := enqueuePG(t, store, "pg.perm", hdlPGPerm)

	w, err := rdq.NewWorker(store, []rdq.QueueSpec{pgSpec("pg.perm")})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	ctx2, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx2) }()

	env := waitTerminalPG(t, store, id)
	cancel()
	if env.Status != envelope.StatusDead {
		t.Fatalf("status = %s, want DEAD", env.Status)
	}
	if env.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1 (Permanent skips retries)", env.AttemptCount)
	}
}
