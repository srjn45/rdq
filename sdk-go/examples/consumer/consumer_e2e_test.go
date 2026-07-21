// SPDX-License-Identifier: Apache-2.0

// End-to-end test for the consumer example (T4.4 acceptance criterion).
// Spins up a throwaway Postgres via testcontainers, drives the demo flow, and
// asserts the task reaches DEAD with the expected attempt history.
// Skipped automatically when Docker is unavailable (RDQ_SKIP_DOCKER env) so
// `go test ./...` stays green on machines without a container runtime.
package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"

	pgmod "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go"

	"github.com/srjn45/rdq/core/envelope"
	pgstore "github.com/srjn45/rdq/storage/postgres"
)

// TestConsumerDemo_E2E exercises the full submit→retry→DLQ lifecycle against a
// real Postgres backend. It mirrors the flow documented in sdk-go/README.md §Run
// and verifies the three acceptance criteria for T4.4:
//
//  1. The task reaches status=DEAD.
//  2. attempt_count equals maxAttempts (every retry was consumed).
//  3. The attempt history is non-empty and every record carries an error.
func TestConsumerDemo_E2E(t *testing.T) {
	ctx := context.Background()
	store := startPostgresForExample(ctx, t)

	taskID, err := runDemo(ctx, store)
	if err != nil {
		t.Fatalf("runDemo: %v", err)
	}

	env, err := store.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(%s): %v", taskID, err)
	}

	if env.Status != envelope.StatusDead {
		t.Errorf("status = %s, want %s", env.Status, envelope.StatusDead)
	}
	if env.AttemptCount != maxAttempts {
		t.Errorf("attempt_count = %d, want %d", env.AttemptCount, maxAttempts)
	}
	if len(env.Attempts) == 0 {
		t.Error("attempt history is empty; want at least one recorded attempt")
	}
	for i, a := range env.Attempts {
		if a.Error == nil {
			t.Errorf("attempts[%d].error is nil; want non-nil error detail", i)
		}
	}
}

// startPostgresForExample brings up a throwaway Postgres container, registers
// cleanup, migrates the schema, and returns a ready *pgstore.Store. It skips
// the test (not fails it) when Docker is unavailable, so CI without a runtime
// stays green.
func startPostgresForExample(ctx context.Context, t *testing.T) *pgstore.Store {
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
		if isExampleDockerUnavailable(err) {
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

	if err := pgstore.Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return pgstore.New(db)
}

// isExampleDockerUnavailable reports whether err looks like "no container
// runtime reachable" rather than a real provisioning failure.
func isExampleDockerUnavailable(err error) bool {
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
		if containsFoldEx(msg, needle) {
			return true
		}
	}
	return false
}

func containsFoldEx(s, sub string) bool {
	return strings.Contains(strings.Map(unicode.ToLower, s), strings.Map(unicode.ToLower, sub))
}
