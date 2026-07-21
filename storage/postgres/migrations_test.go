// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startPostgres brings up a throwaway Postgres via testcontainers and returns an
// open *sql.DB. It skips the test (rather than failing) when Docker is
// unavailable, so `go test ./...` stays green on machines without a container
// runtime while still exercising the real database in CI.
func startPostgres(ctx context.Context, t *testing.T) *sql.DB {
	t.Helper()
	if os.Getenv("RDQ_SKIP_DOCKER") != "" {
		t.Skip("RDQ_SKIP_DOCKER set; skipping testcontainers Postgres test")
	}

	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("rdq"),
		postgres.WithUsername("rdq"),
		postgres.WithPassword("rdq"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		// No reachable Docker daemon is an environment gap, not a failure of
		// the schema under test — skip so local runs without Docker pass.
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
	db, err := Open(dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping db: %v", err)
	}
	return db
}

// isDockerUnavailable reports whether err looks like "no container runtime
// reachable" rather than a real provisioning failure.
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
	return len(sub) == 0 || (len(s) >= len(sub) && indexFold(s, sub) >= 0)
}

func indexFold(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if equalFold(s[i:i+len(sub)], sub) {
			return i
		}
	}
	return -1
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// TestMigrateUpDown applies the migrations, asserts the schema and version gate
// are in place, rolls all the way down, and re-applies — the T2.1 acceptance
// (migrations apply cleanly up AND down in a testcontainers Postgres, and the
// schema-version gate is tested).
func TestMigrateUpDown(t *testing.T) {
	ctx := context.Background()
	db := startPostgres(ctx, t)

	// Before migrating, the gate reports "not initialized".
	if err := CheckSchemaVersion(ctx, db); !errors.Is(err, ErrSchemaNotInitialized) {
		t.Fatalf("pre-migrate gate: got %v, want ErrSchemaNotInitialized", err)
	}

	// --- up ---
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate up: %v", err)
	}
	for _, table := range []string{"rdq_task", "rdq_dlq_task", "rdq_attempt", "rdq_schema_version"} {
		if !tableExists(ctx, t, db, table) {
			t.Errorf("table %s missing after Migrate", table)
		}
	}
	// The claim index (design 02 §4) must exist.
	if !indexExists(ctx, t, db, "rdq_task_due_idx") {
		t.Error("partial claim index rdq_task_due_idx missing after Migrate")
	}
	// The gate now passes and the recorded version matches SchemaVersion.
	if err := CheckSchemaVersion(ctx, db); err != nil {
		t.Fatalf("post-migrate gate: %v", err)
	}
	if got := recordedVersion(ctx, t, db); got != SchemaVersion {
		t.Errorf("rdq_schema_version = %d, want %d", got, SchemaVersion)
	}

	// Migrate is idempotent.
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("second Migrate up: %v", err)
	}

	// --- gate rejects an unknown (newer) version ---
	if _, err := db.ExecContext(ctx,
		"UPDATE rdq_schema_version SET version = $1 WHERE singleton", SchemaVersion+1); err != nil {
		t.Fatalf("bumping schema version: %v", err)
	}
	if err := CheckSchemaVersion(ctx, db); !errors.Is(err, ErrSchemaVersionMismatch) {
		t.Fatalf("gate against newer version: got %v, want ErrSchemaVersionMismatch", err)
	}
	if _, err := db.ExecContext(ctx,
		"UPDATE rdq_schema_version SET version = $1 WHERE singleton", SchemaVersion); err != nil {
		t.Fatalf("restoring schema version: %v", err)
	}

	// --- down ---
	if err := MigrateDown(ctx, db, 0); err != nil {
		t.Fatalf("MigrateDown: %v", err)
	}
	for _, table := range []string{"rdq_task", "rdq_dlq_task", "rdq_attempt", "rdq_schema_version"} {
		if tableExists(ctx, t, db, table) {
			t.Errorf("table %s still present after MigrateDown", table)
		}
	}
	if err := CheckSchemaVersion(ctx, db); !errors.Is(err, ErrSchemaNotInitialized) {
		t.Fatalf("post-down gate: got %v, want ErrSchemaNotInitialized", err)
	}

	// --- up again: clean re-apply proves down was complete ---
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("re-Migrate up after down: %v", err)
	}
	if err := CheckSchemaVersion(ctx, db); err != nil {
		t.Fatalf("gate after re-migrate: %v", err)
	}
}

func tableExists(ctx context.Context, t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(ctx,
		"SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		t.Fatalf("checking table %s: %v", name, err)
	}
	return exists
}

func indexExists(ctx context.Context, t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)", name).Scan(&exists); err != nil {
		t.Fatalf("checking index %s: %v", name, err)
	}
	return exists
}

func recordedVersion(ctx context.Context, t *testing.T, db *sql.DB) int {
	t.Helper()
	var v int
	if err := db.QueryRowContext(ctx,
		"SELECT version FROM rdq_schema_version WHERE singleton").Scan(&v); err != nil {
		t.Fatalf("reading recorded version: %v", err)
	}
	return v
}
