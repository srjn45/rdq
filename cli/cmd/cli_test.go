// SPDX-License-Identifier: Apache-2.0

package cmd_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	pgmod "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/srjn45/rdq/cli/cmd"
	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
	serverhttp "github.com/srjn45/rdq/server/http"
	"github.com/srjn45/rdq/storage/postgres"
)

// startPostgresDSN starts a throwaway Postgres container and returns the DSN.
// Skips the test when Docker is unavailable or RDQ_SKIP_DOCKER is set.
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
			t.Skipf("Docker unavailable, skipping CLI integration test: %v", err)
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
		if strings.Contains(strings.ToLower(msg), strings.ToLower(needle)) {
			return true
		}
	}
	return false
}

// seedDLQ enqueues n tasks in queue and moves each to the DLQ via DeadLetter.
// Returns the task IDs in insertion order.
func seedDLQ(ctx context.Context, t *testing.T, store *postgres.Store, queue string, n int) []string {
	t.Helper()
	due := time.Now().Add(-time.Minute)
	ids := make([]string, n)
	for i := range n {
		id := fmt.Sprintf("cli-test-%s-%02d", queue, i)
		ids[i] = id
		env := envelope.Envelope{
			EnvelopeVersion:    1,
			ID:                 id,
			Queue:              queue,
			HandlerRef:         "test.handler",
			Payload:            []byte(`{"n":1}`),
			PayloadContentType: "application/json",
			Status:             envelope.StatusPending,
			NextAttemptAt:      &due,
			CreatedAt:          due,
		}
		if err := store.Enqueue(ctx, env); err != nil {
			t.Fatalf("seedDLQ Enqueue[%d]: %v", i, err)
		}
		claimed, err := store.ClaimDue(ctx, queue, 1, time.Hour)
		if err != nil || len(claimed) == 0 {
			t.Fatalf("seedDLQ ClaimDue[%d]: %v", i, err)
		}
		now := time.Now()
		attempt := spi.Attempt{
			AttemptNo:  1,
			StartedAt:  now,
			FinishedAt: &now,
			Outcome:    envelope.OutcomePermanentFailure,
			Error: &envelope.Error{
				Type:    "test.PermError",
				Message: "seeded for CLI test",
			},
		}
		if err := store.DeadLetter(ctx, id, claimed[0].Token, attempt); err != nil {
			t.Fatalf("seedDLQ DeadLetter[%d]: %v", i, err)
		}
	}
	return ids
}

// run calls cmd.Run with the given args and returns (output, error).
func run(args ...string) (string, error) {
	var buf bytes.Buffer
	err := cmd.Run(args, &buf)
	return buf.String(), err
}

// ─────────────────────────────────────────────── integration test suite ──

// cliSuite exercises each CLI command against the given transport flags
// (transportArgs is e.g. []string{"--dsn", dsn} or []string{"--server", url}).
func cliSuite(t *testing.T, queue string, transportArgs []string, taskIDs []string) {
	t.Helper()
	ctx := context.Background()
	_ = ctx

	// stats — before any mutation
	t.Run("stats", func(t *testing.T) {
		out, err := run(append(transportArgs, "stats", queue)...)
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		if !strings.Contains(out, "DLQ depth:") {
			t.Errorf("stats output missing DLQ depth: %q", out)
		}
		if !strings.Contains(out, "3") {
			t.Errorf("stats output should contain DLQ depth of 3: %q", out)
		}
	})

	// dlq list — should return all seeded tasks
	t.Run("dlq list", func(t *testing.T) {
		out, err := run(append(transportArgs, "dlq", "list", queue)...)
		if err != nil {
			t.Fatalf("dlq list: %v", err)
		}
		for _, id := range taskIDs {
			if !strings.Contains(out, id) {
				t.Errorf("dlq list output missing task %q:\n%s", id, out)
			}
		}
	})

	// dlq list with --limit 1 and cursor pagination
	t.Run("dlq list/pagination", func(t *testing.T) {
		out, err := run(append(transportArgs, "dlq", "list", queue, "--limit", "1")...)
		if err != nil {
			t.Fatalf("dlq list --limit 1: %v", err)
		}
		if !strings.Contains(out, "next-cursor:") {
			t.Errorf("dlq list --limit 1 missing next-cursor: %q", out)
		}
	})

	// dlq inspect — single task
	t.Run("dlq inspect", func(t *testing.T) {
		out, err := run(append(transportArgs, "dlq", "inspect", taskIDs[0])...)
		if err != nil {
			t.Fatalf("dlq inspect: %v", err)
		}
		if !strings.Contains(out, taskIDs[0]) {
			t.Errorf("dlq inspect output missing task id %q: %q", taskIDs[0], out)
		}
		if !strings.Contains(out, "DEAD") {
			t.Errorf("dlq inspect output missing DEAD status: %q", out)
		}
		if !strings.Contains(out, "test.handler") {
			t.Errorf("dlq inspect output missing handler_ref: %q", out)
		}
	})

	// dlq redrive — by id
	t.Run("dlq redrive by id", func(t *testing.T) {
		out, err := run(append(transportArgs, "dlq", "redrive", queue, "--id", taskIDs[1])...)
		if err != nil {
			t.Fatalf("dlq redrive: %v", err)
		}
		if !strings.Contains(out, "redrived: 1") {
			t.Errorf("dlq redrive output = %q, want redrived: 1", out)
		}
	})

	// dlq purge — by id
	t.Run("dlq purge by id", func(t *testing.T) {
		out, err := run(append(transportArgs, "dlq", "purge", queue, "--id", taskIDs[2])...)
		if err != nil {
			t.Fatalf("dlq purge: %v", err)
		}
		if !strings.Contains(out, "purged: 1") {
			t.Errorf("dlq purge output = %q, want purged: 1", out)
		}
	})

	// dlq redrive with filter (handler-ref)
	t.Run("dlq redrive by filter", func(t *testing.T) {
		// After the id-based redrive above, taskIDs[1] is back in PENDING.
		// The remaining DLQ tasks all share handler ref "test.handler".
		// Only taskIDs[0] should remain in DLQ at this point.
		out, err := run(append(transportArgs, "dlq", "redrive", queue, "--handler-ref", "test.handler")...)
		if err != nil {
			t.Fatalf("dlq redrive by filter: %v", err)
		}
		if !strings.Contains(out, "redrived:") {
			t.Errorf("dlq redrive by filter output = %q, missing redrived:", out)
		}
	})
}

// ──────────────────────────────────────────────────────── main tests ──

// TestCLI_DirectTransport runs the full CLI suite against the direct Postgres
// transport (--dsn). Gated behind Docker availability.
func TestCLI_DirectTransport(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgresDSN(ctx, t)

	// Apply migrations via CLI first — also tests rdq migrate.
	out, err := run("--dsn", dsn, "migrate")
	if err != nil {
		t.Fatalf("migrate: %v\noutput: %s", err, out)
	}
	if !strings.Contains(out, "migrations applied") {
		t.Errorf("migrate output = %q, want 'migrations applied'", out)
	}

	// Idempotency: second migrate call should also succeed.
	if _, err := run("--dsn", dsn, "migrate"); err != nil {
		t.Fatalf("migrate (idempotent): %v", err)
	}

	db, err := postgres.Open(dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := postgres.New(db)

	const queue = "cli-direct-test"
	ids := seedDLQ(ctx, t, store, queue, 3)
	cliSuite(t, queue, []string{"--dsn", dsn}, ids)
}

// TestCLI_APITransport runs the full CLI suite against the API transport
// (--server). Uses an httptest.Server backed by the postgres storage.
// Gated behind Docker availability.
func TestCLI_APITransport(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgresDSN(ctx, t)

	db, err := postgres.Open(dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store := postgres.New(db)

	// Start an httptest server backed by the postgres store.
	srv := serverhttp.New(serverhttp.WithStorage(store))
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	const queue = "cli-api-test"
	ids := seedDLQ(ctx, t, store, queue, 3)
	cliSuite(t, queue, []string{"--server", ts.URL}, ids)
}

// TestCLI_Migrate_DirectOnly verifies that `rdq migrate` fails in API mode.
func TestCLI_Migrate_DirectOnly(t *testing.T) {
	ts := httptest.NewServer(serverhttp.New())
	defer ts.Close()

	_, err := run("--server", ts.URL, "migrate")
	if err == nil {
		t.Fatal("migrate in API mode should fail, but succeeded")
	}
	if !strings.Contains(err.Error(), "direct-storage mode") {
		t.Errorf("migrate in API mode error = %q, want 'direct-storage mode' hint", err.Error())
	}
}

// TestCLI_MissingTransport verifies that the CLI errors without --server or --dsn.
func TestCLI_MissingTransport(t *testing.T) {
	_, err := run("stats", "my-queue")
	if err == nil {
		t.Fatal("expected error without --server or --dsn")
	}
}

// TestCLI_BothTransports verifies that specifying both --server and --dsn is rejected.
func TestCLI_BothTransports(t *testing.T) {
	_, err := run("--server", "http://localhost:8080", "--dsn", "postgres://localhost/rdq", "stats", "q")
	if err == nil {
		t.Fatal("expected error with both --server and --dsn")
	}
}

// TestCLI_EmptySelector verifies that redrive/purge with no selector returns 0.
// No Postgres needed — an empty selector short-circuits before any storage call.
func TestCLI_EmptySelector(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgresDSN(ctx, t)

	db, err := postgres.Open(dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Redrive with empty selector → count 0
	out, err := run("--dsn", dsn, "dlq", "redrive", "empty-queue")
	if err != nil {
		t.Fatalf("redrive empty selector: %v", err)
	}
	if !strings.Contains(out, "redrived: 0") {
		t.Errorf("empty-selector redrive = %q, want redrived: 0", out)
	}
}
