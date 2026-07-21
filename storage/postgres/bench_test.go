// SPDX-License-Identifier: Apache-2.0

// This file implements the contended claim-path benchmark for the Postgres
// storage backend (T2.6). The benchmark body mirrors the backend-neutral
// benchmarkClaims defined in core/compliance/bench_test.go; it is re-stated
// here because that function is unexported and in a test file. The throughput
// target is ≥1 000 claims/sec on a modest node (design 06 T2.6); the measured
// result is recorded in the top-level BENCHMARKS.md.
//
// Run:
//
//	cd storage/postgres && go test -run=^$ -bench=BenchmarkPostgresClaims \
//	    -benchtime=30s -count=3 .

package postgres

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
	"github.com/testcontainers/testcontainers-go"
	pgmod "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// benchClaimPool is the number of due tasks kept resident during the benchmark.
// Large enough that ClaimDue almost always finds work (measures steady-state
// contention, not queue starvation); small enough that the Postgres due-index
// scan stays cheap. Each claimed task is resolved and replaced so the pool
// stays flat across b.N iterations.
const benchClaimPool = 256

// benchLease is the lease handed out during the benchmark. Long enough that
// no task expires mid-run and distorts the resident-pool size.
const benchLease = 10 * time.Second

// BenchmarkPostgresClaims measures contended claim-path throughput on a real
// Postgres backend (T2.6 acceptance: ≥1 000 claims/sec on a modest node). Each
// iteration is exactly one ClaimDue + DeadLetter + Purge + Enqueue, so
// ops/sec == claims/sec as reported by the "claims/sec" custom metric.
func BenchmarkPostgresClaims(b *testing.B) {
	const queue = "q.bench.claims"

	ctx := context.Background()
	s := startMigratedStoreForBench(ctx, b)

	due := time.Now().Add(-time.Minute)
	for i := 0; i < benchClaimPool; i++ {
		id := fmt.Sprintf("bench-seed-%05d", i)
		if err := s.Enqueue(ctx, pendingTask(id, queue, due)); err != nil {
			b.Fatalf("seed Enqueue: %v", err)
		}
	}

	var idc atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// limit=1 keeps the claim grain small so parallel workers genuinely
			// contend on the same due set rather than each draining a batch.
			claimed, err := s.ClaimDue(ctx, queue, 1, benchLease)
			if err != nil {
				b.Errorf("ClaimDue: %v", err)
				return
			}
			if len(claimed) == 0 {
				// Momentarily every task is held; the pool is sized so this is
				// rare. Skip — a peer will return it.
				continue
			}
			c := claimed[0]
			att := pgBenchAttempt()
			if err := s.DeadLetter(ctx, c.Task.ID, c.Token, att); err != nil {
				b.Errorf("DeadLetter: %v", err)
				return
			}
			if _, err := s.Purge(ctx, queue, spi.Selector{IDs: []spi.TaskID{c.Task.ID}}); err != nil {
				b.Errorf("Purge: %v", err)
				return
			}
			replID := fmt.Sprintf("bench-repl-%d", idc.Add(1))
			if err := s.Enqueue(ctx, pendingTask(replID, queue, due)); err != nil {
				b.Errorf("Enqueue replacement: %v", err)
				return
			}
		}
	})
	b.StopTimer()

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "claims/sec")
	}
}

// pgBenchAttempt returns a minimal attempt record for benchmark bookkeeping.
func pgBenchAttempt() envelope.Attempt {
	now := time.Now()
	fin := now
	return envelope.Attempt{
		AttemptNo: 1, StartedAt: now, FinishedAt: &fin,
		Outcome: envelope.OutcomeRetryableFailure,
		Error:   &envelope.Error{Type: "bench.done", Message: "benchmark resolution"},
	}
}

// startMigratedStoreForBench starts a throwaway Postgres via testcontainers,
// applies migrations, and returns a ready Store. It skips the benchmark (not
// fails) when Docker is unavailable.
func startMigratedStoreForBench(ctx context.Context, b *testing.B) *Store {
	b.Helper()
	if os.Getenv("RDQ_SKIP_DOCKER") != "" {
		b.Skip("RDQ_SKIP_DOCKER set; skipping testcontainers Postgres benchmark")
	}

	ctr, err := pgmod.Run(ctx, "postgres:16-alpine",
		pgmod.WithDatabase("rdq"),
		pgmod.WithUsername("rdq"),
		pgmod.WithPassword("rdq"),
		pgmod.BasicWaitStrategies(),
	)
	if err != nil {
		if isDockerUnavailable(err) {
			b.Skipf("Docker unavailable, skipping Postgres benchmark: %v", err)
		}
		b.Fatalf("starting postgres container: %v", err)
	}
	b.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = testcontainers.TerminateContainer(ctr, testcontainers.StopContext(stopCtx))
	})

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatalf("connection string: %v", err)
	}
	db, err := Open(dsn)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(ctx); err != nil {
		b.Fatalf("ping db: %v", err)
	}
	if err := Migrate(ctx, db); err != nil {
		b.Fatalf("Migrate: %v", err)
	}
	return New(db)
}
