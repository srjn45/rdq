// SPDX-License-Identifier: Apache-2.0

// This file implements the Postgres storage benchmarks used for capacity sizing.
//
// T2.6 — BenchmarkPostgresClaims: contended claim-path throughput target
// ≥1 000 claims/sec on a modest node. Mirrors backend-neutral benchmarkClaims
// in core/compliance/bench_test.go.
//
// T8.3 — BenchmarkPostgresEnqueue: pure enqueue throughput (producer path).
// T8.3 — BenchmarkPostgresClaimContention: claim-throughput scaling across 1,
// GOMAXPROCS, 2×GOMAXPROCS, and 4×GOMAXPROCS concurrent claimers — the FOR
// UPDATE SKIP LOCKED story. All T8.3 results are recorded in BENCHMARKS.md and
// drive the guidance in docs/operations/sizing.md.
//
// Run the full sizing suite:
//
//	cd storage/postgres && go test -run=^$ -bench=. -benchtime=10s -count=3 .

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

// BenchmarkPostgresEnqueue measures pure enqueue throughput (T8.3): how many
// tasks per second the Postgres backend admits under concurrent producers. Each
// iteration inserts exactly one task with a unique ID; no claiming takes place.
// Use this number to size the enqueue path for write-heavy workloads and to
// choose connection-pool capacity on the producer side.
//
// Run:
//
//	cd storage/postgres && go test -run=^$ -bench=BenchmarkPostgresEnqueue \
//	    -benchtime=10s -count=3 .
func BenchmarkPostgresEnqueue(b *testing.B) {
	const queue = "q.bench.enqueue"
	ctx := context.Background()
	s := startMigratedStoreForBench(ctx, b)
	due := time.Now().Add(-time.Minute)

	var idc atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := fmt.Sprintf("bench-enq-%d", idc.Add(1))
			if err := s.Enqueue(ctx, pendingTask(id, queue, due)); err != nil {
				b.Errorf("Enqueue: %v", err)
				return
			}
		}
	})
	b.StopTimer()

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "enqueues/sec")
	}
}

// BenchmarkPostgresClaimContention shows how claim throughput scales with N
// concurrent claimers racing for the same queue — the FOR UPDATE SKIP LOCKED
// story (T8.3). Four sub-benchmarks share one Postgres container:
//
//	serial  — 1 goroutine, plain loop; the non-contended baseline
//	par-1x  — GOMAXPROCS goroutines (SetParallelism(1), the default)
//	par-2x  — 2×GOMAXPROCS goroutines (SetParallelism(2))
//	par-4x  — 4×GOMAXPROCS goroutines (SetParallelism(4))
//
// Each iteration is ClaimDue + DeadLetter + Purge + Enqueue (same accounting
// as BenchmarkPostgresClaims), so claims/sec is directly comparable. Compare
// sub-benchmarks to see how SKIP LOCKED lets workers add aggregate throughput
// without blocking each other.
//
// Run:
//
//	cd storage/postgres && go test -run=^$ \
//	    -bench=BenchmarkPostgresClaimContention -benchtime=10s -count=3 .
func BenchmarkPostgresClaimContention(b *testing.B) {
	ctx := context.Background()
	s := startMigratedStoreForBench(ctx, b) // one container for all sub-benchmarks
	due := time.Now().Add(-time.Minute)

	type subcase struct {
		name string
		par  int // SetParallelism factor; 0 = serial plain loop
	}
	cases := []subcase{
		{"serial", 0},
		{"par-1x", 1},
		{"par-2x", 2},
		{"par-4x", 4},
	}

	for _, tc := range cases {
		tc := tc
		queue := fmt.Sprintf("q.bench.ctn.%s", tc.name)

		for i := 0; i < benchClaimPool; i++ {
			id := fmt.Sprintf("bench-ctn-%s-seed-%05d", tc.name, i)
			if err := s.Enqueue(ctx, pendingTask(id, queue, due)); err != nil {
				b.Fatalf("seed Enqueue (%s): %v", tc.name, err)
			}
		}

		b.Run(tc.name, func(b *testing.B) {
			var idc atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()

			claimCycle := func() bool {
				claimed, err := s.ClaimDue(ctx, queue, 1, benchLease)
				if err != nil {
					b.Errorf("ClaimDue: %v", err)
					return false
				}
				if len(claimed) == 0 {
					return true // pool momentarily empty; a peer will return a task
				}
				c := claimed[0]
				if err := s.DeadLetter(ctx, c.Task.ID, c.Token, pgBenchAttempt()); err != nil {
					b.Errorf("DeadLetter: %v", err)
					return false
				}
				if _, err := s.Purge(ctx, queue, spi.Selector{IDs: []spi.TaskID{c.Task.ID}}); err != nil {
					b.Errorf("Purge: %v", err)
					return false
				}
				replID := fmt.Sprintf("bench-ctn-repl-%d", idc.Add(1))
				if err := s.Enqueue(ctx, pendingTask(replID, queue, due)); err != nil {
					b.Errorf("Enqueue replacement: %v", err)
					return false
				}
				return true
			}

			if tc.par == 0 {
				for i := 0; i < b.N; i++ {
					if !claimCycle() {
						return
					}
				}
			} else {
				b.SetParallelism(tc.par)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if !claimCycle() {
							return
						}
					}
				})
			}
			b.StopTimer()

			if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
				b.ReportMetric(float64(b.N)/elapsed, "claims/sec")
			}
		})
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
