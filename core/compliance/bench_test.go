// SPDX-License-Identifier: Apache-2.0

// This file adds the compliance kit's contention benchmark (backlog T1.8). Like
// the invariant drivers in claims.go, the benchmark body is backend-neutral: it
// is parameterized by the same `factory func() spi.Storage` the kit's Run uses,
// so a real backend is measured by driving the identical claim path through it.
// The Postgres binding (M2 / T2.6) reuses this exact shape against a
// testcontainers database; here it runs against memstore.
//
// Throughput target. On Postgres this benchmark is held to **>= 1,000
// claims/sec** on a modest node — the sustained contended-claim throughput the
// v1 storage contract promises (design 06 T1.8 / T2.6). memstore is an
// in-process map behind one mutex, so it reports far higher; that number is NOT
// the contract and is NOT recorded anywhere as a result. Its only job is to keep
// the benchmark compiled, wired into the kit, and exercised in core CI so the
// harness cannot bit-rot before the real number lands on Postgres (the figure
// M2 commits to storage/postgres/BENCHMARKS.md, per T2.6). Read `claims/sec` off
// the `-bench=Claims` output: each iteration is exactly one claim + its
// resolving outcome, so ops/sec == claims/sec (also reported explicitly via
// ReportMetric).
package compliance

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/srjn45/rdq/core/memstore"
	"github.com/srjn45/rdq/core/spi"
)

// BenchmarkClaims measures contended claim-path throughput against the memstore
// reference store — the kit's standing subject. It is the memstore instantiation
// of benchmarkClaims, the backend-neutral body a real backend (Postgres, T2.6)
// runs with its own factory.
func BenchmarkClaims(b *testing.B) {
	benchmarkClaims(b, func() spi.Storage { return memstore.New() })
}

// benchClaimPool is the number of due tasks kept resident during the benchmark.
// It is large relative to the parallel claimant count so ClaimDue almost always
// has due work to hand out (the measurement reflects steady-state contention on
// the claim path, not queue starvation), yet small enough that the reference
// store's per-claim due scan stays cheap. Each claimed task is resolved and
// replaced by a fresh one, so the resident set — and thus memory and scan cost —
// stays flat across b.N iterations rather than growing with it.
const benchClaimPool = 256

// benchmarkClaims times the hot claim path under concurrent claimants: ClaimDue,
// the DeadLetter that resolves the claim, and the Enqueue of a replacement that
// keeps the pool due. Many workers racing for the same due set, each fenced to
// exactly one task, is precisely the contention the SPI's claim semantics must
// survive at speed — what the >=1k claims/sec target measures on a real backend.
//
// It is factored out and parameterized by factory so any spi.Storage is driven
// through the identical loop. Resolving each claim with DeadLetter + Purge (then
// replenishing with a fresh task) keeps the resident set flat and history-free,
// so neither the reference store's memory nor its per-claim scan cost drifts
// across the run and the throughput number reflects a genuine steady state.
func benchmarkClaims(b *testing.B, factory func() spi.Storage) {
	const queue = "q.bench.claims"
	s := factory()
	ctx := context.Background()

	// Seed the resident working set of immediately-due tasks (untimed).
	for i := 0; i < benchClaimPool; i++ {
		if err := s.Enqueue(ctx, newPendingTask(fmt.Sprintf("bench-seed-%05d", i), queue)); err != nil {
			b.Fatalf("seed Enqueue: %v", err)
		}
	}

	// Unique ids for the replacement tasks each worker enqueues, so a completed
	// id is never reused (re-enqueue of a live id is an idempotent no-op).
	var idc atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// limit=1 keeps the claim grain small so parallel workers genuinely
			// contend on the same due set rather than each draining a batch.
			claimed, err := s.ClaimDue(ctx, queue, 1, longLease)
			if err != nil {
				b.Errorf("ClaimDue: %v", err)
				return
			}
			if len(claimed) == 0 {
				// Momentarily every task is held by a peer; the pool is sized so
				// this is rare. Skip without resolving — the peer will return it.
				continue
			}
			c := claimed[0]
			// Resolve the claim, then drop the task and drop in a replacement so
			// the resident set stays flat and carries no growing history.
			if err := s.DeadLetter(ctx, c.Task.ID, c.Token, retryAttempt(1, "bench.done")); err != nil {
				b.Errorf("DeadLetter: %v", err)
				return
			}
			if _, err := s.Purge(ctx, queue, spi.Selector{IDs: []spi.TaskID{c.Task.ID}}); err != nil {
				b.Errorf("Purge: %v", err)
				return
			}
			replID := fmt.Sprintf("bench-repl-%d", idc.Add(1))
			if err := s.Enqueue(ctx, newPendingTask(replID, queue)); err != nil {
				b.Errorf("Enqueue replacement: %v", err)
				return
			}
		}
	})
	b.StopTimer()

	// Report throughput in the contract's own unit so the >=1k claims/sec target
	// (checked on Postgres at T2.6) is read directly off the benchmark output.
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "claims/sec")
	}
}
