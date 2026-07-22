// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// TestChaos_MultiWorkerNoDoubleClaim is the T8.1 scale chaos proof (PRD §11): N
// concurrent workers claim and process from ONE queue against a real Postgres,
// and a bounded fraction of them are kill-9'd mid-processing — their database
// connection is dropped WITHOUT a graceful lease release while the task is
// IN_FLIGHT. The test then asserts the two safety properties the fenced
// claim/lease design (design 02 §3) promises under this abuse:
//
//   - zero double-claims: no task's live-lease window overlaps another's, and
//     each task reaches its terminal SUCCEEDED outcome exactly once; and
//   - reclaim-after-lease: a killed worker's orphaned task becomes claimable
//     again only AFTER its lease expires, records a LEASE_EXPIRED attempt, and
//     is then completed by a surviving worker.
//
// It uses exactly ONE Postgres container for the whole run and modest
// worker/task counts (enough to exercise the property without OOMing a tight
// host); orchestration is deterministic (kill budget + fenced tokens), not
// probabilistic. The recorded metric (tasks, kills, reclaims, double-claims=0)
// is logged and optionally written to $RDQ_CHAOS_ARTIFACT.
func TestChaos_MultiWorkerNoDoubleClaim(t *testing.T) {
	const (
		queue       = "q.chaos"
		numWorkers  = 8
		numTasks    = 300
		killBudget  = 40                     // ≤ numTasks; each killed once, always reclaimable
		lease       = 750 * time.Millisecond // comfortably longer than a survivor's ~instant processing
		poll        = 20 * time.Millisecond  // idle backoff while killed tasks wait out their leases
		runDeadline = 90 * time.Second       // safety net; the property guarantees far quicker drain
	)

	ctx := context.Background()
	dsn := startPostgresDSN(ctx, t)

	// Admin connection: migrate once and seed the whole queue up front.
	admin, err := Open(dsn)
	if err != nil {
		t.Fatalf("open admin db: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := Migrate(ctx, admin); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// Start from a clean slate so the run is self-contained and repeatable even
	// against a reused database (a fresh testcontainers instance is already empty).
	if _, err := admin.ExecContext(ctx,
		"TRUNCATE rdq_task, rdq_dlq_task, rdq_attempt RESTART IDENTITY"); err != nil {
		t.Fatalf("truncate for clean slate: %v", err)
	}

	adminStore := New(admin)
	due := time.Now().Add(-time.Minute)
	for i := 0; i < numTasks; i++ {
		id := fmt.Sprintf("chaos-%05d", i)
		if err := adminStore.Enqueue(ctx, pendingTask(id, queue, due)); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}

	tr := newChaosTracker()
	var (
		completed atomic.Int64 // successful terminal Completes across all workers
		kills     atomic.Int64 // kill-9 events performed
		budget    atomic.Int64 // remaining kill budget
	)
	budget.Store(killBudget)

	deadline := time.Now().Add(runDeadline)
	start := time.Now()

	var (
		wg      sync.WaitGroup
		errMu   sync.Mutex
		workErr error
	)
	fail := func(err error) {
		errMu.Lock()
		if workErr == nil {
			workErr = err
		}
		errMu.Unlock()
	}

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			if err := runChaosWorker(workerID, dsn, queue, lease, poll, numTasks,
				deadline, tr, &completed, &kills, &budget); err != nil {
				fail(err)
			}
		}(w)
	}
	wg.Wait()

	if workErr != nil {
		t.Fatalf("chaos worker failed: %v", workErr)
	}

	elapsed := time.Since(start)

	// --- Property 1: every task terminally SUCCEEDED exactly once -----------
	doubleCompletes := 0
	for id, n := range tr.completes {
		if n != 1 {
			doubleCompletes++
			t.Errorf("task %s completed %d times, want exactly once", id, n)
		}
	}
	if got := int(completed.Load()); got != numTasks {
		t.Errorf("total completes = %d, want %d", got, numTasks)
	}
	if tr.staleCompletes != 0 {
		// A survivor's Complete hitting ErrStaleClaim means its live lease was
		// stolen mid-processing — a genuine double-claim (or a lease far too
		// short for the work). Either way the exactly-once property is broken.
		t.Errorf("stale completes = %d, want 0 (a live lease was reclaimed under a worker)", tr.staleCompletes)
	}

	// --- Property 2: no overlapping live leases; reclaim only after expiry ---
	doubleClaims := 0
	reclaims := 0
	for id, claims := range tr.claims {
		// Order a task's claims by when the DB minted them (lease_expires_at -
		// lease is the DB-clock claim instant, immune to client-clock skew).
		sort.Slice(claims, func(i, j int) bool {
			return claims[i].claimAt().Before(claims[j].claimAt())
		})
		for i := 1; i < len(claims); i++ {
			prev, cur := claims[i-1], claims[i]
			reclaims++
			// The only way a task is claimed twice is a reclaim of an expired
			// lease: a completed task is SUCCEEDED (never re-claimable) and this
			// harness never reschedules. So every re-claim MUST begin at or after
			// the prior claim's lease expiry — otherwise two live leases overlapped.
			if cur.claimAt().Before(prev.leaseExp) {
				doubleClaims++
				t.Errorf("task %s: claim by worker %d at %s overlaps live lease of worker %d (expires %s)",
					id, cur.workerID, cur.claimAt().Format(time.RFC3339Nano),
					prev.workerID, prev.leaseExp.Format(time.RFC3339Nano))
			}
			// The reclaiming claim must have observed the LEASE_EXPIRED bookkeeping
			// (design 02 §3 invariant 3): attempt_count advanced past the dead attempt.
			if cur.attemptCnt < 1 {
				t.Errorf("task %s: reclaim by worker %d saw attempt_count=%d, want ≥1 (LEASE_EXPIRED not recorded)",
					id, cur.workerID, cur.attemptCnt)
			}
		}
	}
	if doubleClaims != 0 {
		t.Errorf("double-claims = %d, want 0", doubleClaims)
	}
	// Every kill orphans exactly one task, which is reclaimed exactly once.
	if int64(reclaims) != kills.Load() {
		t.Errorf("reclaims = %d, want %d (one per kill)", reclaims, kills.Load())
	}
	if kills.Load() == 0 {
		t.Error("no kills executed — the chaos harness did not exercise the reclaim path")
	}

	// --- Terminal DB cross-check: the whole queue drained to SUCCEEDED -------
	assertQueueFullySucceeded(ctx, t, admin, queue, numTasks)
	assertLeaseExpiredRecorded(ctx, t, admin, int(kills.Load()))

	// --- Record the metric (PRD §11) ----------------------------------------
	metric := chaosMetric{
		Test:         t.Name(),
		Workers:      numWorkers,
		Tasks:        numTasks,
		Kills:        int(kills.Load()),
		Reclaims:     reclaims,
		DoubleClaims: doubleClaims,
		Completed:    int(completed.Load()),
		LeaseMillis:  lease.Milliseconds(),
		ElapsedMs:    elapsed.Milliseconds(),
	}
	recordChaosMetric(t, metric)
}

// chaosClaim is one recorded claim of a task: the DB-authoritative lease expiry
// and the attempt_count seen at claim time are enough to reconstruct the
// live-lease window and detect any overlap without trusting the client clock.
type chaosClaim struct {
	workerID   int
	token      spi.ClaimToken
	leaseExp   time.Time
	attemptCnt int
	lease      time.Duration
}

// claimAt reconstructs the DB-clock instant the claim was minted: the server set
// lease_expires_at = now() + lease, so now() = lease_expires_at - lease. Both
// ends of the comparison in the overlap check derive from the same DB clock.
func (c chaosClaim) claimAt() time.Time { return c.leaseExp.Add(-c.lease) }

type chaosTracker struct {
	mu             sync.Mutex
	claims         map[string][]chaosClaim
	completes      map[string]int
	staleCompletes int
}

func newChaosTracker() *chaosTracker {
	return &chaosTracker{
		claims:    make(map[string][]chaosClaim),
		completes: make(map[string]int),
	}
}

func (tr *chaosTracker) recordClaim(id string, c chaosClaim) {
	tr.mu.Lock()
	tr.claims[id] = append(tr.claims[id], c)
	tr.mu.Unlock()
}

func (tr *chaosTracker) recordComplete(id string) {
	tr.mu.Lock()
	tr.completes[id]++
	tr.mu.Unlock()
}

func (tr *chaosTracker) recordStaleComplete() {
	tr.mu.Lock()
	tr.staleCompletes++
	tr.mu.Unlock()
}

// runChaosWorker drives one worker until the whole queue has drained (or the
// safety deadline trips). Each worker owns its own connection pool so a kill can
// drop that pool the way kill -9 drops a process's sockets, and a "restarted"
// worker comes up on a fresh pool — all against the one shared Postgres.
func runChaosWorker(
	workerID int, dsn, queue string, lease, poll time.Duration, numTasks int,
	deadline time.Time, tr *chaosTracker,
	completed, kills, budget *atomic.Int64,
) error {
	db, err := openChaosPool(dsn)
	if err != nil {
		return fmt.Errorf("worker %d: open pool: %w", workerID, err)
	}
	defer func() { _ = db.Close() }()
	s := New(db)
	ctx := context.Background()

	for {
		if completed.Load() >= int64(numTasks) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("worker %d: run deadline exceeded with %d/%d tasks completed",
				workerID, completed.Load(), numTasks)
		}

		claimed, err := s.ClaimDue(ctx, queue, 1, lease)
		if err != nil {
			return fmt.Errorf("worker %d: ClaimDue: %w", workerID, err)
		}
		if len(claimed) == 0 {
			time.Sleep(poll) // no due work: wait for a killed task's lease to lapse
			continue
		}

		c := claimed[0]
		if c.Task.LeaseExpiresAt == nil {
			return fmt.Errorf("worker %d: claimed %s without a lease", workerID, c.Task.ID)
		}
		tr.recordClaim(c.Task.ID, chaosClaim{
			workerID:   workerID,
			token:      c.Token,
			leaseExp:   *c.Task.LeaseExpiresAt,
			attemptCnt: c.Task.AttemptCount,
			lease:      lease,
		})

		// Kill only on a task's FIRST claim, and only while budget remains: this
		// keeps liveness deterministic — a killed task always carries
		// attempt_count ≥ 1 on reclaim, so its reclaimer never kills it again and
		// always drives it to completion.
		if c.Task.AttemptCount == 0 && budget.Add(-1) >= 0 {
			kills.Add(1)
			// Simulate kill -9: drop the connection pool with the task still
			// IN_FLIGHT and no outcome reported. The lease will lapse and a
			// survivor reclaims it.
			_ = db.Close()
			db, err = openChaosPool(dsn) // the replacement worker process
			if err != nil {
				return fmt.Errorf("worker %d: reopen pool after kill: %w", workerID, err)
			}
			s = New(db)
			continue
		}

		// Survivor: process (instantly) and report success under the live token.
		now := time.Now()
		att := envelope.Attempt{
			AttemptNo:  c.Task.AttemptCount + 1,
			StartedAt:  now,
			FinishedAt: &now,
			Outcome:    envelope.OutcomeSuccess,
		}
		if err := s.Complete(ctx, spi.TaskID(c.Task.ID), c.Token, att); err != nil {
			if errors.Is(err, spi.ErrStaleClaim) {
				// Our live lease was reclaimed under us — recorded and asserted 0
				// by the caller (would indicate a double-claim / too-short lease).
				tr.recordStaleComplete()
				continue
			}
			return fmt.Errorf("worker %d: Complete %s: %w", workerID, c.Task.ID, err)
		}
		tr.recordComplete(c.Task.ID)
		completed.Add(1)
	}
}

// openChaosPool opens a small, bounded connection pool for one worker. The cap
// keeps N workers (each reopening across kills) well under Postgres's default
// max_connections on a tight host.
func openChaosPool(dsn string) (*sql.DB, error) {
	db, err := Open(dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Minute)
	return db, nil
}

// assertQueueFullySucceeded is the terminal cross-check straight from the DB:
// the whole queue has drained to SUCCEEDED with nothing left PENDING/IN_FLIGHT
// and nothing dead-lettered.
func assertQueueFullySucceeded(ctx context.Context, t *testing.T, db *sql.DB, queue string, want int) {
	t.Helper()
	var succeeded, other, dead int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FILTER (WHERE status = 'SUCCEEDED'),
		        count(*) FILTER (WHERE status <> 'SUCCEEDED')
		   FROM rdq_task WHERE queue = $1`, queue).Scan(&succeeded, &other); err != nil {
		t.Fatalf("counting terminal task states: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM rdq_dlq_task WHERE queue = $1`, queue).Scan(&dead); err != nil {
		t.Fatalf("counting DLQ: %v", err)
	}
	if succeeded != want {
		t.Errorf("SUCCEEDED tasks = %d, want %d", succeeded, want)
	}
	if other != 0 {
		t.Errorf("non-SUCCEEDED tasks remaining = %d, want 0", other)
	}
	if dead != 0 {
		t.Errorf("dead-lettered tasks = %d, want 0 (chaos never exhausts retries)", dead)
	}
}

// assertLeaseExpiredRecorded confirms the reclaim path recorded exactly one
// LEASE_EXPIRED attempt per kill (design 02 §3 invariant 3).
func assertLeaseExpiredRecorded(ctx context.Context, t *testing.T, db *sql.DB, wantKills int) {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM rdq_attempt WHERE outcome = 'LEASE_EXPIRED'`).Scan(&n); err != nil {
		t.Fatalf("counting LEASE_EXPIRED attempts: %v", err)
	}
	if n != wantKills {
		t.Errorf("LEASE_EXPIRED attempts = %d, want %d (one per kill)", n, wantKills)
	}
}

// chaosMetric is the recorded PRD §11 result: the headline is DoubleClaims == 0
// across Tasks tasks under Kills kill-9 events.
type chaosMetric struct {
	Test         string `json:"test"`
	Workers      int    `json:"workers"`
	Tasks        int    `json:"tasks"`
	Kills        int    `json:"kills"`
	Reclaims     int    `json:"reclaims"`
	DoubleClaims int    `json:"double_claims"`
	Completed    int    `json:"completed"`
	LeaseMillis  int64  `json:"lease_ms"`
	ElapsedMs    int64  `json:"elapsed_ms"`
}

// recordChaosMetric logs the metric into the test output and, when
// $RDQ_CHAOS_ARTIFACT names a path, writes it there as a JSON artifact for CI to
// collect.
func recordChaosMetric(t *testing.T, m chaosMetric) {
	t.Helper()
	blob, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("marshal chaos metric: %v", err)
	}
	t.Logf("T8.1 chaos metric (PRD §11): %s", blob)
	t.Logf("RESULT: %d tasks, %d workers, %d kill-9 events, %d reclaims, double-claims=%d",
		m.Tasks, m.Workers, m.Kills, m.Reclaims, m.DoubleClaims)

	if path := os.Getenv("RDQ_CHAOS_ARTIFACT"); path != "" {
		if err := os.WriteFile(path, append(blob, '\n'), 0o644); err != nil {
			t.Fatalf("writing chaos artifact to %s: %v", path, err)
		}
		t.Logf("chaos metric artifact written to %s", path)
	}
}
