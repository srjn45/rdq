// SPDX-License-Identifier: Apache-2.0

// This file implements design 02 §3 invariant 1 (no double claim). It is a
// regular (non-_test.go) source file, not claims_test.go, on purpose: the
// exported Run in kit.go calls these functions, so they must compile into the
// importable package — a _test.go file is invisible to a plugin in another
// module that imports the kit (e.g. the Postgres binding at T2.6). The backlog's
// "claims_test.go" naming predates that constraint; the behavior it asks for is
// unchanged.
package compliance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// testNoDoubleClaim verifies invariant 1 (design 02 §3): under N concurrent
// claimants every claim is fenced — at most one valid token per task at any
// moment — and a dropped (crashed) worker's task is reclaimable after its lease
// expires, with the dropped worker's token rendered dead.
func testNoDoubleClaim(t *testing.T, factory func() spi.Storage) {
	t.Run("concurrent-exclusivity", func(t *testing.T) { testConcurrentExclusivity(t, factory) })
	t.Run("drop-worker-reclaim", func(t *testing.T) { testDropWorkerReclaim(t, factory) })
}

// testConcurrentExclusivity floods a queue of known size with more claimants
// than there are tasks and asserts the union of everything claimed contains each
// task exactly once (no double claim) with a unique token per claim. With a lease
// long enough that nothing expires mid-run, a claimed task is IN_FLIGHT and no
// longer due, so a compliant backend hands each task to exactly one claimant.
func testConcurrentExclusivity(t *testing.T, factory func() spi.Storage) {
	const (
		queue   = "q.claims"
		tasks   = 200
		workers = 8
	)
	s := factory()
	ctx := context.Background()
	for i := 0; i < tasks; i++ {
		mustEnqueue(t, s, newPendingTask(fmt.Sprintf("t%03d", i), queue))
	}

	var (
		mu         sync.Mutex
		claimCount = make(map[spi.TaskID]int, tasks)
		tokens     = make(map[spi.ClaimToken]int)
		claimErr   error
		wg         sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// limit=1 keeps the claim grain small so many workers race on
				// the same due set rather than each draining a batch.
				claimed, err := s.ClaimDue(ctx, queue, 1, longLease)
				if err != nil {
					mu.Lock()
					if claimErr == nil {
						claimErr = err
					}
					mu.Unlock()
					return
				}
				if len(claimed) == 0 {
					return // queue drained: everything is IN_FLIGHT, nothing due
				}
				mu.Lock()
				for _, c := range claimed {
					claimCount[c.Task.ID]++
					tokens[c.Token]++
					if c.Task.Status != envelope.StatusInFlight && claimErr == nil {
						claimErr = fmt.Errorf("claimed task %s status = %s, want IN_FLIGHT", c.Task.ID, c.Task.Status)
					}
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if claimErr != nil {
		t.Fatalf("concurrent ClaimDue: %v", claimErr)
	}
	// Exactly the enqueued set was claimed, none twice.
	if len(claimCount) != tasks {
		t.Fatalf("distinct tasks claimed = %d, want %d", len(claimCount), tasks)
	}
	for id, n := range claimCount {
		if n != 1 {
			t.Fatalf("task %s claimed %d times, want exactly 1 (double claim)", id, n)
		}
	}
	// Every claim minted a distinct fencing token.
	if len(tokens) != tasks {
		t.Fatalf("distinct tokens = %d, want %d (tokens must be unique per claim)", len(tokens), tasks)
	}
	for tok, n := range tokens {
		if n != 1 {
			t.Fatalf("token %q handed out %d times, want 1", tok, n)
		}
	}
}

// testDropWorkerReclaim simulates a claimant that crashes (never reports an
// outcome). After the lease lapses the task must be reclaimable with a fresh
// token, and the crashed worker's original token must be dead: every mutation it
// attempts fails with ErrStaleClaim. This is the chaos clause of invariant 1.
func testDropWorkerReclaim(t *testing.T, factory func() spi.Storage) {
	const queue = "q.drop"
	s := factory()
	ctx := context.Background()
	mustEnqueue(t, s, newPendingTask("drop", queue))

	// A worker claims, then "crashes" — it never calls Complete/Reschedule/etc.
	dropped := mustClaimOne(t, s, queue, shortLease)

	// Wait past the lease so built-in crash recovery can reclaim the task.
	time.Sleep(expireWait)

	reclaimed := mustClaimOne(t, s, queue, longLease)
	if reclaimed.Token == dropped.Token {
		t.Fatalf("reclaim reused the dropped worker's token %q", dropped.Token)
	}

	// The dropped worker's token is dead.
	assertStaleToken(t, s, "drop", dropped.Token)

	// The reclaiming worker's token still works.
	if err := s.Complete(ctx, "drop", reclaimed.Token, retryAttempt(2, "done")); err != nil {
		t.Fatalf("Complete with reclaimed token: %v", err)
	}
}
