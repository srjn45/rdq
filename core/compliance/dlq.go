// SPDX-License-Identifier: Apache-2.0

// This file implements design 02 §3 invariants 7 (redrive resets, history
// persists) and 8 (stable cursor pagination). See claims.go for why the bodies
// live in a regular .go file rather than dlq_test.go.
package compliance

import (
	"context"
	"testing"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// testRedriveReset verifies invariant 7 (design 02 §3): a redriven DLQ task
// returns to PENDING with attempt_count reset to 0 and redrive_count incremented,
// while its full prior attempt history is preserved and it leaves the DLQ and
// becomes claimable again.
func testRedriveReset(t *testing.T, factory func() spi.Storage) {
	const queue = "q.redrive"
	s := factory()
	ctx := context.Background()

	// Two recorded attempts, then dead-letter, so attempt_count is non-zero and
	// there is real history to preserve.
	mustEnqueue(t, s, newPendingTask("t", queue))
	c := mustClaimOne(t, s, queue, longLease)
	if err := s.Reschedule(ctx, "t", c.Token, retryAttempt(1, "boom"), pastDue()); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}
	c = mustClaimOne(t, s, queue, longLease)
	if err := s.DeadLetter(ctx, "t", c.Token, retryAttempt(2, "boom")); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	before, err := s.Get(ctx, "t")
	if err != nil {
		t.Fatalf("Get before redrive: %v", err)
	}
	if before.AttemptCount != 2 || len(before.Attempts) != 2 {
		t.Fatalf("pre-redrive attempt_count=%d attempts=%d, want 2/2", before.AttemptCount, len(before.Attempts))
	}

	n, err := s.Redrive(ctx, queue, spi.Selector{IDs: []spi.TaskID{"t"}})
	if err != nil {
		t.Fatalf("Redrive: %v", err)
	}
	if n != 1 {
		t.Fatalf("Redrive count = %d, want 1", n)
	}

	after, err := s.Get(ctx, "t")
	if err != nil {
		t.Fatalf("Get after redrive: %v", err)
	}
	if after.Status != envelope.StatusPending {
		t.Fatalf("post-redrive status = %s, want PENDING", after.Status)
	}
	if after.AttemptCount != 0 {
		t.Fatalf("post-redrive attempt_count = %d, want 0", after.AttemptCount)
	}
	if after.RedriveCount != before.RedriveCount+1 {
		t.Fatalf("post-redrive redrive_count = %d, want %d", after.RedriveCount, before.RedriveCount+1)
	}
	if len(after.Attempts) != 2 {
		t.Fatalf("post-redrive history = %d attempts, want 2 (kept)", len(after.Attempts))
	}
	if after.NextAttemptAt == nil {
		t.Fatalf("post-redrive next_attempt_at is nil, want a due task")
	}

	// It has left the DLQ and is claimable again.
	list, _, err := s.DLQList(ctx, queue, spi.DLQFilter{}, spi.Page{})
	if err != nil {
		t.Fatalf("DLQList: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("redriven task still in DLQ: %d entries", len(list))
	}
	if got := mustClaimOne(t, s, queue, longLease); got.Task.ID != "t" {
		t.Fatalf("redriven task not reclaimable, got %q", got.Task.ID)
	}
}

// testStablePagination verifies invariant 8 (design 02 §3): cursor-based DLQList
// paging neither skips nor duplicates entries even when new tasks are dead-
// lettered mid-pagination. Entries arriving after paging began may or may not
// appear, but they must never disturb the entries that predate the cursor.
func testStablePagination(t *testing.T, factory func() spi.Storage) {
	const queue = "q.page"
	s := factory()
	ctx := context.Background()

	original := []string{"d1", "d2", "d3", "d4", "d5"}
	for _, id := range original {
		driveTaskToDLQ(t, s, id, queue, "boom")
	}

	seen := make([]string, 0, len(original)+1)
	var cur spi.Cursor
	pages := 0
	for {
		list, next, err := s.DLQList(ctx, queue, spi.DLQFilter{}, spi.Page{Limit: 2, After: cur})
		if err != nil {
			t.Fatalf("DLQList page %d: %v", pages+1, err)
		}
		for _, e := range list {
			seen = append(seen, e.ID)
		}
		pages++
		if next == "" {
			break
		}
		// A fresh arrival after the first page must not skip or duplicate the
		// entries already established before the cursor.
		if pages == 1 {
			driveTaskToDLQ(t, s, "late", queue, "boom")
		}
		cur = next
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}

	if dup := firstDuplicate(seen); dup != "" {
		t.Fatalf("id %q appeared on more than one page (seen=%v)", dup, seen)
	}
	for _, want := range original {
		if !contains(seen, want) {
			t.Fatalf("original entry %q was skipped across pages (seen=%v)", want, seen)
		}
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func firstDuplicate(xs []string) string {
	seen := make(map[string]bool, len(xs))
	for _, x := range xs {
		if seen[x] {
			return x
		}
		seen[x] = true
	}
	return ""
}
