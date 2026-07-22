// SPDX-License-Identifier: Apache-2.0

package memstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// testClock is a manually advanced clock so due-ness and lease expiry are
// deterministic (the store is the time authority, G9).
type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time          { return c.now }
func (c *testClock) advance(d time.Duration) { c.now = c.now.Add(d) }

// newStore builds a store whose clock starts at a fixed instant.
func newStore() (*Store, *testClock) {
	clk := &testClock{now: time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)}
	return New(WithClock(clk.Now)), clk
}

// pendingTask builds a PENDING envelope due at dueAt.
func pendingTask(id, queue string, dueAt time.Time) envelope.Envelope {
	return envelope.Envelope{
		EnvelopeVersion:    1,
		ID:                 id,
		Queue:              queue,
		HandlerRef:         "h.process",
		Payload:            []byte("payload-" + id),
		PayloadContentType: "application/octet-stream",
		Status:             envelope.StatusPending,
		NextAttemptAt:      &dueAt,
		CreatedAt:          dueAt,
	}
}

// failAttempt builds a retryable-failure attempt record.
func failAttempt(no int, at time.Time, errType string) spi.Attempt {
	fin := at
	return envelope.Attempt{
		AttemptNo:  no,
		StartedAt:  at,
		FinishedAt: &fin,
		Outcome:    envelope.OutcomeRetryableFailure,
		Error:      &envelope.Error{Type: errType, Message: "boom"},
	}
}

func mustClaimOne(t *testing.T, s *Store, queue string, lease time.Duration) spi.Claimed {
	t.Helper()
	claimed, err := s.ClaimDue(context.Background(), queue, 10, lease)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("ClaimDue returned %d tasks, want 1", len(claimed))
	}
	return claimed[0]
}

func TestCapabilitiesAllFalse(t *testing.T) {
	s, _ := newStore()
	if got := s.Capabilities(); got != (spi.Capabilities{}) {
		t.Fatalf("Capabilities() = %+v, want all-false zero value", got)
	}
}

func TestEnqueueIdempotentSameQueue(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	task := pendingTask("t1", "q1", clk.now)

	if err := s.Enqueue(ctx, task); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	// Re-enqueue in the same queue is a no-op and must not disturb existing
	// state (e.g. an already-advanced attempt_count).
	claimed := mustClaimOne(t, s, "q1", time.Minute)
	if err := s.Reschedule(ctx, "t1", claimed.Token, failAttempt(1, clk.now, "E"), clk.now); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}
	if err := s.Enqueue(ctx, task); err != nil {
		t.Fatalf("re-Enqueue same queue: %v", err)
	}
	got, err := s.Get(ctx, "t1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.AttemptCount != 1 || len(got.Attempts) != 1 {
		t.Fatalf("re-enqueue clobbered state: attempt_count=%d attempts=%d, want 1/1", got.AttemptCount, len(got.Attempts))
	}
}

func TestEnqueueCrossQueueConflict(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	if err := s.Enqueue(ctx, pendingTask("dup", "q1", clk.now)); err != nil {
		t.Fatalf("Enqueue q1: %v", err)
	}
	err := s.Enqueue(ctx, pendingTask("dup", "q2", clk.now))
	if !errors.Is(err, spi.ErrIDConflict) {
		t.Fatalf("cross-queue Enqueue error = %v, want ErrIDConflict", err)
	}
}

func TestClaimDuePendingAndOrdering(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	// Two due now, one due in the future.
	early := clk.now.Add(-2 * time.Minute)
	late := clk.now.Add(-1 * time.Minute)
	future := clk.now.Add(time.Hour)
	_ = s.Enqueue(ctx, pendingTask("b", "q", late))
	_ = s.Enqueue(ctx, pendingTask("a", "q", early))
	_ = s.Enqueue(ctx, pendingTask("c", "q", future))

	claimed, err := s.ClaimDue(ctx, "q", 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d, want 2 (future task not due)", len(claimed))
	}
	if claimed[0].Task.ID != "a" || claimed[1].Task.ID != "b" {
		t.Fatalf("order = %s,%s, want a,b (by next_attempt_at asc)", claimed[0].Task.ID, claimed[1].Task.ID)
	}
	for _, c := range claimed {
		if c.Task.Status != envelope.StatusInFlight {
			t.Fatalf("claimed task %s status = %s, want IN_FLIGHT", c.Task.ID, c.Task.Status)
		}
		if c.Task.LeaseExpiresAt == nil {
			t.Fatalf("claimed task %s has nil lease", c.Task.ID)
		}
		if c.Token == "" {
			t.Fatalf("claimed task %s has empty token", c.Task.ID)
		}
	}
	if claimed[0].Token == claimed[1].Token {
		t.Fatalf("tokens not unique: %q", claimed[0].Token)
	}
}

func TestClaimDueLimit(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		_ = s.Enqueue(ctx, pendingTask(id, "q", clk.now))
	}
	claimed, err := s.ClaimDue(ctx, "q", 2, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("limit not honored: got %d, want 2", len(claimed))
	}
}

func TestFencingStaleTokenRejected(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	_ = s.Enqueue(ctx, pendingTask("t", "q", clk.now))
	first := mustClaimOne(t, s, "q", time.Minute)

	// Lease expires; a fresh claim reclaims the task and mints a new token.
	clk.advance(2 * time.Minute)
	second := mustClaimOne(t, s, "q", time.Minute)
	if second.Token == first.Token {
		t.Fatalf("reclaim reused token %q", first.Token)
	}

	// The old token is now dead: every outcome call must fail with
	// ErrStaleClaim and change nothing.
	att := failAttempt(9, clk.now, "E")
	for name, call := range map[string]func() error{
		"Reschedule":  func() error { return s.Reschedule(ctx, "t", first.Token, att, clk.now) },
		"Complete":    func() error { return s.Complete(ctx, "t", first.Token, att) },
		"DeadLetter":  func() error { return s.DeadLetter(ctx, "t", first.Token, att) },
		"ExtendLease": func() error { return s.ExtendLease(ctx, "t", first.Token, time.Minute) },
	} {
		if err := call(); !errors.Is(err, spi.ErrStaleClaim) {
			t.Fatalf("%s with stale token = %v, want ErrStaleClaim", name, err)
		}
	}

	// No state change: the task is still IN_FLIGHT under the second claim, and
	// its history holds only the single LEASE_EXPIRED reclaim record.
	got, err := s.Get(ctx, "t")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != envelope.StatusInFlight {
		t.Fatalf("status = %s, want IN_FLIGHT (stale calls must not mutate)", got.Status)
	}
	if len(got.Attempts) != 1 {
		t.Fatalf("attempts = %d, want 1 (only the reclaim record)", len(got.Attempts))
	}

	// The valid (second) token still works.
	if err := s.Complete(ctx, "t", second.Token, failAttempt(2, clk.now, "E")); err != nil {
		t.Fatalf("Complete with valid token: %v", err)
	}
}

func TestLeaseReclaimAppendsLeaseExpired(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	_ = s.Enqueue(ctx, pendingTask("t", "q", clk.now))

	first := mustClaimOne(t, s, "q", time.Minute)
	claimStart := clk.now

	// Before expiry the task is not due (lease still held).
	clk.advance(30 * time.Second)
	if c, _ := s.ClaimDue(ctx, "q", 10, time.Minute); len(c) != 0 {
		t.Fatalf("task claimable while lease held: got %d", len(c))
	}

	// After expiry it is due again; reclaim appends a LEASE_EXPIRED attempt.
	clk.advance(time.Minute)
	reclaim := mustClaimOne(t, s, "q", time.Minute)
	if reclaim.Token == first.Token {
		t.Fatalf("reclaim reused old token")
	}
	if len(reclaim.Task.Attempts) != 1 {
		t.Fatalf("attempts after reclaim = %d, want 1", len(reclaim.Task.Attempts))
	}
	a := reclaim.Task.Attempts[0]
	if a.Outcome != envelope.OutcomeLeaseExpired {
		t.Fatalf("reclaim attempt outcome = %s, want LEASE_EXPIRED", a.Outcome)
	}
	if a.Error == nil || a.Error.Type != "rdq.LeaseExpired" {
		t.Fatalf("reclaim attempt error = %+v, want type rdq.LeaseExpired", a.Error)
	}
	if !a.StartedAt.Equal(claimStart) {
		t.Fatalf("reclaim attempt StartedAt = %v, want claim start %v", a.StartedAt, claimStart)
	}
	if reclaim.Task.AttemptCount != 1 {
		t.Fatalf("attempt_count after reclaim = %d, want 1 (lease expiry counts)", reclaim.Task.AttemptCount)
	}
}

func TestGetAnyStatusAndNotFound(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()

	if _, err := s.Get(ctx, "missing"); !errors.Is(err, spi.ErrNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrNotFound", err)
	}

	_ = s.Enqueue(ctx, pendingTask("t", "q", clk.now))
	if got, _ := s.Get(ctx, "t"); got.Status != envelope.StatusPending {
		t.Fatalf("PENDING Get status = %s", got.Status)
	}
	c := mustClaimOne(t, s, "q", time.Minute)
	if got, _ := s.Get(ctx, "t"); got.Status != envelope.StatusInFlight {
		t.Fatalf("IN_FLIGHT Get status = %s", got.Status)
	}
	if err := s.Complete(ctx, "t", c.Token, failAttempt(1, clk.now, "E")); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got, _ := s.Get(ctx, "t"); got.Status != envelope.StatusSucceeded {
		t.Fatalf("SUCCEEDED Get status = %s", got.Status)
	}
}

func TestGetReturnsIndependentCopy(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	_ = s.Enqueue(ctx, pendingTask("t", "q", clk.now))

	got, _ := s.Get(ctx, "t")
	got.Payload[0] = 'X' // mutate the returned copy
	got.HandlerRef = "tampered"

	fresh, _ := s.Get(ctx, "t")
	if string(fresh.Payload) == string(got.Payload) {
		t.Fatalf("payload aliased: caller mutation leaked into store")
	}
	if fresh.HandlerRef == "tampered" {
		t.Fatalf("handler_ref aliased: caller mutation leaked into store")
	}
}

// deadLetter drives a task all the way to the DLQ and returns nothing.
func deadLetter(t *testing.T, s *Store, clk *testClock, id, queue, errType string) {
	t.Helper()
	ctx := context.Background()
	if err := s.Enqueue(ctx, pendingTask(id, queue, clk.now)); err != nil {
		t.Fatalf("Enqueue %s: %v", id, err)
	}
	c := mustClaimOne(t, s, queue, time.Minute)
	if err := s.DeadLetter(ctx, id, c.Token, failAttempt(1, clk.now, errType)); err != nil {
		t.Fatalf("DeadLetter %s: %v", id, err)
	}
}

func TestDeadLetterAndDLQList(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	deadLetter(t, s, clk, "d1", "q", "E1")

	// Default: no attempt bodies (G13).
	list, cur, err := s.DLQList(ctx, "q", spi.DLQFilter{}, spi.Page{})
	if err != nil {
		t.Fatalf("DLQList: %v", err)
	}
	if len(list) != 1 || cur != "" {
		t.Fatalf("DLQList = %d items, cursor %q; want 1 item, empty cursor", len(list), cur)
	}
	if list[0].Status != envelope.StatusDead {
		t.Fatalf("listed status = %s, want DEAD", list[0].Status)
	}
	if len(list[0].Attempts) != 0 {
		t.Fatalf("attempts included by default: %d", len(list[0].Attempts))
	}

	// IncludeAttempts brings the history back.
	withAtt, _, _ := s.DLQList(ctx, "q", spi.DLQFilter{IncludeAttempts: true}, spi.Page{})
	if len(withAtt[0].Attempts) != 1 {
		t.Fatalf("IncludeAttempts histories = %d, want 1", len(withAtt[0].Attempts))
	}

	// Get always returns full history regardless.
	if got, _ := s.Get(ctx, "d1"); len(got.Attempts) != 1 {
		t.Fatalf("Get attempts = %d, want 1", len(got.Attempts))
	}
}

func TestDLQFilter(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	deadLetter(t, s, clk, "a", "q", "TypeA")
	deadLetter(t, s, clk, "b", "q", "TypeB")

	list, _, err := s.DLQList(ctx, "q", spi.DLQFilter{ErrorType: "TypeB"}, spi.Page{})
	if err != nil {
		t.Fatalf("DLQList: %v", err)
	}
	if len(list) != 1 || list[0].ID != "b" {
		t.Fatalf("ErrorType filter = %v, want only b", ids(list))
	}

	byHandler, _, _ := s.DLQList(ctx, "q", spi.DLQFilter{HandlerRef: "h.process"}, spi.Page{})
	if len(byHandler) != 2 {
		t.Fatalf("HandlerRef filter matched %d, want 2", len(byHandler))
	}
	none, _, _ := s.DLQList(ctx, "q", spi.DLQFilter{HandlerRef: "nope"}, spi.Page{})
	if len(none) != 0 {
		t.Fatalf("non-matching HandlerRef returned %d", len(none))
	}
}

func TestDLQPaginationStable(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	for _, id := range []string{"d1", "d2", "d3", "d4", "d5"} {
		deadLetter(t, s, clk, id, "q", "E")
	}

	// Page size 2 through the whole DLQ, collecting ids.
	var seen []string
	var cur spi.Cursor
	pages := 0
	for {
		list, next, err := s.DLQList(ctx, "q", spi.DLQFilter{}, spi.Page{Limit: 2, After: cur})
		if err != nil {
			t.Fatalf("DLQList page: %v", err)
		}
		seen = append(seen, ids(list)...)
		pages++
		if next == "" {
			break
		}
		// A new arrival mid-pagination must not skip or duplicate earlier pages.
		if pages == 1 {
			deadLetter(t, s, clk, "late", "q", "E")
		}
		cur = next
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}

	if dup := firstDup(seen); dup != "" {
		t.Fatalf("duplicate across pages: %q (seen=%v)", dup, seen)
	}
	for _, want := range []string{"d1", "d2", "d3", "d4", "d5"} {
		if !contains(seen, want) {
			t.Fatalf("original entry %q skipped (seen=%v)", want, seen)
		}
	}
}

func TestDLQListStaleCursor(t *testing.T) {
	s, _ := newStore()
	if _, _, err := s.DLQList(context.Background(), "q", spi.DLQFilter{}, spi.Page{After: "not-a-cursor!!"}); !errors.Is(err, spi.ErrStaleCursor) {
		t.Fatalf("garbage cursor = %v, want ErrStaleCursor", err)
	}
}

func TestRedriveResetsHistoryKept(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()

	// Two failed attempts then dead-letter, so attempt_count is non-zero.
	_ = s.Enqueue(ctx, pendingTask("t", "q", clk.now))
	c := mustClaimOne(t, s, "q", time.Minute)
	_ = s.Reschedule(ctx, "t", c.Token, failAttempt(1, clk.now, "E"), clk.now)
	c = mustClaimOne(t, s, "q", time.Minute)
	_ = s.DeadLetter(ctx, "t", c.Token, failAttempt(2, clk.now, "E"))

	before, _ := s.Get(ctx, "t")
	if before.AttemptCount != 2 || len(before.Attempts) != 2 {
		t.Fatalf("pre-redrive attempt_count=%d attempts=%d, want 2/2", before.AttemptCount, len(before.Attempts))
	}

	n, err := s.Redrive(ctx, "q", spi.Selector{IDs: []string{"t"}})
	if err != nil {
		t.Fatalf("Redrive: %v", err)
	}
	if n != 1 {
		t.Fatalf("Redrive count = %d, want 1", n)
	}

	after, _ := s.Get(ctx, "t")
	if after.Status != envelope.StatusPending {
		t.Fatalf("post-redrive status = %s, want PENDING", after.Status)
	}
	if after.AttemptCount != 0 {
		t.Fatalf("post-redrive attempt_count = %d, want 0", after.AttemptCount)
	}
	if after.RedriveCount != 1 {
		t.Fatalf("post-redrive redrive_count = %d, want 1", after.RedriveCount)
	}
	if len(after.Attempts) != 2 {
		t.Fatalf("post-redrive history = %d, want 2 (kept)", len(after.Attempts))
	}
	if after.NextAttemptAt == nil {
		t.Fatalf("post-redrive next_attempt_at is nil, want due")
	}

	// A redriven task is claimable again.
	if got := mustClaimOne(t, s, "q", time.Minute); got.Task.ID != "t" {
		t.Fatalf("redriven task not reclaimable")
	}
	// It has left the DLQ.
	if list, _, _ := s.DLQList(ctx, "q", spi.DLQFilter{}, spi.Page{}); len(list) != 0 {
		t.Fatalf("redriven task still in DLQ: %d", len(list))
	}
}

func TestRedriveByFilter(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	deadLetter(t, s, clk, "a", "q", "TypeA")
	deadLetter(t, s, clk, "b", "q", "TypeB")

	n, err := s.Redrive(ctx, "q", spi.Selector{Filter: &spi.DLQFilter{ErrorType: "TypeA"}})
	if err != nil {
		t.Fatalf("Redrive: %v", err)
	}
	if n != 1 {
		t.Fatalf("filter redrive count = %d, want 1", n)
	}
	if got, _ := s.Get(ctx, "a"); got.Status != envelope.StatusPending {
		t.Fatalf("a not redriven: %s", got.Status)
	}
	if got, _ := s.Get(ctx, "b"); got.Status != envelope.StatusDead {
		t.Fatalf("b wrongly redriven: %s", got.Status)
	}
}

func TestPurgeRemovesFromDLQ(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	deadLetter(t, s, clk, "a", "q", "E")
	deadLetter(t, s, clk, "b", "q", "E")

	n, err := s.Purge(ctx, "q", spi.Selector{IDs: []string{"a"}})
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n != 1 {
		t.Fatalf("Purge count = %d, want 1", n)
	}
	if _, err := s.Get(ctx, "a"); !errors.Is(err, spi.ErrNotFound) {
		t.Fatalf("purged task still present: %v", err)
	}
	if _, err := s.Get(ctx, "b"); err != nil {
		t.Fatalf("non-selected task removed: %v", err)
	}
}

func TestEmptySelectorSelectsNothing(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	deadLetter(t, s, clk, "a", "q", "E")

	if n, _ := s.Redrive(ctx, "q", spi.Selector{}); n != 0 {
		t.Fatalf("empty selector redrove %d, want 0", n)
	}
	if n, _ := s.Purge(ctx, "q", spi.Selector{}); n != 0 {
		t.Fatalf("empty selector purged %d, want 0", n)
	}
}

func TestStats(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()

	// Dead-letter "d" first, while the queue has no other pending tasks (the
	// deadLetter helper claims whatever is due).
	deadLetter(t, s, clk, "d", "q", "E")

	// "p" was created 5m ago but is scheduled for the future, so it stays
	// PENDING (the oldest) without being claimed.
	pend := pendingTask("p", "q", clk.now.Add(time.Hour))
	pend.CreatedAt = clk.now.Add(-5 * time.Minute)
	_ = s.Enqueue(ctx, pend)
	_ = s.Enqueue(ctx, pendingTask("f", "q", clk.now))

	// Claim the only due task ("f") so it is IN_FLIGHT; "p" stays pending.
	claimed, err := s.ClaimDue(ctx, "q", 10, time.Hour)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 1 || claimed[0].Task.ID != "f" {
		t.Fatalf("claimed %v, want only f", claimed)
	}

	st, err := s.Stats(ctx, "q")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Pending != 1 {
		t.Fatalf("Pending = %d, want 1", st.Pending)
	}
	if st.InFlight != 1 {
		t.Fatalf("InFlight = %d, want 1", st.InFlight)
	}
	if st.DLQDepth != 1 {
		t.Fatalf("DLQDepth = %d, want 1", st.DLQDepth)
	}
	if st.OldestPendingAge < 5*time.Minute {
		t.Fatalf("OldestPendingAge = %v, want >= 5m", st.OldestPendingAge)
	}
}

func TestPurgeSucceeded(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	_ = s.Enqueue(ctx, pendingTask("t", "q", clk.now))
	c := mustClaimOne(t, s, "q", time.Minute)
	if err := s.Complete(ctx, "t", c.Token, failAttempt(1, clk.now, "E")); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	completedAt := clk.now

	// Not old enough yet.
	if n, _ := s.PurgeSucceeded(ctx, "q", completedAt); n != 0 {
		t.Fatalf("PurgeSucceeded removed %d before threshold, want 0", n)
	}
	// Advance the cutoff past completion.
	if n, _ := s.PurgeSucceeded(ctx, "q", completedAt.Add(time.Second)); n != 1 {
		t.Fatalf("PurgeSucceeded = %d, want 1", n)
	}
	if _, err := s.Get(ctx, "t"); !errors.Is(err, spi.ErrNotFound) {
		t.Fatalf("succeeded task not purged: %v", err)
	}
}

func TestQueueIsolation(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	_ = s.Enqueue(ctx, pendingTask("a", "q1", clk.now))
	_ = s.Enqueue(ctx, pendingTask("b", "q2", clk.now))

	claimed, err := s.ClaimDue(ctx, "q1", 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 1 || claimed[0].Task.ID != "a" {
		t.Fatalf("queue isolation broken: %v", claimed)
	}
}

// --- small test helpers ---

func ids(list []envelope.Envelope) []string {
	out := make([]string, len(list))
	for i, e := range list {
		out[i] = e.ID
	}
	return out
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func firstDup(xs []string) string {
	seen := make(map[string]bool, len(xs))
	for _, x := range xs {
		if seen[x] {
			return x
		}
		seen[x] = true
	}
	return ""
}
