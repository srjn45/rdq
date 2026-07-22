// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// baseTime is a fixed instant the DLQ/stats tests build timestamps around, so
// ordering and time-range assertions are deterministic.
var baseTime = time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

// newTestStore brings up a throwaway migrated Postgres (skipping without Docker,
// per startPostgres in migrations_test.go) and returns a Store over it.
func newTestStore(ctx context.Context, t *testing.T) *Store {
	t.Helper()
	db := startPostgres(ctx, t)
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return New(db)
}

// deadEnvelope builds a DEAD task with two attempts whose terminal error type is
// errType (denormalized into rdq_dlq_task.error_type).
func deadEnvelope(id, queue, handlerRef, errType string) envelope.Envelope {
	fin := baseTime.Add(time.Second)
	return envelope.Envelope{
		EnvelopeVersion:    1,
		ID:                 id,
		Queue:              queue,
		HandlerRef:         handlerRef,
		Payload:            []byte(`{}`),
		PayloadContentType: "application/json",
		Status:             envelope.StatusDead,
		AttemptCount:       2,
		Attempts: []envelope.Attempt{
			{AttemptNo: 1, StartedAt: baseTime, FinishedAt: &fin, Outcome: envelope.OutcomeRetryableFailure,
				Error: &envelope.Error{Type: "transient", Message: "boom"}},
			{AttemptNo: 2, StartedAt: baseTime, FinishedAt: &fin, Outcome: envelope.OutcomePermanentFailure,
				Error: &envelope.Error{Type: errType, Message: "boom"}},
		},
		CreatedAt: baseTime,
	}
}

// seedDead inserts env directly into rdq_dlq_task (plus its attempts), stamping
// the given dead-letter time. It bypasses the claim/DeadLetter pipeline (T2.3) so
// the DLQ read/lifecycle paths are testable in isolation.
func seedDead(ctx context.Context, t *testing.T, s *Store, env envelope.Envelope, deadAt time.Time) {
	t.Helper()
	row, err := taskRowFromEnvelope(&env)
	if err != nil {
		t.Fatalf("taskRowFromEnvelope: %v", err)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO rdq_dlq_task
		(id, queue, envelope_version, handler_ref, handler_version, payload,
		 payload_content_type, payload_ref, headers, status, attempt_count,
		 redrive_count, next_attempt_at, lease_expires_at, created_at, residual,
		 dead_lettered_at, error_type)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,$16::jsonb,$17,$18)`,
		row.ID, row.Queue, row.EnvelopeVersion, row.HandlerRef, row.HandlerVersion, row.Payload,
		row.PayloadContentType, row.PayloadRef, string(row.Headers), row.Status, row.AttemptCount,
		row.RedriveCount, row.NextAttemptAt, row.LeaseExpiresAt, row.CreatedAt, string(row.Residual),
		deadAt, terminalErrorType(&env))
	if err != nil {
		t.Fatalf("insert dlq task %s: %v", env.ID, err)
	}
	seedAttempts(ctx, t, s, &env)
}

// seedLive inserts env directly into rdq_task (plus its attempts).
func seedLive(ctx context.Context, t *testing.T, s *Store, env envelope.Envelope) {
	t.Helper()
	row, err := taskRowFromEnvelope(&env)
	if err != nil {
		t.Fatalf("taskRowFromEnvelope: %v", err)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO rdq_task
		(id, queue, envelope_version, handler_ref, handler_version, payload,
		 payload_content_type, payload_ref, headers, status, attempt_count,
		 redrive_count, next_attempt_at, lease_expires_at, created_at, residual)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,$16::jsonb)`,
		row.ID, row.Queue, row.EnvelopeVersion, row.HandlerRef, row.HandlerVersion, row.Payload,
		row.PayloadContentType, row.PayloadRef, string(row.Headers), row.Status, row.AttemptCount,
		row.RedriveCount, row.NextAttemptAt, row.LeaseExpiresAt, row.CreatedAt, string(row.Residual))
	if err != nil {
		t.Fatalf("insert task %s: %v", env.ID, err)
	}
	seedAttempts(ctx, t, s, &env)
}

func seedAttempts(ctx context.Context, t *testing.T, s *Store, env *envelope.Envelope) {
	t.Helper()
	rows, err := attemptRowsFromEnvelope(env)
	if err != nil {
		t.Fatalf("attemptRowsFromEnvelope: %v", err)
	}
	for _, a := range rows {
		var detail any
		if len(a.ErrorDetail) > 0 {
			detail = string(a.ErrorDetail)
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO rdq_attempt
			(task_id, attempt_no, started_at, finished_at, outcome, error_type,
			 error_message, error_detail, error_stack, residual)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10::jsonb)`,
			a.TaskID, a.AttemptNo, a.StartedAt, a.FinishedAt, a.Outcome, a.ErrorType,
			a.ErrorMessage, detail, a.ErrorStack, string(a.Residual)); err != nil {
			t.Fatalf("insert attempt %s/%d: %v", a.TaskID, a.AttemptNo, err)
		}
	}
}

// TestDLQListAndGet covers the basic read paths: list omits attempt bodies by
// default, list order is by dead_lettered_at, and Get returns full history from
// either table plus ErrNotFound for the absent id.
func TestDLQListAndGet(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	seedDead(ctx, t, s, deadEnvelope("d1", "q.list", "h1", "boom"), baseTime.Add(1*time.Minute))
	seedDead(ctx, t, s, deadEnvelope("d2", "q.list", "h1", "boom"), baseTime.Add(2*time.Minute))
	seedDead(ctx, t, s, deadEnvelope("other", "q.other", "h1", "boom"), baseTime.Add(3*time.Minute))

	list, cur, err := s.DLQList(ctx, "q.list", spi.DLQFilter{}, spi.Page{})
	if err != nil {
		t.Fatalf("DLQList: %v", err)
	}
	if cur != "" {
		t.Errorf("cursor on last page = %q, want empty", cur)
	}
	if len(list) != 2 {
		t.Fatalf("DLQList len = %d, want 2 (other queue excluded)", len(list))
	}
	if list[0].ID != "d1" || list[1].ID != "d2" {
		t.Errorf("DLQList order = [%s %s], want [d1 d2]", list[0].ID, list[1].ID)
	}
	// Attempt bodies are omitted by default (G13) but the count is preserved.
	if list[0].Attempts != nil {
		t.Errorf("DLQList returned attempt bodies without IncludeAttempts")
	}
	if list[0].AttemptCount != 2 {
		t.Errorf("attempt_count = %d, want 2", list[0].AttemptCount)
	}

	withAtt, _, err := s.DLQList(ctx, "q.list", spi.DLQFilter{IncludeAttempts: true}, spi.Page{})
	if err != nil {
		t.Fatalf("DLQList IncludeAttempts: %v", err)
	}
	if len(withAtt[0].Attempts) != 2 {
		t.Errorf("IncludeAttempts history = %d, want 2", len(withAtt[0].Attempts))
	}

	got, err := s.Get(ctx, "d1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != envelope.StatusDead || len(got.Attempts) != 2 {
		t.Errorf("Get(d1) = status %s / %d attempts, want DEAD / 2", got.Status, len(got.Attempts))
	}

	// Get spans rdq_task too.
	live := deadEnvelope("liveid", "q.list", "h1", "boom")
	live.Status = envelope.StatusPending
	live.Attempts = nil
	live.AttemptCount = 0
	seedLive(ctx, t, s, live)
	if g, err := s.Get(ctx, "liveid"); err != nil || g.Status != envelope.StatusPending {
		t.Errorf("Get(liveid) = %+v, err %v; want PENDING", g.Status, err)
	}

	if _, err := s.Get(ctx, "nope"); !errors.Is(err, spi.ErrNotFound) {
		t.Errorf("Get(absent) err = %v, want ErrNotFound", err)
	}
}

// TestDLQListFilterPushdown checks the SQL filter pushdown: handler_ref,
// error_type, and the inclusive-lower / exclusive-upper dead-letter time range.
func TestDLQListFilterPushdown(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)
	const q = "q.filter"

	seedDead(ctx, t, s, deadEnvelope("a", q, "handlerA", "TypeX"), baseTime.Add(1*time.Minute))
	seedDead(ctx, t, s, deadEnvelope("b", q, "handlerB", "TypeY"), baseTime.Add(2*time.Minute))
	seedDead(ctx, t, s, deadEnvelope("c", q, "handlerA", "TypeY"), baseTime.Add(3*time.Minute))

	ids := func(f spi.DLQFilter) []string {
		t.Helper()
		list, _, err := s.DLQList(ctx, q, f, spi.Page{})
		if err != nil {
			t.Fatalf("DLQList: %v", err)
		}
		out := make([]string, len(list))
		for i, e := range list {
			out[i] = e.ID
		}
		return out
	}

	if got := ids(spi.DLQFilter{HandlerRef: "handlerA"}); !equalStrings(got, []string{"a", "c"}) {
		t.Errorf("HandlerRef filter = %v, want [a c]", got)
	}
	if got := ids(spi.DLQFilter{ErrorType: "TypeY"}); !equalStrings(got, []string{"b", "c"}) {
		t.Errorf("ErrorType filter = %v, want [b c]", got)
	}
	if got := ids(spi.DLQFilter{HandlerRef: "handlerA", ErrorType: "TypeY"}); !equalStrings(got, []string{"c"}) {
		t.Errorf("combined filter = %v, want [c]", got)
	}

	// Time range: [2min, 3min) selects only b (inclusive lower, exclusive upper).
	after := baseTime.Add(2 * time.Minute)
	before := baseTime.Add(3 * time.Minute)
	if got := ids(spi.DLQFilter{DeadLetteredAfter: &after, DeadLetteredBefore: &before}); !equalStrings(got, []string{"b"}) {
		t.Errorf("time-range filter = %v, want [b]", got)
	}
}

// TestDLQListPaginationStable is the T2.4 acceptance: cursor paging neither skips
// nor duplicates established entries even when a new task is dead-lettered mid-
// pagination (design 02 §3 invariant 8; mirrors compliance.testStablePagination).
func TestDLQListPaginationStable(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)
	const q = "q.page"

	original := []string{"d1", "d2", "d3", "d4", "d5"}
	for i, id := range original {
		seedDead(ctx, t, s, deadEnvelope(id, q, "h", "boom"), baseTime.Add(time.Duration(i+1)*time.Minute))
	}

	seen := make([]string, 0, len(original)+1)
	var cur spi.Cursor
	pages := 0
	for {
		list, next, err := s.DLQList(ctx, q, spi.DLQFilter{}, spi.Page{Limit: 2, After: cur})
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
		if pages == 1 {
			// A fresh arrival after the first page must not disturb the entries
			// established before the cursor. Its dead_lettered_at sorts last.
			seedDead(ctx, t, s, deadEnvelope("late", q, "h", "boom"), baseTime.Add(99*time.Minute))
		}
		cur = next
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}

	if dup := firstDup(seen); dup != "" {
		t.Fatalf("id %q appeared on more than one page (seen=%v)", dup, seen)
	}
	for _, want := range original {
		if !contains(seen, want) {
			t.Fatalf("original entry %q skipped across pages (seen=%v)", want, seen)
		}
	}
}

// TestRedrive verifies invariant 7: redriven tasks return to PENDING with
// attempt_count reset, redrive_count incremented, history preserved, and leave
// the DLQ. Covers both id-based and filter-based selectors.
func TestRedrive(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)
	const q = "q.redrive"

	seedDead(ctx, t, s, deadEnvelope("r1", q, "h", "boom"), baseTime.Add(1*time.Minute))
	seedDead(ctx, t, s, deadEnvelope("r2", q, "h", "keep"), baseTime.Add(2*time.Minute))

	n, err := s.Redrive(ctx, q, spi.Selector{IDs: []spi.TaskID{"r1"}})
	if err != nil {
		t.Fatalf("Redrive: %v", err)
	}
	if n != 1 {
		t.Fatalf("Redrive count = %d, want 1", n)
	}

	after, err := s.Get(ctx, "r1")
	if err != nil {
		t.Fatalf("Get after redrive: %v", err)
	}
	if after.Status != envelope.StatusPending {
		t.Errorf("status = %s, want PENDING", after.Status)
	}
	if after.AttemptCount != 0 {
		t.Errorf("attempt_count = %d, want 0", after.AttemptCount)
	}
	if after.RedriveCount != 1 {
		t.Errorf("redrive_count = %d, want 1", after.RedriveCount)
	}
	if len(after.Attempts) != 2 {
		t.Errorf("history = %d attempts, want 2 (preserved)", len(after.Attempts))
	}
	if after.NextAttemptAt == nil {
		t.Errorf("next_attempt_at is nil, want a due task")
	}

	// It left the DLQ; r2 remains.
	list, _, err := s.DLQList(ctx, q, spi.DLQFilter{}, spi.Page{})
	if err != nil {
		t.Fatalf("DLQList: %v", err)
	}
	if len(list) != 1 || list[0].ID != "r2" {
		t.Fatalf("DLQ after redrive = %v, want [r2]", listIDs(list))
	}

	// Filter-based redrive.
	n, err = s.Redrive(ctx, q, spi.Selector{Filter: &spi.DLQFilter{ErrorType: "keep"}})
	if err != nil || n != 1 {
		t.Fatalf("filter Redrive = (%d, %v), want (1, nil)", n, err)
	}
	if list, _, _ := s.DLQList(ctx, q, spi.DLQFilter{}, spi.Page{}); len(list) != 0 {
		t.Fatalf("DLQ not empty after filter redrive: %v", listIDs(list))
	}

	// Empty selector is a no-op.
	if n, err := s.Redrive(ctx, q, spi.Selector{}); err != nil || n != 0 {
		t.Fatalf("empty Redrive = (%d, %v), want (0, nil)", n, err)
	}
}

// TestPurge verifies permanent removal of DLQ tasks and their attempt rows via
// both selectors, and that an empty selector removes nothing.
func TestPurge(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)
	const q = "q.purge"

	seedDead(ctx, t, s, deadEnvelope("p1", q, "h", "boom"), baseTime.Add(1*time.Minute))
	seedDead(ctx, t, s, deadEnvelope("p2", q, "h", "boom"), baseTime.Add(2*time.Minute))
	seedDead(ctx, t, s, deadEnvelope("p3", q, "h", "gone"), baseTime.Add(3*time.Minute))

	if n, err := s.Purge(ctx, q, spi.Selector{}); err != nil || n != 0 {
		t.Fatalf("empty Purge = (%d, %v), want (0, nil)", n, err)
	}

	n, err := s.Purge(ctx, q, spi.Selector{IDs: []spi.TaskID{"p1", "p2"}})
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n != 2 {
		t.Fatalf("Purge count = %d, want 2", n)
	}
	if _, err := s.Get(ctx, "p1"); !errors.Is(err, spi.ErrNotFound) {
		t.Errorf("p1 still present after purge: %v", err)
	}
	// Attempt rows are gone too (no orphans).
	if c := countAttempts(ctx, t, s, "p1"); c != 0 {
		t.Errorf("p1 attempt rows after purge = %d, want 0", c)
	}

	if n, err := s.Purge(ctx, q, spi.Selector{Filter: &spi.DLQFilter{ErrorType: "gone"}}); err != nil || n != 1 {
		t.Fatalf("filter Purge = (%d, %v), want (1, nil)", n, err)
	}
	if list, _, _ := s.DLQList(ctx, q, spi.DLQFilter{}, spi.Page{}); len(list) != 0 {
		t.Fatalf("DLQ not empty after purge: %v", listIDs(list))
	}
}

// TestDLQCursorCodec exercises the cursor round-trip and rejection paths without
// a database.
func TestDLQCursorCodec(t *testing.T) {
	at := time.Date(2026, 7, 21, 12, 30, 45, 123456000, time.UTC) // microsecond precision
	c := encodeDLQCursor(at, "01J2ZID")

	gotAt, gotID, hasAfter, err := decodeDLQCursor(c)
	if err != nil || !hasAfter {
		t.Fatalf("decode round-trip: hasAfter=%v err=%v", hasAfter, err)
	}
	if !gotAt.Equal(at) || gotID != "01J2ZID" {
		t.Errorf("round-trip = (%v, %q), want (%v, 01J2ZID)", gotAt, gotID, at)
	}

	if _, _, hasAfter, err := decodeDLQCursor(""); hasAfter || err != nil {
		t.Errorf("empty cursor = hasAfter %v err %v, want (false, nil)", hasAfter, err)
	}

	for _, bad := range []spi.Cursor{"!!!not-base64!!!", "Zm9v" /* "foo", wrong prefix */} {
		if _, _, _, err := decodeDLQCursor(bad); !errors.Is(err, spi.ErrStaleCursor) {
			t.Errorf("decodeDLQCursor(%q) err = %v, want ErrStaleCursor", bad, err)
		}
	}
}

// TestDLQSelectorClause checks the selector→SQL builder without a database: an
// empty selector selects nothing; ids and filter each produce a scoped clause.
func TestDLQSelectorClause(t *testing.T) {
	if _, _, nothing := dlqSelectorClause("q", spi.Selector{}, 1); !nothing {
		t.Error("empty selector should select nothing")
	}
	if _, _, nothing := dlqSelectorClause("q", spi.Selector{Filter: &spi.DLQFilter{}}, 1); nothing {
		t.Error("empty-filter selector should still select (all in queue), not nothing")
	}
	where, args, nothing := dlqSelectorClause("q", spi.Selector{IDs: []spi.TaskID{"a"}}, 1)
	if nothing || len(args) != 2 {
		t.Fatalf("ids selector: nothing=%v args=%d", nothing, len(args))
	}
	if want := "queue = $1 AND id = ANY($2)"; where != want {
		t.Errorf("ids clause = %q, want %q", where, want)
	}
}

// --- small test helpers ---

func countAttempts(ctx context.Context, t *testing.T, s *Store, taskID string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRowContext(ctx,
		"SELECT count(*) FROM rdq_attempt WHERE task_id = $1", taskID).Scan(&n); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0
		}
		t.Fatalf("countAttempts: %v", err)
	}
	return n
}

func listIDs(list []envelope.Envelope) []string {
	out := make([]string, len(list))
	for i, e := range list {
		out[i] = e.ID
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
