// SPDX-License-Identifier: Apache-2.0

package postgres

// DLQ / redrive / purge operations (design 02 §2 "--- DLQ ---", T2.4). This
// file owns the read and lifecycle paths over the dead-letter table
// (rdq_dlq_task) plus Get, which spans both task tables. It reuses the T2.2
// envelope↔rows mapping (mapping.go) so a row read here reassembles into the
// exact same envelope the writer decomposed.
//
// Pagination is cursor-based keyset paging over (dead_lettered_at, id), the
// stable-ordering contract of design 02 §3 invariant 8: a fresh dead-letter
// arrival always sorts at or after now() and therefore never perturbs a page
// that predates the cursor — no offset skew, no skips, no duplicates under
// concurrent inserts. Filters (error_type, handler_ref, dead-letter time range)
// are pushed down to SQL against the denormalized rdq_dlq_task columns and their
// indexes (FilterPushdown capability, advertised by T2.5).

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// The Store type and its New constructor are declared in claim.go (T2.3); this
// file only adds the DLQ/ops methods to it.

// defaultDLQPageLimit is the DLQList page size when Page.Limit is zero or
// negative (mirrors memstore.defaultPageLimit).
const defaultDLQPageLimit = 100

// dlqCursorPrefix tags an encoded DLQList cursor so a foreign or corrupt token
// is rejected with ErrStaleCursor rather than silently mis-paging.
const dlqCursorPrefix = "pgdlq1:"

// The shared row helpers this file relies on — the taskColumns projection,
// rowScanner, scanTaskRow, the querier interface, and loadAttempts — are declared
// in claim.go (T2.3) and reused here unchanged.

// DLQList pages the dead-letter queue for queue with stable cursor-based
// pagination (design 02 §3 invariant 8). Envelopes omit attempt bodies unless
// f.IncludeAttempts is set (G13). The returned Cursor is empty on the last page;
// an unparseable cursor yields ErrStaleCursor.
func (s *Store) DLQList(ctx context.Context, queue string, f spi.DLQFilter, page spi.Page) ([]envelope.Envelope, spi.Cursor, error) {
	afterAt, afterID, hasAfter, err := decodeDLQCursor(page.After)
	if err != nil {
		return nil, "", err
	}

	limit := page.Limit
	if limit <= 0 {
		limit = defaultDLQPageLimit
	}

	args := []any{queue}
	conds := []string{"queue = $1"}
	addCond := func(sqlFmt string, val any) {
		args = append(args, val)
		conds = append(conds, fmt.Sprintf(sqlFmt, len(args)))
	}
	if f.HandlerRef != "" {
		addCond("handler_ref = $%d", f.HandlerRef)
	}
	if f.ErrorType != "" {
		addCond("error_type = $%d", f.ErrorType)
	}
	if f.DeadLetteredAfter != nil {
		addCond("dead_lettered_at >= $%d", *f.DeadLetteredAfter) // inclusive lower bound
	}
	if f.DeadLetteredBefore != nil {
		addCond("dead_lettered_at < $%d", *f.DeadLetteredBefore) // exclusive upper bound
	}
	if hasAfter {
		args = append(args, afterAt, afterID)
		conds = append(conds, fmt.Sprintf("(dead_lettered_at, id) > ($%d, $%d)", len(args)-1, len(args)))
	}
	// Fetch one extra row to detect whether a further page exists.
	args = append(args, limit+1)
	query := "SELECT " + taskColumns + ", dead_lettered_at FROM rdq_dlq_task WHERE " +
		strings.Join(conds, " AND ") +
		" ORDER BY dead_lettered_at, id LIMIT $" + strconv.Itoa(len(args))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("rdq/postgres: DLQList query: %w", err)
	}
	defer rows.Close()

	type paged struct {
		row    taskRow
		deadAt time.Time
	}
	collected := make([]paged, 0, limit+1)
	for rows.Next() {
		var p paged
		p.row, err = scanTaskRow(rows, &p.deadAt)
		if err != nil {
			return nil, "", fmt.Errorf("rdq/postgres: DLQList scan: %w", err)
		}
		collected = append(collected, p)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("rdq/postgres: DLQList rows: %w", err)
	}

	var next spi.Cursor
	if len(collected) > limit {
		collected = collected[:limit]
		last := collected[len(collected)-1]
		next = encodeDLQCursor(last.deadAt, last.row.ID)
	}

	out := make([]envelope.Envelope, 0, len(collected))
	for _, p := range collected {
		var attempts []attemptRow
		if f.IncludeAttempts {
			attempts, err = loadAttempts(ctx, s.db, p.row.ID)
			if err != nil {
				return nil, "", err
			}
		}
		env, err := envelopeFromRows(p.row, attempts)
		if err != nil {
			return nil, "", err
		}
		out = append(out, *env)
	}
	return out, next, nil
}

// Get fetches one task by id in ANY status (PENDING/IN_FLIGHT/SUCCEEDED/DEAD)
// with full attempt history; ErrNotFound if absent. A task lives in exactly one
// of rdq_task / rdq_dlq_task, so the UNION resolves to at most one row.
func (s *Store) Get(ctx context.Context, id spi.TaskID) (envelope.Envelope, error) {
	const q = "SELECT " + taskColumns + " FROM rdq_task WHERE id = $1 " +
		"UNION ALL " +
		"SELECT " + taskColumns + " FROM rdq_dlq_task WHERE id = $1"

	row, err := scanTaskRow(s.db.QueryRowContext(ctx, q, id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return envelope.Envelope{}, spi.ErrNotFound
		}
		return envelope.Envelope{}, fmt.Errorf("rdq/postgres: Get query: %w", err)
	}
	attempts, err := loadAttempts(ctx, s.db, id)
	if err != nil {
		return envelope.Envelope{}, err
	}
	env, err := envelopeFromRows(row, attempts)
	if err != nil {
		return envelope.Envelope{}, err
	}
	return *env, nil
}

// Redrive returns the selected DLQ tasks to PENDING with attempt_count reset to
// 0 and redrive_count incremented, preserving prior attempt history (design 02
// §3 invariant 7). Storage owns the clock (G9): next_attempt_at is set to the
// backend's now(), so redriven tasks are immediately due. The move out of
// rdq_dlq_task and into rdq_task is a single atomic statement (a data-modifying
// CTE); the attempt rows keep the same task_id and are untouched.
func (s *Store) Redrive(ctx context.Context, queue string, sel spi.Selector) (int, error) {
	where, args, selectsNothing := dlqSelectorClause(queue, sel, 1)
	if selectsNothing {
		return 0, nil
	}
	// DELETE ... RETURNING captures exactly the selected rows; INSERT reads them
	// from the CTE so the selection is evaluated once. claim_token/lease are
	// cleared and status flips to PENDING.
	query := `
WITH moved AS (
    DELETE FROM rdq_dlq_task
    WHERE ` + where + `
    RETURNING id, queue, envelope_version, handler_ref, handler_version,
              payload, payload_content_type, payload_ref, headers,
              redrive_count, created_at, residual
)
INSERT INTO rdq_task (id, queue, envelope_version, handler_ref, handler_version,
                      payload, payload_content_type, payload_ref, headers, status,
                      attempt_count, redrive_count, next_attempt_at, lease_expires_at,
                      claim_token, created_at, residual)
SELECT id, queue, envelope_version, handler_ref, handler_version,
       payload, payload_content_type, payload_ref, headers, 'PENDING',
       0, redrive_count + 1, now(), NULL, NULL, created_at, residual
FROM moved
RETURNING id`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("rdq/postgres: Redrive: %w", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rdq/postgres: Redrive rows: %w", err)
	}
	return n, nil
}

// Purge permanently removes the selected DLQ tasks and their attempt history.
// Returns the count removed. rdq_attempt has no FK cascade (a task moves between
// tables), so the attempt rows are deleted explicitly alongside the task rows in
// one atomic statement.
func (s *Store) Purge(ctx context.Context, queue string, sel spi.Selector) (int, error) {
	where, args, selectsNothing := dlqSelectorClause(queue, sel, 1)
	if selectsNothing {
		return 0, nil
	}
	query := `
WITH purged AS (
    DELETE FROM rdq_dlq_task
    WHERE ` + where + `
    RETURNING id
),
_att AS (
    DELETE FROM rdq_attempt WHERE task_id IN (SELECT id FROM purged)
)
SELECT count(*) FROM purged`

	var n int
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("rdq/postgres: Purge: %w", err)
	}
	return n, nil
}

// dlqSelectorClause builds the WHERE clause selecting DEAD tasks in queue per
// sel, scoped to rdq_dlq_task. IDs and Filter are mutually exclusive; an empty
// Selector selects nothing (design 02 §2), reported via selectsNothing so the
// caller can short-circuit without issuing a query. startArg is the first
// positional-parameter number to use ($startArg ...); the returned args are
// ordered to match.
func dlqSelectorClause(queue string, sel spi.Selector, startArg int) (where string, args []any, selectsNothing bool) {
	args = []any{queue}
	next := startArg + 1 // $startArg is queue
	conds := []string{fmt.Sprintf("queue = $%d", startArg)}

	if len(sel.IDs) > 0 {
		args = append(args, sel.IDs)
		conds = append(conds, fmt.Sprintf("id = ANY($%d)", next))
		return strings.Join(conds, " AND "), args, false
	}
	if sel.Filter == nil {
		return "", nil, true // neither ids nor filter: selects nothing
	}

	f := sel.Filter
	add := func(sqlFmt string, val any) {
		args = append(args, val)
		conds = append(conds, fmt.Sprintf(sqlFmt, next))
		next++
	}
	if f.HandlerRef != "" {
		add("handler_ref = $%d", f.HandlerRef)
	}
	if f.ErrorType != "" {
		add("error_type = $%d", f.ErrorType)
	}
	if f.DeadLetteredAfter != nil {
		add("dead_lettered_at >= $%d", *f.DeadLetteredAfter)
	}
	if f.DeadLetteredBefore != nil {
		add("dead_lettered_at < $%d", *f.DeadLetteredBefore)
	}
	return strings.Join(conds, " AND "), args, false
}

// --- cursor codec ---

// encodeDLQCursor packs a (dead_lettered_at, id) keyset position into an opaque,
// URL-safe cursor. The instant is stored as UnixNano; rdq_dlq_task timestamps
// are microsecond-precise, so the value round-trips exactly.
func encodeDLQCursor(deadAt time.Time, id string) spi.Cursor {
	raw := dlqCursorPrefix + strconv.FormatInt(deadAt.UnixNano(), 10) + ":" + id
	return spi.Cursor(base64.RawURLEncoding.EncodeToString([]byte(raw)))
}

// decodeDLQCursor resolves a cursor to its (dead_lettered_at, id) threshold. The
// empty cursor starts from the beginning (hasAfter=false). Anything that does
// not decode to a valid tagged token is ErrStaleCursor.
func decodeDLQCursor(c spi.Cursor) (deadAt time.Time, id string, hasAfter bool, err error) {
	if c == "" {
		return time.Time{}, "", false, nil
	}
	rawBytes, decErr := base64.RawURLEncoding.DecodeString(string(c))
	if decErr != nil {
		return time.Time{}, "", false, spi.ErrStaleCursor
	}
	raw := string(rawBytes)
	if !strings.HasPrefix(raw, dlqCursorPrefix) {
		return time.Time{}, "", false, spi.ErrStaleCursor
	}
	rest := raw[len(dlqCursorPrefix):]
	sep := strings.IndexByte(rest, ':')
	if sep < 0 {
		return time.Time{}, "", false, spi.ErrStaleCursor
	}
	nanos, parseErr := strconv.ParseInt(rest[:sep], 10, 64)
	if parseErr != nil {
		return time.Time{}, "", false, spi.ErrStaleCursor
	}
	return time.Unix(0, nanos).UTC(), rest[sep+1:], true, nil
}
