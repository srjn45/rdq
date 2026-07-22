// SPDX-License-Identifier: Apache-2.0

package postgres

// Claim path (design 02 §4, T2.3). ClaimDue is the atomic FOR UPDATE SKIP LOCKED
// statement that hands out fenced leases; the post-claim mutations that spend a
// lease live in mutations.go. Together they own the storage-managed columns the
// mapping layer (T2.2) deliberately left alone: claim_token (the fencing token),
// lease_expires_at, and the rdq_task ⇄ rdq_dlq_task move.
//
// Fencing. Every claim mints a fresh claim_token (gen_random_uuid()); every
// mutation carries it back and matches it in the WHERE clause. A zombie worker
// (expired lease, reclaimed elsewhere) presents an old token, matches zero rows,
// and is told ErrStaleClaim — it can never corrupt state (design 02 §1, §3
// invariant 2). The token is compared as text (claim_token::text = $token) so a
// malformed or foreign token is a clean non-match, never a SQL type error.

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// leaseExpiredType / leaseExpiredMessage are the error.type and message recorded
// on the LEASE_EXPIRED attempt appended when an expired lease is reclaimed (G7);
// they match the in-memory reference store so both backends produce the same
// attempt record.
const (
	leaseExpiredType    = "rdq.LeaseExpired"
	leaseExpiredMessage = "lease expired before an outcome was reported"
)

// SHARED with T2.4 — de-dup at integration, keep one copy. Both this task
// (claim.go/mutations.go) and T2.4 (dlq.go/stats.go) add methods to
// postgres.Store, so each branch declares the type verbatim to compile and test
// standalone; the integrator drops one identical copy. Neither branch adds the
// `var _ spi.Storage = (*Store)(nil)` assertion (neither implements the full
// interface alone — that lands at integration / T2.5).

// Store is the PostgreSQL spi.Storage backend. Construct with New.
type Store struct {
	db *sql.DB
}

// New returns a Store backed by db. The caller owns db's lifecycle.
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// querier is the subset of *sql.DB / *sql.Tx the row helpers need, so they run
// unchanged against a bare connection or inside a transaction.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// taskColumns is the envelope-derived column list of rdq_task / rdq_dlq_task in
// the fixed order scanTaskRow expects (it mirrors the taskRow field order in
// mapping.go). The storage-managed claim_token and the DLQ denormalizations are
// selected separately by the callers that need them.
const taskColumns = `id, queue, envelope_version, handler_ref, handler_version,
	payload, payload_content_type, payload_ref, headers, status,
	attempt_count, redrive_count, next_attempt_at, lease_expires_at,
	created_at, residual`

// rowScanner is the common Scan surface of *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanTaskRow scans the taskColumns projection into a taskRow, appending any
// extra destinations (e.g. the claim token, prior-status bookkeeping) the
// caller's query returned after the shared columns.
func scanTaskRow(sc rowScanner, extra ...any) (taskRow, error) {
	var r taskRow
	dest := []any{
		&r.ID, &r.Queue, &r.EnvelopeVersion, &r.HandlerRef, &r.HandlerVersion,
		&r.Payload, &r.PayloadContentType, &r.PayloadRef, &r.Headers, &r.Status,
		&r.AttemptCount, &r.RedriveCount, &r.NextAttemptAt, &r.LeaseExpiresAt,
		&r.CreatedAt, &r.Residual,
	}
	dest = append(dest, extra...)
	if err := sc.Scan(dest...); err != nil {
		return taskRow{}, err
	}
	return r, nil
}

// loadAttempts reads a task's ordered attempt history from rdq_attempt. Callers
// pass the same querier they are working on so the read sees the transaction's
// own writes (e.g. a just-appended LEASE_EXPIRED record).
func loadAttempts(ctx context.Context, q querier, taskID string) ([]attemptRow, error) {
	rows, err := q.QueryContext(ctx, `SELECT
		task_id, attempt_no, started_at, finished_at, outcome,
		error_type, error_message, error_detail, error_stack, residual
		FROM rdq_attempt WHERE task_id = $1 ORDER BY attempt_no`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []attemptRow
	for rows.Next() {
		var a attemptRow
		if err := rows.Scan(
			&a.TaskID, &a.AttemptNo, &a.StartedAt, &a.FinishedAt, &a.Outcome,
			&a.ErrorType, &a.ErrorMessage, &a.ErrorDetail, &a.ErrorStack, &a.Residual,
		); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// claimSQL is the design 02 §4 claim: a partial-index scan of due candidates
// locked with FOR UPDATE SKIP LOCKED, atomically flipped to IN_FLIGHT with a
// fresh lease and fencing token. The `due` CTE also captures each candidate's
// pre-claim status and lease so the caller can append a LEASE_EXPIRED attempt
// for rows it reclaimed from an expired lease (design 02 §3 invariant 3).
const claimSQL = `
WITH due AS (
    SELECT id,
           status           AS prev_status,
           lease_expires_at AS prev_lease
    FROM rdq_task
    WHERE queue = $1
      AND ( (status = 'PENDING'   AND next_attempt_at  <= now())
         OR (status = 'IN_FLIGHT' AND lease_expires_at <= now()) )
    ORDER BY next_attempt_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
),
claimed AS (
    UPDATE rdq_task t SET
        status           = 'IN_FLIGHT',
        lease_expires_at = now() + make_interval(secs => $3),
        claim_token      = gen_random_uuid()
    FROM due
    WHERE t.id = due.id
    RETURNING
        t.id, t.queue, t.envelope_version, t.handler_ref, t.handler_version,
        t.payload, t.payload_content_type, t.payload_ref, t.headers, t.status,
        t.attempt_count, t.redrive_count, t.next_attempt_at, t.lease_expires_at,
        t.created_at, t.residual,
        t.claim_token::text AS token,
        due.prev_status, due.prev_lease
)
SELECT * FROM claimed`

// insertLeaseExpiredSQL appends the LEASE_EXPIRED attempt recorded when an
// expired lease is reclaimed (G7). attempt_no is derived from the task's
// history (MAX(attempt_no)+1), not from attempt_count, so redriven tasks
// (attempt_count=0, history 1..N preserved) do not collide on the
// UNIQUE(task_id,attempt_no) constraint. The task row is locked within the
// claim transaction, making the subquery race-free. started_at is the moment
// the lost lease lapsed; finished_at is the reclaim time (both storage clock).
const insertLeaseExpiredSQL = `
INSERT INTO rdq_attempt
    (task_id, attempt_no, started_at, finished_at, outcome, error_type, error_message)
VALUES ($1, (SELECT COALESCE(MAX(attempt_no), 0) + 1 FROM rdq_attempt WHERE task_id = $1),
        COALESCE($2, now()), now(), 'LEASE_EXPIRED', $3, $4)`

// ClaimDue atomically claims up to limit due tasks for queue (design 02 §4). A
// task is due when, by the storage clock (G9), it is PENDING with
// next_attempt_at <= now or IN_FLIGHT with an expired lease. Claimed tasks
// become IN_FLIGHT with lease_expires_at = now + lease and a fresh fencing
// token; reclaiming an expired lease atomically appends a LEASE_EXPIRED attempt
// and counts it against the attempt history. FOR UPDATE SKIP LOCKED guarantees
// no task another live claim holds is ever returned.
func (s *Store) ClaimDue(ctx context.Context, queue string, limit int, lease time.Duration) ([]spi.Claimed, error) {
	if limit <= 0 {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op once Commit succeeds

	// One statement does the scan, lock, and lease flip; RETURNING carries the
	// new token plus the pre-claim status/lease needed for lease reclaim.
	rows, err := tx.QueryContext(ctx, claimSQL, queue, limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("rdq/postgres: claim query: %w", err)
	}

	// Drain the claim result before issuing follow-up statements: database/sql
	// holds the transaction's single connection while rows are open.
	type claimedRow struct {
		row        taskRow
		token      string
		prevStatus string
		prevLease  *time.Time
	}
	var claimedRows []claimedRow
	for rows.Next() {
		var cr claimedRow
		tr, err := scanTaskRow(rows, &cr.token, &cr.prevStatus, &cr.prevLease)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("rdq/postgres: scanning claimed row: %w", err)
		}
		cr.row = tr
		claimedRows = append(claimedRows, cr)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	// Reclaimed leases (prev status IN_FLIGHT): append LEASE_EXPIRED and count
	// it, atomically with the re-claim (design 02 §3 invariant 3).
	for i := range claimedRows {
		cr := &claimedRows[i]
		if cr.prevStatus != string(envelope.StatusInFlight) {
			continue
		}
		if _, err := tx.ExecContext(ctx, insertLeaseExpiredSQL,
			cr.row.ID, cr.prevLease, leaseExpiredType, leaseExpiredMessage); err != nil {
			return nil, fmt.Errorf("rdq/postgres: recording lease-expired attempt: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE rdq_task SET attempt_count = attempt_count + 1 WHERE id = $1`, cr.row.ID); err != nil {
			return nil, fmt.Errorf("rdq/postgres: counting lease-expired attempt: %w", err)
		}
		cr.row.AttemptCount++
	}

	// Assemble each claimed envelope with its full (now up-to-date) history.
	claimed := make([]spi.Claimed, 0, len(claimedRows))
	for i := range claimedRows {
		cr := &claimedRows[i]
		attempts, err := loadAttempts(ctx, tx, cr.row.ID)
		if err != nil {
			return nil, fmt.Errorf("rdq/postgres: loading attempts for %s: %w", cr.row.ID, err)
		}
		env, err := envelopeFromRows(cr.row, attempts)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, spi.Claimed{Task: *env, Token: spi.ClaimToken(cr.token)})
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}
