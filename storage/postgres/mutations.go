// SPDX-License-Identifier: Apache-2.0

package postgres

// Post-claim mutations (design 02 §2/§3, T2.3). ExtendLease renews a lease;
// Reschedule/Complete/DeadLetter spend one, each appending an attempt and moving
// the task to its next state. Every statement is fenced: the claim_token appears
// in the WHERE clause, so a stale or foreign token matches no row and the caller
// gets spi.ErrStaleClaim with nothing changed (design 02 §3 invariant 2). The
// mutations that also write attempt history run in a short transaction so the
// state change and the appended attempt commit together (atomicity, invariant 4).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// ExtendLease renews the lease for a live claim (heartbeat for long handlers).
// It is a single fenced statement: an unmatched token (lease lost, task
// reclaimed) affects zero rows and yields ErrStaleClaim, on which the handler
// must abandon its work.
func (s *Store) ExtendLease(ctx context.Context, id spi.TaskID, token spi.ClaimToken, lease time.Duration) error {
	res, err := s.db.ExecContext(ctx, `UPDATE rdq_task
		SET lease_expires_at = now() + make_interval(secs => $3)
		WHERE id = $1 AND status = 'IN_FLIGHT' AND claim_token::text = $2`,
		id, string(token), lease.Seconds())
	if err != nil {
		return fmt.Errorf("rdq/postgres: extending lease: %w", err)
	}
	return staleIfNoRows(res)
}

// Reschedule is the failure path: append the attempt and return the task to
// PENDING with next_attempt_at = nextAt (engine-computed backoff), clearing the
// lease and token. Fenced by token.
func (s *Store) Reschedule(ctx context.Context, id spi.TaskID, token spi.ClaimToken, attempt spi.Attempt, nextAt time.Time) error {
	return s.spendClaim(ctx, id, attempt, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE rdq_task SET
			status = 'PENDING', next_attempt_at = $3, lease_expires_at = NULL,
			claim_token = NULL, attempt_count = attempt_count + 1
			WHERE id = $1 AND status = 'IN_FLIGHT' AND claim_token::text = $2`,
			id, string(token), nextAt)
		if err != nil {
			return err
		}
		return staleIfNoRows(res)
	})
}

// Complete is the success path: append the attempt and mark the task SUCCEEDED
// (retained until task_ttl purge), clearing the lease and token. Fenced by token.
func (s *Store) Complete(ctx context.Context, id spi.TaskID, token spi.ClaimToken, attempt spi.Attempt) error {
	return s.spendClaim(ctx, id, attempt, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE rdq_task SET
			status = 'SUCCEEDED', next_attempt_at = NULL, lease_expires_at = NULL,
			claim_token = NULL, attempt_count = attempt_count + 1
			WHERE id = $1 AND status = 'IN_FLIGHT' AND claim_token::text = $2`,
			id, string(token))
		if err != nil {
			return err
		}
		return staleIfNoRows(res)
	})
}

// deadLetterSQL moves a fenced task from rdq_task to rdq_dlq_task in one
// statement: the data-modifying `moved` CTE deletes the live row (only when the
// token matches) and feeds it to the DLQ insert, which stamps status DEAD,
// counts the final attempt, records dead_lettered_at, and denormalizes the
// terminal error_type for DLQFilter pushdown (design 02 §4). RETURNING id is
// empty when the token did not match — the signal for ErrStaleClaim.
const deadLetterSQL = `
WITH moved AS (
    DELETE FROM rdq_task
    WHERE id = $1 AND status = 'IN_FLIGHT' AND claim_token::text = $2
    RETURNING id, queue, envelope_version, handler_ref, handler_version,
              payload, payload_content_type, payload_ref, headers,
              attempt_count, redrive_count, created_at, residual
)
INSERT INTO rdq_dlq_task (
    id, queue, envelope_version, handler_ref, handler_version,
    payload, payload_content_type, payload_ref, headers, status,
    attempt_count, redrive_count, next_attempt_at, lease_expires_at,
    claim_token, created_at, residual, dead_lettered_at, error_type)
SELECT id, queue, envelope_version, handler_ref, handler_version,
       payload, payload_content_type, payload_ref, headers, 'DEAD',
       attempt_count + 1, redrive_count, NULL, NULL,
       NULL, created_at, residual, now(), $3
FROM moved
RETURNING id`

// DeadLetter is exhaustion / permanent failure: append the attempt and move the
// task to the DLQ. Fenced by token; the move and the appended attempt commit
// together.
func (s *Store) DeadLetter(ctx context.Context, id spi.TaskID, token spi.ClaimToken, attempt spi.Attempt) error {
	row, err := attemptRowFromAttempt(id, attempt)
	if err != nil {
		return err
	}
	// error_type on the DLQ row is the terminal (this) attempt's error type,
	// indexed for DLQFilter pushdown; NULL when the attempt carries no error.
	var errType any
	if attempt.Error != nil && attempt.Error.Type != "" {
		errType = attempt.Error.Type
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op once Commit succeeds

	var movedID string
	if err := tx.QueryRowContext(ctx, deadLetterSQL, id, string(token), errType).Scan(&movedID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return spi.ErrStaleClaim // token did not match: nothing moved
		}
		return fmt.Errorf("rdq/postgres: dead-lettering: %w", err)
	}
	if err := insertAttempt(ctx, tx, row); err != nil {
		return fmt.Errorf("rdq/postgres: recording attempt: %w", err)
	}
	return tx.Commit()
}

// spendClaim runs a fenced state-change statement (update) and, only if it
// matched the claim, appends the attempt — both in one transaction so the
// outcome and its attempt record are atomic (design 02 §3 invariant 4). update
// returns spi.ErrStaleClaim (via staleIfNoRows) when the token did not match, in
// which case the transaction rolls back and no attempt is written.
func (s *Store) spendClaim(ctx context.Context, id spi.TaskID, attempt spi.Attempt, update func(*sql.Tx) error) error {
	row, err := attemptRowFromAttempt(id, attempt)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op once Commit succeeds

	if err := update(tx); err != nil {
		return err // ErrStaleClaim or a real failure; defer rolls back
	}
	if err := insertAttempt(ctx, tx, row); err != nil {
		return fmt.Errorf("rdq/postgres: recording attempt: %w", err)
	}
	return tx.Commit()
}

// staleIfNoRows maps a zero-rows-affected result to spi.ErrStaleClaim: a fenced
// mutation that matched no row means the token was stale (design 02 §3).
func staleIfNoRows(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return spi.ErrStaleClaim
	}
	return nil
}

// insertAttempt writes one attemptRow into rdq_attempt. A nil ErrorDetail binds
// as SQL NULL (never an empty jsonb payload).
func insertAttempt(ctx context.Context, q querier, r attemptRow) error {
	var detail any
	if len(r.ErrorDetail) > 0 {
		detail = r.ErrorDetail
	}
	_, err := q.ExecContext(ctx, `INSERT INTO rdq_attempt
		(task_id, attempt_no, started_at, finished_at, outcome,
		 error_type, error_message, error_detail, error_stack, residual)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		r.TaskID, r.AttemptNo, r.StartedAt, r.FinishedAt, r.Outcome,
		r.ErrorType, r.ErrorMessage, detail, r.ErrorStack, r.Residual)
	return err
}

// attemptRowFromAttempt decomposes a single engine-supplied Attempt into its
// rdq_attempt column projection. It mirrors the per-attempt decomposition in
// mapping.go (attemptRowsFromEnvelope) for the mutation path, which appends one
// attempt at a time rather than a whole history.
func attemptRowFromAttempt(taskID string, a envelope.Attempt) (attemptRow, error) {
	residual, err := encodeResidual(a.Residual)
	if err != nil {
		return attemptRow{}, fmt.Errorf("rdq/postgres: encoding attempt %d residual: %w", a.AttemptNo, err)
	}
	row := attemptRow{
		TaskID:     taskID,
		AttemptNo:  a.AttemptNo,
		StartedAt:  a.StartedAt,
		FinishedAt: a.FinishedAt,
		Outcome:    string(a.Outcome),
		Residual:   residual,
	}
	if a.Error != nil {
		t := a.Error.Type
		m := a.Error.Message
		row.ErrorType = &t
		row.ErrorMessage = &m
		row.ErrorStack = emptyToNil(a.Error.Stack)
		if len(a.Error.Detail) > 0 {
			row.ErrorDetail = a.Error.Detail
		}
	}
	return row, nil
}
