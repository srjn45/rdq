// SPDX-License-Identifier: Apache-2.0

package postgres

// Enqueue (design 02 §2 lifecycle, §3 invariant 5, T2.5) — the admission path
// that completes the Postgres Storage implementation. It decomposes the wire
// envelope with the T2.2 mapping and inserts the live task row plus any attempt
// history in one transaction, firing the Notify wakeup (notify.go) so a waiting
// worker claims it immediately.
//
// Idempotency (invariant 5, G8): a task id is unique across the whole store, so a
// re-enqueue of an existing id in the SAME queue is a no-op (safe submit retry)
// while the same id already present in a DIFFERENT queue is ErrIDConflict — never
// a silent no-op that would hand back a foreign envelope. The id may live in
// either task table, so both are consulted; the fresh INSERT is additionally
// guarded so two racing enqueues of a new id resolve to exactly one insert.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// insertTaskSQL admits a fresh task into rdq_task. claim_token/lease are absent
// for a just-enqueued task (it is not IN_FLIGHT). ON CONFLICT (id) DO NOTHING makes
// the insert a no-op under a concurrent enqueue of the same id (the rows-affected
// count tells the caller which enqueue won), closing the check-then-insert race.
const insertTaskSQL = `INSERT INTO rdq_task
	(id, queue, envelope_version, handler_ref, handler_version, payload,
	 payload_content_type, payload_ref, headers, status, attempt_count,
	 redrive_count, next_attempt_at, lease_expires_at, claim_token, created_at, residual)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NULL,$15,$16)
	ON CONFLICT (id) DO NOTHING`

// Enqueue admits task. It is idempotent by id within a queue: a re-enqueue of an
// existing id in the same queue changes nothing (invariant 5), while the same id
// in a different queue returns spi.ErrIDConflict (G8). A newly admitted task fires
// the Notify wakeup so a WaitDue-blocked worker claims it without waiting out its
// poll interval; the notification commits atomically with the insert.
func (s *Store) Enqueue(ctx context.Context, task envelope.Envelope) error {
	row, err := taskRowFromEnvelope(&task)
	if err != nil {
		return err
	}
	attempts, err := attemptRowsFromEnvelope(&task)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op once Commit succeeds

	// Fast path for the common retry: the id already exists. Same queue → no-op;
	// different queue → conflict. Nothing is written either way.
	existingQueue, found, err := lookupQueue(ctx, tx, row.ID)
	if err != nil {
		return fmt.Errorf("rdq/postgres: Enqueue lookup: %w", err)
	}
	if found {
		if existingQueue != row.Queue {
			return spi.ErrIDConflict
		}
		return nil // idempotent no-op; state left untouched
	}

	res, err := tx.ExecContext(ctx, insertTaskSQL,
		row.ID, row.Queue, row.EnvelopeVersion, row.HandlerRef, row.HandlerVersion,
		row.Payload, row.PayloadContentType, row.PayloadRef, row.Headers, row.Status,
		row.AttemptCount, row.RedriveCount, row.NextAttemptAt, row.LeaseExpiresAt,
		row.CreatedAt, row.Residual)
	if err != nil {
		return fmt.Errorf("rdq/postgres: Enqueue insert: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		// A concurrent enqueue of the same id won the insert. Re-resolve to tell
		// an idempotent same-queue no-op from a cross-queue conflict.
		existingQueue, _, err := lookupQueue(ctx, tx, row.ID)
		if err != nil {
			return fmt.Errorf("rdq/postgres: Enqueue lookup: %w", err)
		}
		if existingQueue != row.Queue {
			return spi.ErrIDConflict
		}
		return nil
	}

	for _, a := range attempts {
		if err := insertAttempt(ctx, tx, a); err != nil {
			return fmt.Errorf("rdq/postgres: Enqueue recording attempt: %w", err)
		}
	}
	if err := notifyDue(ctx, tx, row.Queue); err != nil {
		return err
	}
	return tx.Commit()
}

// lookupQueue reports the queue a task id currently lives in, checking both task
// tables (a task occupies exactly one). found is false when the id is unknown.
func lookupQueue(ctx context.Context, q querier, id string) (queue string, found bool, err error) {
	err = q.QueryRowContext(ctx,
		`SELECT queue FROM rdq_task     WHERE id = $1
		 UNION ALL
		 SELECT queue FROM rdq_dlq_task WHERE id = $1`, id).Scan(&queue)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return queue, true, nil
}
