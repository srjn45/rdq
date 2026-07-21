// SPDX-License-Identifier: Apache-2.0

package postgres

// Operational queries (design 02 §2 "--- ops ---", T2.4): the per-queue Stats
// snapshot that powers the Prometheus metrics (PRD FR-22) and PurgeSucceeded,
// which enforces task_ttl retention by deleting aged-out successes.
//
// Storage owns the clock (G9): OldestPendingAge is computed against the
// backend's now(), read in the same statement as the counts so the age reflects
// a single consistent instant rather than the engine's (possibly skewed) clock.

import (
	"context"
	"fmt"
	"time"

	"github.com/srjn45/rdq/core/spi"
)

// Stats returns a per-queue operational snapshot: PENDING and IN_FLIGHT counts
// and oldest-pending age from rdq_task, DLQ depth from rdq_dlq_task. All four
// figures plus now() come from one round trip. OldestPendingAge mirrors the
// memstore: the age of the oldest PENDING task by created_at, zero when the
// queue has no pending work.
func (s *Store) Stats(ctx context.Context, queue string) (spi.Stats, error) {
	const q = `
SELECT
    (SELECT count(*)      FROM rdq_task     WHERE queue = $1 AND status = 'PENDING'),
    (SELECT count(*)      FROM rdq_task     WHERE queue = $1 AND status = 'IN_FLIGHT'),
    (SELECT count(*)      FROM rdq_dlq_task WHERE queue = $1),
    (SELECT min(created_at) FROM rdq_task   WHERE queue = $1 AND status = 'PENDING'),
    now()`

	var (
		st            spi.Stats
		oldestPending *time.Time
		now           time.Time
	)
	if err := s.db.QueryRowContext(ctx, q, queue).Scan(
		&st.Pending, &st.InFlight, &st.DLQDepth, &oldestPending, &now,
	); err != nil {
		return spi.Stats{}, fmt.Errorf("rdq/postgres: Stats: %w", err)
	}
	if oldestPending != nil {
		if age := now.Sub(*oldestPending); age > 0 {
			st.OldestPendingAge = age
		}
	}
	return st, nil
}

// PurgeSucceeded removes SUCCEEDED tasks in queue that completed before
// olderThan, enforcing task_ttl retention (design 02 §7 OI-3: no archive, just
// delete). "Completed before" is measured by the terminal attempt's finished_at
// — the completion instant, mirroring the memstore's terminalAt — falling back
// to created_at for the (unexpected) case of a success with no attempt history.
// The task rows and their attempt history are removed in one atomic statement.
func (s *Store) PurgeSucceeded(ctx context.Context, queue string, olderThan time.Time) (int, error) {
	const q = `
WITH purged AS (
    DELETE FROM rdq_task t
    WHERE t.queue = $1
      AND t.status = 'SUCCEEDED'
      AND COALESCE(
              (SELECT max(a.finished_at) FROM rdq_attempt a WHERE a.task_id = t.id),
              t.created_at
          ) < $2
    RETURNING id
),
_att AS (
    DELETE FROM rdq_attempt WHERE task_id IN (SELECT id FROM purged)
)
SELECT count(*) FROM purged`

	var n int
	if err := s.db.QueryRowContext(ctx, q, queue, olderThan).Scan(&n); err != nil {
		return 0, fmt.Errorf("rdq/postgres: PurgeSucceeded: %w", err)
	}
	return n, nil
}
