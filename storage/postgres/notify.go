// SPDX-License-Identifier: Apache-2.0

package postgres

// Change notification (design 02 §2 "Optional capabilities", §4, T2.5). Postgres
// LISTEN/NOTIFY realizes the Notify capability: Enqueue and Reschedule fire a
// pg_notify when a task is (or becomes) claimable, and WaitDue blocks a worker
// until such a notification arrives instead of polling. It only removes idle
// latency — claims still go through the mandatory ClaimDue floor, which alone
// decides due-ness (design 02 §2), so a spurious or missed wakeup is at worst a
// slightly early or late poll, never a correctness problem.
//
// core/spi is a FROZEN contract and deliberately has NO Notifier interface:
// WaitDue is a concrete method on *Store that the engine discovers by a type
// assertion. All notifications ride a single fixed channel (dueChannel) with the
// queue name as the payload, so an arbitrary queue string never has to be a valid
// SQL identifier and a worker filters to its own queue in WaitDue.

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
)

// dueChannel is the single LISTEN/NOTIFY channel every enqueue/reschedule signals
// on; the notified queue rides in the payload. A fixed, valid-identifier channel
// keeps arbitrary queue names out of the channel name (which must be a Postgres
// identifier) — WaitDue filters by payload instead.
const dueChannel = "rdq_due"

// notifyDue fires a pg_notify announcing that queue may have claimable work. It
// runs on the caller's querier so, inside a transaction, the notification is held
// and delivered atomically when that transaction commits (a rolled-back enqueue
// wakes nobody).
func notifyDue(ctx context.Context, q querier, queue string) error {
	if _, err := q.ExecContext(ctx, "SELECT pg_notify($1, $2)", dueChannel, queue); err != nil {
		return fmt.Errorf("rdq/postgres: pg_notify: %w", err)
	}
	return nil
}

// WaitDue blocks until a task may be due in queue, unblocking on the LISTEN/NOTIFY
// fired by a concurrent Enqueue or Reschedule (design 02 §2 Notify). It is the
// concrete realization of the Notify capability the engine type-asserts for; the
// engine bounds the wait with ctx (a poll interval) so a missed edge only delays a
// re-poll. It returns nil when woken by a notification for queue, or ctx.Err() when
// ctx is cancelled or its deadline passes.
//
// The wait holds a dedicated pooled connection: LISTEN and the blocking receive
// must share one backend, and the connection is returned to the pool (Close) when
// the wait ends. Notifications for other queues are skipped, so one shared channel
// serves every queue without cross-waking workers.
func (s *Store) WaitDue(ctx context.Context, queue string) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	// pgx's ResetSession does not UNLISTEN, so drop the registration before the
	// connection returns to the pool — otherwise a reused backend keeps buffering
	// notifications for nobody. A fresh context is used because ctx is often
	// already done (deadline reached) by the time the wait ends.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = conn.ExecContext(cleanupCtx, "UNLISTEN "+dueChannel)
		cancel()
		_ = conn.Close()
	}()

	// LISTEN before the first receive so any notification committed after this
	// point is buffered by the driver and delivered, not lost.
	if _, err := conn.ExecContext(ctx, "LISTEN "+dueChannel); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("rdq/postgres: LISTEN: %w", err)
	}

	for {
		var payload string
		err := conn.Raw(func(dc any) error {
			// database/sql hands us the pgx driver conn; drop to the native pgx
			// connection to block on notifications on this very backend.
			n, err := dc.(*stdlib.Conn).Conn().WaitForNotification(ctx)
			if err != nil {
				return err
			}
			payload = n.Payload
			return nil
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("rdq/postgres: WaitDue: %w", err)
		}
		if payload == queue {
			return nil // work may be due; the engine claims via ClaimDue
		}
		// Notification for a different queue on the shared channel: keep waiting.
	}
}
