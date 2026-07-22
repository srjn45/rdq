// SPDX-License-Identifier: Apache-2.0
/*
 * Copyright 2025-2026 Srajan Pathak
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.srjn45.rdq.worker.spi;

import io.github.srjn45.rdq.client.envelope.Attempt;
import io.github.srjn45.rdq.client.envelope.Envelope;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * The mandatory floor every storage backend implements &mdash; the Java shape of
 * the same FROZEN contract as the Go {@code core/spi.Storage} (design 02
 * &sect;2). Method documentation restates the invariants the compliance kit
 * tests (design 02 &sect;3); optional accelerations are advertised via
 * {@link #capabilities()}.
 *
 * <p><b>Time authority.</b> The backend's clock is the single source of truth for
 * due-ness and lease expiry (G9): the engine computes {@code nextAttemptAt}
 * values, but the backend decides "now" when evaluating them (e.g. Postgres
 * {@code now()}). Engines tolerate clock skew accordingly.
 *
 * <p><b>Atomicity &amp; fencing.</b> Every mutating method is all-or-nothing: a
 * crash between any two calls leaves a task in a valid state &mdash; at worst
 * retried after lease expiry (at-least-once, never lost). Claims are fenced by
 * {@link ClaimToken}: at most one valid token per task exists at any moment, and
 * any outcome call bearing a stale token throws {@link StaleClaimException} and
 * changes nothing.
 */
public interface Storage {

    // --- lifecycle ---

    /**
     * Admits a task. Idempotent by {@code task.id()} within a queue: re-enqueue
     * of an existing id in the SAME queue is a no-op (safe submit retries). The
     * same id already present in a DIFFERENT queue is NOT a no-op &mdash; it
     * throws {@link IdConflictException}, since a silent cross-queue no-op would
     * return a confusing foreign envelope (G8). Maps to HTTP 409 at the API.
     *
     * @throws IdConflictException if the id exists in a different queue
     */
    void enqueue(Envelope task);

    /**
     * Atomically claims up to {@code limit} due tasks for {@code queue}. A task
     * is due when, by the backend's clock (G9):
     * <pre>
     *   (status=PENDING   AND nextAttemptAt   &lt;= now)
     *   OR (status=IN_FLIGHT AND leaseExpiresAt &lt;= now)   // crash recovery
     * </pre>
     * Claimed tasks become {@code IN_FLIGHT} with {@code leaseExpiresAt = now +
     * lease}. Reclaiming an expired lease atomically appends a
     * {@code LEASE_EXPIRED} attempt record. Ordering is best-effort by
     * {@code nextAttemptAt} ascending. It NEVER returns a task another live claim
     * holds, and mints one fencing {@link ClaimToken} per returned task.
     *
     * @return the claimed tasks, each paired with its token (may be empty)
     */
    List<Claimed> claimDue(String queue, int limit, Duration lease);

    /**
     * Renews the lease for a long-running handler (heartbeat). Throws
     * {@link StaleClaimException} if the lease was lost (task reclaimed
     * elsewhere), on which the handler must abandon its work.
     *
     * @throws StaleClaimException if {@code token} is no longer valid
     */
    void extendLease(String id, ClaimToken token, Duration lease);

    // --- outcome resolution (all require a valid token; StaleClaimException otherwise) ---

    /**
     * Failure path: append {@code attempt} and set the task {@code PENDING} with
     * {@code nextAttemptAt = nextAt} (engine-computed backoff).
     *
     * @throws StaleClaimException if {@code token} is no longer valid
     */
    void reschedule(String id, ClaimToken token, Attempt attempt, Instant nextAt);

    /**
     * Success path: append {@code attempt} and mark the task {@code SUCCEEDED}
     * (retained until {@code task_ttl} purge).
     *
     * @throws StaleClaimException if {@code token} is no longer valid
     */
    void complete(String id, ClaimToken token, Attempt attempt);

    /**
     * Exhaustion / permanent failure: append {@code attempt} and move the task to
     * the DLQ.
     *
     * @throws StaleClaimException if {@code token} is no longer valid
     */
    void deadLetter(String id, ClaimToken token, Attempt attempt);

    // --- DLQ ---

    /**
     * Pages the dead-letter queue for {@code queue} with stable cursor-based
     * pagination (no skips/dupes across pages while entries are added, design 02
     * &sect;3 invariant 8). Envelopes are returned WITHOUT attempt bodies unless
     * {@code filter.includeAttempts()} is set &mdash; histories make pages heavy
     * (G13). The returned {@link DlqPage#nextCursor()} is empty on the last page;
     * an unresolvable cursor throws {@link StaleCursorException}.
     *
     * @throws StaleCursorException if {@code page.after()} can no longer resolve
     */
    DlqPage dlqList(String queue, DlqFilter filter, Page page);

    /**
     * Fetches one task by id in ANY status (PENDING/IN_FLIGHT/SUCCEEDED/DEAD)
     * with full attempt history. Backs {@code GET /v1/tasks/{id}}.
     *
     * @throws NotFoundException if no task with {@code id} exists
     */
    Envelope get(String id);

    /**
     * Returns the selected DLQ tasks to {@code PENDING} with
     * {@code attemptCount=0} and {@code redriveCount} incremented, preserving
     * prior attempt history (envelope &sect;2).
     *
     * @return the count of tasks redriven
     */
    int redrive(String queue, Selector selector);

    /**
     * Permanently removes the selected DLQ tasks.
     *
     * @return the count of tasks removed
     */
    int purge(String queue, Selector selector);

    // --- ops ---

    /**
     * Returns a per-queue operational snapshot (pending, in-flight, dlq depth,
     * oldest-pending age) powering the Prometheus metrics.
     */
    Stats stats(String queue);

    /**
     * Removes {@code SUCCEEDED} tasks older than {@code olderThan} ({@code
     * task_ttl} enforcement).
     *
     * @return the count of tasks removed
     */
    int purgeSucceeded(String queue, Instant olderThan);

    /** Reports the optional features this backend accelerates. */
    Capabilities capabilities();
}
