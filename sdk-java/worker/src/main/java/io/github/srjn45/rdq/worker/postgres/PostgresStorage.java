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

package io.github.srjn45.rdq.worker.postgres;

import io.github.srjn45.rdq.client.envelope.Attempt;
import io.github.srjn45.rdq.client.envelope.Envelope;
import io.github.srjn45.rdq.client.envelope.ErrorInfo;
import io.github.srjn45.rdq.client.envelope.Status;
import io.github.srjn45.rdq.worker.spi.Capabilities;
import io.github.srjn45.rdq.worker.spi.Claimed;
import io.github.srjn45.rdq.worker.spi.ClaimToken;
import io.github.srjn45.rdq.worker.spi.DlqFilter;
import io.github.srjn45.rdq.worker.spi.DlqPage;
import io.github.srjn45.rdq.worker.spi.IdConflictException;
import io.github.srjn45.rdq.worker.spi.NotFoundException;
import io.github.srjn45.rdq.worker.spi.Page;
import io.github.srjn45.rdq.worker.spi.Selector;
import io.github.srjn45.rdq.worker.spi.StaleClaimException;
import io.github.srjn45.rdq.worker.spi.Stats;
import io.github.srjn45.rdq.worker.spi.Storage;
import io.github.srjn45.rdq.worker.spi.StorageException;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The PostgreSQL {@link Storage} binding over the SHARED rdq schema (design 02
 * &sect;4, design 05 G5). It implements the SAME tables and claim semantics as
 * the Go plugin &mdash; {@code FOR UPDATE SKIP LOCKED} claim, fencing token,
 * lease reclaim &mdash; and NEVER defines its own schema; the T2.1 migrations own
 * the DDL. {@link #open(DataSource)} enforces the {@code rdq_schema_version} gate
 * at startup and refuses a mismatched schema.
 *
 * <p>Construct via {@link #open(DataSource)}. The caller owns the {@link
 * DataSource} lifecycle; a pooled source is recommended for the poll-based claim
 * loop. Every mutating call is atomic (a short transaction where it writes both a
 * state change and an attempt), and every post-claim mutation is fenced: a stale
 * or foreign token matches no row and yields {@link StaleClaimException} with
 * nothing changed.
 */
public final class PostgresStorage implements Storage {

    /**
     * The error type/message recorded on the {@code LEASE_EXPIRED} attempt
     * appended when an expired lease is reclaimed (G7). They match the Go binding
     * and the in-memory reference store so every backend produces the same record.
     */
    private static final String LEASE_EXPIRED_TYPE = "rdq.LeaseExpired";
    private static final String LEASE_EXPIRED_MESSAGE = "lease expired before an outcome was reported";

    private static final int DEFAULT_DLQ_PAGE_LIMIT = 100;

    private final DataSource dataSource;

    private PostgresStorage(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Opens a binding over {@code dataSource}, verifying the {@code
     * rdq_schema_version} gate before returning (design 05 G5).
     *
     * @throws SchemaNotInitializedException  if the migrations have not been applied
     * @throws SchemaVersionMismatchException if the recorded version differs
     */
    public static PostgresStorage open(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        try (Connection conn = dataSource.getConnection()) {
            SchemaGate.verify(conn);
        } catch (SQLException ex) {
            throw new StorageException("rdq/postgres: opening storage", ex);
        }
        return new PostgresStorage(dataSource);
    }

    // --- lifecycle -----------------------------------------------------------

    private static final String ENQUEUE_SQL = """
        INSERT INTO rdq_task
            (id, queue, envelope_version, handler_ref, handler_version, payload,
             payload_content_type, payload_ref, headers, status, attempt_count,
             redrive_count, next_attempt_at, lease_expires_at, claim_token,
             created_at, residual)
        SELECT ?, ?, ?, ?, ?, ?, ?, NULL, ?::jsonb, ?, ?, ?, ?, ?, NULL, ?, ?::jsonb
        WHERE NOT EXISTS (SELECT 1 FROM rdq_task     WHERE id = ?)
          AND NOT EXISTS (SELECT 1 FROM rdq_dlq_task WHERE id = ?)""";

    @Override
    public void enqueue(Envelope task) {
        Objects.requireNonNull(task, "task");
        inTransaction(conn -> {
            int inserted = insertIfAbsent(conn, task);
            if (inserted == 0) {
                // The id already exists somewhere: idempotent no-op in the SAME
                // queue (safe submit retry), conflict in a DIFFERENT one (G8).
                String existingQueue = existingQueue(conn, task.id());
                if (existingQueue != null && !existingQueue.equals(task.queue())) {
                    throw new IdConflictException(task.id());
                }
                return null;
            }
            List<Attempt> attempts = task.attempts();
            if (attempts != null) {
                for (Attempt a : attempts) {
                    insertAttempt(conn, task.id(), a);
                }
            }
            return null;
        });
    }

    private int insertIfAbsent(Connection conn, Envelope e) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(ENQUEUE_SQL)) {
            int i = 1;
            ps.setString(i++, e.id());
            ps.setString(i++, e.queue());
            ps.setInt(i++, e.envelopeVersion());
            ps.setString(i++, e.handlerRef());
            ps.setString(i++, e.handlerVersion());
            ps.setBytes(i++, e.payload());
            ps.setString(i++, e.payloadContentType());
            setJsonb(ps, i++, RowMapper.encodeStringMap(e.headers()));
            ps.setString(i++, statusWire(e.status()));
            ps.setInt(i++, e.attemptCount());
            ps.setInt(i++, e.redriveCount());
            setTimestamp(ps, i++, e.nextAttemptAt());
            setTimestamp(ps, i++, e.leaseExpiresAt());
            setTimestamp(ps, i++, e.createdAt());
            setJsonb(ps, i++, RowMapper.encodeResidual(e.unknownFields()));
            ps.setString(i++, e.id());
            ps.setString(i, e.id());
            return ps.executeUpdate();
        }
    }

    private String existingQueue(Connection conn, String id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT queue FROM rdq_task WHERE id = ? "
                + "UNION ALL SELECT queue FROM rdq_dlq_task WHERE id = ?")) {
            ps.setString(1, id);
            ps.setString(2, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    // --- claim ---------------------------------------------------------------

    // The design 02 §4 claim: a partial-index scan of due candidates locked with
    // FOR UPDATE SKIP LOCKED, atomically flipped to IN_FLIGHT with a fresh lease
    // and fencing token. The `due` CTE captures each candidate's pre-claim status
    // and lease so the caller can append a LEASE_EXPIRED attempt for rows it
    // reclaimed from an expired lease (design 02 §3 invariant 3).
    private static final String CLAIM_SQL = """
        WITH due AS (
            SELECT id,
                   status           AS prev_status,
                   lease_expires_at AS prev_lease
            FROM rdq_task
            WHERE queue = ?
              AND ( (status = 'PENDING'   AND next_attempt_at  <= now())
                 OR (status = 'IN_FLIGHT' AND lease_expires_at <= now()) )
            ORDER BY next_attempt_at
            LIMIT ?
            FOR UPDATE SKIP LOCKED
        ),
        claimed AS (
            UPDATE rdq_task t SET
                status           = 'IN_FLIGHT',
                lease_expires_at = now() + make_interval(secs => ?),
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
        SELECT * FROM claimed""";

    // Appends the LEASE_EXPIRED attempt recorded when an expired lease is reclaimed
    // (G7). attempt_no is derived from the task's history (MAX(attempt_no)+1), not
    // from attempt_count, so redriven tasks (attempt_count=0, history 1..N
    // preserved) do not collide on the UNIQUE(task_id,attempt_no) constraint. The
    // task row is locked within the claim transaction, making the subquery
    // race-free. The first ? and the subquery's ? are both bound to task_id.
    private static final String INSERT_LEASE_EXPIRED_SQL = """
        INSERT INTO rdq_attempt
            (task_id, attempt_no, started_at, finished_at, outcome, error_type, error_message)
        VALUES (?, (SELECT COALESCE(MAX(attempt_no), 0) + 1 FROM rdq_attempt WHERE task_id = ?),
                COALESCE(?, now()), now(), 'LEASE_EXPIRED', ?, ?)""";

    @Override
    public List<Claimed> claimDue(String queue, int limit, Duration lease) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(lease, "lease");
        if (limit <= 0) {
            return List.of();
        }
        return inTransaction(conn -> {
            List<ClaimedRow> rows = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(CLAIM_SQL)) {
                ps.setString(1, queue);
                ps.setInt(2, limit);
                ps.setDouble(3, seconds(lease));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(new ClaimedRow(
                            rs.getString("id"),
                            rs.getString("token"),
                            rs.getString("prev_status"),
                            RowMapper.readInstant(rs, "prev_lease"),
                            rs.getInt("attempt_count")));
                    }
                }
            }

            // Reclaimed leases (prev status IN_FLIGHT): append LEASE_EXPIRED and
            // count it, atomically with the re-claim (design 02 §3 invariant 3).
            for (ClaimedRow cr : rows) {
                if (!Status.IN_FLIGHT.wire().equals(cr.prevStatus)) {
                    continue;
                }
                try (PreparedStatement ps = conn.prepareStatement(INSERT_LEASE_EXPIRED_SQL)) {
                    ps.setString(1, cr.id);
                    ps.setString(2, cr.id); // subquery task_id
                    setTimestamp(ps, 3, cr.prevLease);
                    ps.setString(4, LEASE_EXPIRED_TYPE);
                    ps.setString(5, LEASE_EXPIRED_MESSAGE);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE rdq_task SET attempt_count = attempt_count + 1 WHERE id = ?")) {
                    ps.setString(1, cr.id);
                    ps.executeUpdate();
                }
            }

            // Re-read each claimed task with its full (now up-to-date) history.
            List<Claimed> claimed = new ArrayList<>(rows.size());
            for (ClaimedRow cr : rows) {
                Envelope env = getInTx(conn, cr.id);
                claimed.add(new Claimed(env, ClaimToken.of(cr.token)));
            }
            return claimed;
        });
    }

    /** A claimed row's storage-managed bookkeeping, drained before follow-ups. */
    private record ClaimedRow(String id, String token, String prevStatus, Instant prevLease, int attemptCount) {
    }

    // --- outcome resolution --------------------------------------------------

    private static final String EXTEND_LEASE_SQL = """
        UPDATE rdq_task SET lease_expires_at = now() + make_interval(secs => ?)
        WHERE id = ? AND status = 'IN_FLIGHT' AND claim_token::text = ?""";

    @Override
    public void extendLease(String id, ClaimToken token, Duration lease) {
        requireIdentity(id, token);
        Objects.requireNonNull(lease, "lease");
        withConnection(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(EXTEND_LEASE_SQL)) {
                ps.setDouble(1, seconds(lease));
                ps.setString(2, id);
                ps.setString(3, token.value());
                requireFenced(ps.executeUpdate());
            }
            return null;
        });
    }

    private static final String RESCHEDULE_SQL = """
        UPDATE rdq_task SET
            status = 'PENDING', next_attempt_at = ?, lease_expires_at = NULL,
            claim_token = NULL, attempt_count = attempt_count + 1
        WHERE id = ? AND status = 'IN_FLIGHT' AND claim_token::text = ?""";

    @Override
    public void reschedule(String id, ClaimToken token, Attempt attempt, Instant nextAt) {
        requireIdentity(id, token);
        Objects.requireNonNull(nextAt, "nextAt");
        spendClaim(id, attempt, conn -> {
            try (PreparedStatement ps = conn.prepareStatement(RESCHEDULE_SQL)) {
                setTimestamp(ps, 1, nextAt);
                ps.setString(2, id);
                ps.setString(3, token.value());
                requireFenced(ps.executeUpdate());
            }
        });
    }

    private static final String COMPLETE_SQL = """
        UPDATE rdq_task SET
            status = 'SUCCEEDED', next_attempt_at = NULL, lease_expires_at = NULL,
            claim_token = NULL, attempt_count = attempt_count + 1
        WHERE id = ? AND status = 'IN_FLIGHT' AND claim_token::text = ?""";

    @Override
    public void complete(String id, ClaimToken token, Attempt attempt) {
        requireIdentity(id, token);
        spendClaim(id, attempt, conn -> {
            try (PreparedStatement ps = conn.prepareStatement(COMPLETE_SQL)) {
                ps.setString(1, id);
                ps.setString(2, token.value());
                requireFenced(ps.executeUpdate());
            }
        });
    }

    // Moves a fenced task from rdq_task to rdq_dlq_task in one statement: the
    // data-modifying `moved` CTE deletes the live row (only when the token matches)
    // and feeds it to the DLQ insert, which stamps status DEAD, counts the final
    // attempt, records dead_lettered_at, and denormalizes the terminal error_type
    // for DLQFilter pushdown. RETURNING id is empty when the token did not match.
    private static final String DEAD_LETTER_SQL = """
        WITH moved AS (
            DELETE FROM rdq_task
            WHERE id = ? AND status = 'IN_FLIGHT' AND claim_token::text = ?
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
               NULL, created_at, residual, now(), ?
        FROM moved
        RETURNING id""";

    @Override
    public void deadLetter(String id, ClaimToken token, Attempt attempt) {
        requireIdentity(id, token);
        Objects.requireNonNull(attempt, "attempt");
        inTransaction(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(DEAD_LETTER_SQL)) {
                ps.setString(1, id);
                ps.setString(2, token.value());
                ps.setString(3, terminalErrorType(attempt)); // nullable text
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new StaleClaimException(); // token did not match: nothing moved
                    }
                }
            }
            insertAttempt(conn, id, attempt);
            return null;
        });
    }

    /** Runs a fenced state-change then appends the attempt, both in one transaction. */
    private void spendClaim(String id, Attempt attempt, FencedUpdate update) {
        Objects.requireNonNull(attempt, "attempt");
        inTransaction(conn -> {
            update.apply(conn); // throws StaleClaimException when the token did not match
            insertAttempt(conn, id, attempt);
            return null;
        });
    }

    @FunctionalInterface
    private interface FencedUpdate {
        void apply(Connection conn) throws SQLException;
    }

    private static final String INSERT_ATTEMPT_SQL = """
        INSERT INTO rdq_attempt
            (task_id, attempt_no, started_at, finished_at, outcome,
             error_type, error_message, error_detail, error_stack, residual)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?::jsonb)""";

    private void insertAttempt(Connection conn, String taskId, Attempt a) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(INSERT_ATTEMPT_SQL)) {
            int i = 1;
            ps.setString(i++, taskId);
            ps.setInt(i++, a.attemptNo());
            setTimestamp(ps, i++, a.startedAt());
            setTimestamp(ps, i++, a.finishedAt());
            ps.setString(i++, a.outcome().wire());
            ErrorInfo err = a.error();
            if (err != null) {
                ps.setString(i++, err.type());
                ps.setString(i++, err.message());
                setJsonb(ps, i++, RowMapper.writeNode(err.detail()));
                ps.setString(i++, err.stack());
            } else {
                ps.setString(i++, null);
                ps.setString(i++, null);
                setJsonb(ps, i++, null);
                ps.setString(i++, null);
            }
            setJsonb(ps, i, RowMapper.encodeResidual(a.unknownFields()));
            ps.executeUpdate();
        }
    }

    // --- DLQ -----------------------------------------------------------------

    @Override
    public DlqPage dlqList(String queue, DlqFilter filter, Page page) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(filter, "filter");
        Objects.requireNonNull(page, "page");
        DlqCursor after = DlqCursor.decode(page.after());
        int limit = page.limit() <= 0 ? DEFAULT_DLQ_PAGE_LIMIT : page.limit();

        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder("queue = ?");
        args.add(queue);
        if (isSet(filter.handlerRef())) {
            where.append(" AND handler_ref = ?");
            args.add(filter.handlerRef());
        }
        if (isSet(filter.errorType())) {
            where.append(" AND error_type = ?");
            args.add(filter.errorType());
        }
        if (filter.deadLetteredAfter() != null) {
            where.append(" AND dead_lettered_at >= ?"); // inclusive lower bound
            args.add(RowMapper.toOffset(filter.deadLetteredAfter()));
        }
        if (filter.deadLetteredBefore() != null) {
            where.append(" AND dead_lettered_at < ?"); // exclusive upper bound
            args.add(RowMapper.toOffset(filter.deadLetteredBefore()));
        }
        if (after != null) {
            where.append(" AND (dead_lettered_at, id) > (?, ?)");
            args.add(RowMapper.toOffset(after.deadLetteredAt()));
            args.add(after.id());
        }
        // Fetch one extra row to detect whether a further page exists.
        args.add(limit + 1);
        String sql = "SELECT " + RowMapper.TASK_COLUMNS + ", dead_lettered_at FROM rdq_dlq_task WHERE "
            + where + " ORDER BY dead_lettered_at, id LIMIT ?";

        return withConnection(conn -> {
            List<Envelope> collected = new ArrayList<>();
            List<Instant> deadAts = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bindAll(ps, args);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        List<Attempt> attempts = filter.includeAttempts()
                            ? loadAttempts(conn, rs.getString("id"))
                            : List.of();
                        collected.add(RowMapper.readEnvelope(rs, attempts));
                        deadAts.add(RowMapper.readInstant(rs, "dead_lettered_at"));
                    }
                }
            }

            String nextCursor = "";
            if (collected.size() > limit) {
                collected = collected.subList(0, limit);
                Envelope last = collected.get(limit - 1);
                nextCursor = DlqCursor.encode(deadAts.get(limit - 1), last.id());
            }
            return new DlqPage(collected, nextCursor);
        });
    }

    @Override
    public Envelope get(String id) {
        Objects.requireNonNull(id, "id");
        return withConnection(conn -> getInTx(conn, id));
    }

    private static final String GET_SQL =
        "SELECT " + RowMapper.TASK_COLUMNS + " FROM rdq_task WHERE id = ? "
            + "UNION ALL SELECT " + RowMapper.TASK_COLUMNS + " FROM rdq_dlq_task WHERE id = ?";

    /** Fetches one task (any status) with full history, or throws NotFound. */
    private Envelope getInTx(Connection conn, String id) throws SQLException {
        List<Attempt> attempts = loadAttempts(conn, id);
        try (PreparedStatement ps = conn.prepareStatement(GET_SQL)) {
            ps.setString(1, id);
            ps.setString(2, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new NotFoundException(id);
                }
                return RowMapper.readEnvelope(rs, attempts);
            }
        }
    }

    private static final String LOAD_ATTEMPTS_SQL = """
        SELECT task_id, attempt_no, started_at, finished_at, outcome,
               error_type, error_message, error_detail, error_stack, residual
        FROM rdq_attempt WHERE task_id = ? ORDER BY attempt_no""";

    private List<Attempt> loadAttempts(Connection conn, String taskId) throws SQLException {
        List<Attempt> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(LOAD_ATTEMPTS_SQL)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(RowMapper.readAttempt(rs));
                }
            }
        }
        return out;
    }

    // Redrive: DELETE ... RETURNING captures the selected rows; INSERT reads them
    // from the CTE so the selection is evaluated once. Back to PENDING, due at
    // now() (storage owns the clock, G9), attempt_count reset, redrive_count + 1,
    // history preserved (design 02 §3 invariant 7).
    private static final String REDRIVE_SQL_HEAD = """
        WITH moved AS (
            DELETE FROM rdq_dlq_task
            WHERE """;
    private static final String REDRIVE_SQL_TAIL = """

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
        RETURNING id""";

    @Override
    public int redrive(String queue, Selector selector) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(selector, "selector");
        if (selector.selectsNothing()) {
            return 0;
        }
        SelectorClause clause = selectorClause(queue, selector);
        // A space between HEAD's trailing `WHERE` (text blocks strip trailing
        // whitespace) and the clause keeps the keyword separated.
        String sql = REDRIVE_SQL_HEAD + " " + clause.where + REDRIVE_SQL_TAIL;
        return inTransaction(conn -> {
            int n = 0;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bindSelector(conn, ps, clause);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        n++;
                    }
                }
            }
            return n;
        });
    }

    // Purge: rdq_attempt has no FK cascade (a task moves between tables), so the
    // attempt rows are deleted explicitly alongside the task rows in one statement.
    private static final String PURGE_SQL_HEAD = """
        WITH purged AS (
            DELETE FROM rdq_dlq_task
            WHERE """;
    private static final String PURGE_SQL_TAIL = """

            RETURNING id
        ),
        _att AS (
            DELETE FROM rdq_attempt WHERE task_id IN (SELECT id FROM purged)
        )
        SELECT count(*) FROM purged""";

    @Override
    public int purge(String queue, Selector selector) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(selector, "selector");
        if (selector.selectsNothing()) {
            return 0;
        }
        SelectorClause clause = selectorClause(queue, selector);
        String sql = PURGE_SQL_HEAD + " " + clause.where + PURGE_SQL_TAIL;
        return inTransaction(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bindSelector(conn, ps, clause);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getInt(1);
                }
            }
        });
    }

    /** A selector's WHERE fragment (scoped to a queue) plus its ordered args. */
    private record SelectorClause(String where, List<Object> args, List<String> idArray) {
    }

    /**
     * Builds the WHERE clause selecting DEAD tasks in {@code queue} per
     * {@code sel}. IDs and Filter are mutually exclusive (design 02 &sect;2). The
     * caller has already ruled out the selects-nothing case.
     */
    private SelectorClause selectorClause(String queue, Selector sel) {
        List<Object> args = new ArrayList<>();
        args.add(queue);
        if (sel.ids() != null && !sel.ids().isEmpty()) {
            // id = ANY(?) binds the id list as a single SQL text[] array.
            return new SelectorClause("queue = ? AND id = ANY(?)", args, sel.ids());
        }
        DlqFilter f = sel.filter();
        StringBuilder where = new StringBuilder("queue = ?");
        if (isSet(f.handlerRef())) {
            where.append(" AND handler_ref = ?");
            args.add(f.handlerRef());
        }
        if (isSet(f.errorType())) {
            where.append(" AND error_type = ?");
            args.add(f.errorType());
        }
        if (f.deadLetteredAfter() != null) {
            where.append(" AND dead_lettered_at >= ?");
            args.add(RowMapper.toOffset(f.deadLetteredAfter()));
        }
        if (f.deadLetteredBefore() != null) {
            where.append(" AND dead_lettered_at < ?");
            args.add(RowMapper.toOffset(f.deadLetteredBefore()));
        }
        return new SelectorClause(where.toString(), args, null);
    }

    private void bindSelector(Connection conn, PreparedStatement ps, SelectorClause clause)
        throws SQLException {
        int i = bindAll(ps, clause.args);
        if (clause.idArray != null) {
            Array array = conn.createArrayOf("text", clause.idArray.toArray());
            ps.setArray(i, array);
        }
    }

    // --- ops -----------------------------------------------------------------

    private static final String STATS_SQL = """
        SELECT
            (SELECT count(*)        FROM rdq_task     WHERE queue = ? AND status = 'PENDING'),
            (SELECT count(*)        FROM rdq_task     WHERE queue = ? AND status = 'IN_FLIGHT'),
            (SELECT count(*)        FROM rdq_dlq_task WHERE queue = ?),
            (SELECT min(created_at) FROM rdq_task     WHERE queue = ? AND status = 'PENDING'),
            now()""";

    @Override
    public Stats stats(String queue) {
        Objects.requireNonNull(queue, "queue");
        return withConnection(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(STATS_SQL)) {
                ps.setString(1, queue);
                ps.setString(2, queue);
                ps.setString(3, queue);
                ps.setString(4, queue);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    long pending = rs.getLong(1);
                    long inFlight = rs.getLong(2);
                    long dlqDepth = rs.getLong(3);
                    OffsetDateTime oldestOdt = rs.getObject(4, OffsetDateTime.class);
                    Instant oldest = oldestOdt == null ? null : oldestOdt.toInstant();
                    Instant now = rs.getObject(5, OffsetDateTime.class).toInstant();
                    Duration age = Duration.ZERO;
                    if (oldest != null) {
                        Duration d = Duration.between(oldest, now);
                        if (!d.isNegative() && !d.isZero()) {
                            age = d;
                        }
                    }
                    return new Stats(pending, inFlight, dlqDepth, age);
                }
            }
        });
    }

    // PurgeSucceeded removes SUCCEEDED tasks that completed before olderThan
    // (task_ttl enforcement; design 02 §7 OI-3: no archive, just delete).
    // "Completed before" is the terminal attempt's finished_at, falling back to
    // created_at for a success with no attempt history.
    private static final String PURGE_SUCCEEDED_SQL = """
        WITH purged AS (
            DELETE FROM rdq_task t
            WHERE t.queue = ?
              AND t.status = 'SUCCEEDED'
              AND COALESCE(
                      (SELECT max(a.finished_at) FROM rdq_attempt a WHERE a.task_id = t.id),
                      t.created_at
                  ) < ?
            RETURNING id
        ),
        _att AS (
            DELETE FROM rdq_attempt WHERE task_id IN (SELECT id FROM purged)
        )
        SELECT count(*) FROM purged""";

    @Override
    public int purgeSucceeded(String queue, Instant olderThan) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(olderThan, "olderThan");
        return inTransaction(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(PURGE_SUCCEEDED_SQL)) {
                ps.setString(1, queue);
                setTimestamp(ps, 2, olderThan);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getInt(1);
                }
            }
        });
    }

    @Override
    public Capabilities capabilities() {
        // Filters are pushed down to SQL against the denormalized rdq_dlq_task
        // columns; LISTEN/NOTIFY and native batch enqueue are post-v1 (design 02
        // §4, §5), so this binding advertises the poll-based floor for those.
        return new Capabilities(false, true, false);
    }

    // --- plumbing ------------------------------------------------------------

    @FunctionalInterface
    private interface SqlWork<T> {
        T run(Connection conn) throws SQLException;
    }

    private <T> T withConnection(SqlWork<T> work) {
        try (Connection conn = dataSource.getConnection()) {
            return work.run(conn);
        } catch (SQLException ex) {
            throw new StorageException("rdq/postgres: query failed", ex);
        }
    }

    /** Runs {@code work} in a transaction; commits on success, rolls back on any failure. */
    private <T> T inTransaction(SqlWork<T> work) {
        try (Connection conn = dataSource.getConnection()) {
            boolean previousAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                T result = work.run(conn);
                conn.commit();
                return result;
            } catch (SQLException | RuntimeException ex) {
                conn.rollback();
                throw ex;
            } finally {
                conn.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException ex) {
            throw new StorageException("rdq/postgres: transaction failed", ex);
        }
    }

    /** Binds {@code args} positionally starting at 1; returns the next free index. */
    private static int bindAll(PreparedStatement ps, List<Object> args) throws SQLException {
        int i = 1;
        for (Object arg : args) {
            ps.setObject(i++, arg);
        }
        return i;
    }

    private static void setTimestamp(PreparedStatement ps, int index, Instant instant)
        throws SQLException {
        if (instant == null) {
            ps.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            ps.setObject(index, RowMapper.toOffset(instant));
        }
    }

    /** Binds a JSONB text value (the SQL uses a {@code ?::jsonb} cast), null-safe. */
    private static void setJsonb(PreparedStatement ps, int index, String json) throws SQLException {
        if (json == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, json);
        }
    }

    private static double seconds(Duration d) {
        return d.toNanos() / 1_000_000_000.0;
    }

    private static String statusWire(Status status) {
        return status == null ? null : status.wire();
    }

    private static String terminalErrorType(Attempt attempt) {
        ErrorInfo err = attempt.error();
        if (err != null && isSet(err.type())) {
            return err.type();
        }
        return null;
    }

    private static boolean isSet(String s) {
        return s != null && !s.isEmpty();
    }

    /** Maps a zero-rows-affected fenced update to a stale-claim failure. */
    private static void requireFenced(int rowsAffected) {
        if (rowsAffected == 0) {
            throw new StaleClaimException();
        }
    }

    private static void requireIdentity(String id, ClaimToken token) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(token, "token");
    }
}
