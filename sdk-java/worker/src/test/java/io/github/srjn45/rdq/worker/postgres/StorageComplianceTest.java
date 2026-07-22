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
import io.github.srjn45.rdq.client.envelope.EnvelopeCodec;
import io.github.srjn45.rdq.client.envelope.ErrorInfo;
import io.github.srjn45.rdq.client.envelope.Outcome;
import io.github.srjn45.rdq.client.envelope.Status;
import io.github.srjn45.rdq.worker.spi.Claimed;
import io.github.srjn45.rdq.worker.spi.ClaimToken;
import io.github.srjn45.rdq.worker.spi.DlqFilter;
import io.github.srjn45.rdq.worker.spi.DlqPage;
import io.github.srjn45.rdq.worker.spi.IdConflictException;
import io.github.srjn45.rdq.worker.spi.Page;
import io.github.srjn45.rdq.worker.spi.Selector;
import io.github.srjn45.rdq.worker.spi.StaleClaimException;
import io.github.srjn45.rdq.worker.spi.Storage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;

/**
 * The Java port of the storage compliance kit (design 02 &sect;3): the same eight
 * correctness invariants the Go in-memory and Postgres backends pass, run against
 * a testcontainers Postgres executing the FROZEN T2.1 migrations. Passing this is
 * what freezes the storage contract as a cross-backend guarantee &mdash; the Java
 * binding claims exactly like the Go one (FOR UPDATE SKIP LOCKED, fencing token,
 * lease reclaim) against the SAME shared schema.
 *
 * <p>The class is skipped (not failed) when Docker is unavailable. Like the Go
 * kit it never injects a clock &mdash; the backend owns time (G9) &mdash; so
 * lease-expiry invariants use short real leases and wait past them.
 */
@Testcontainers(disabledWithoutDocker = true)
class StorageComplianceTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(TestPostgres.IMAGE);

    private static DataSource dataSource;

    private static final Duration SHORT_LEASE = Duration.ofMillis(40);
    private static final Duration LONG_LEASE = Duration.ofSeconds(10);
    private static final long EXPIRE_WAIT_MS = 250;

    @BeforeAll
    static void migrate() {
        dataSource = TestPostgres.dataSource(POSTGRES);
        TestPostgres.applyMigrations(dataSource);
    }

    @BeforeEach
    void reset() {
        TestPostgres.truncate(dataSource);
    }

    private Storage store() {
        return PostgresStorage.open(dataSource);
    }

    // --- invariant 1: no double claim ---------------------------------------

    @Test
    void concurrentExclusivity_handsEachTaskToExactlyOneClaimant() throws Exception {
        final String queue = "q.claims";
        final int tasks = 200;
        final int workers = 8;
        Storage s = store();
        for (int i = 0; i < tasks; i++) {
            s.enqueue(newPendingTask(String.format("t%03d", i), queue));
        }

        Map<String, Integer> claimCount = new ConcurrentHashMap<>();
        Map<String, Integer> tokens = new ConcurrentHashMap<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch done = new CountDownLatch(workers);
        for (int w = 0; w < workers; w++) {
            pool.execute(() -> {
                try {
                    while (true) {
                        // limit=1 keeps the claim grain small so many workers race
                        // on the same due set rather than each draining a batch.
                        List<Claimed> claimed = s.claimDue(queue, 1, LONG_LEASE);
                        if (claimed.isEmpty()) {
                            return; // queue drained: everything IN_FLIGHT, nothing due
                        }
                        for (Claimed c : claimed) {
                            claimCount.merge(c.task().id(), 1, Integer::sum);
                            tokens.merge(c.token().value(), 1, Integer::sum);
                            if (c.task().status() != Status.IN_FLIGHT) {
                                failure.compareAndSet(null, new AssertionError(
                                    "claimed task " + c.task().id() + " status = " + c.task().status()));
                            }
                        }
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            });
        }
        boolean finished = done.await(60, TimeUnit.SECONDS);
        pool.shutdownNow();
        assertThat(finished).as("all claim workers finished within the timeout").isTrue();

        if (failure.get() != null) {
            fail("concurrent claimDue failed", failure.get());
        }
        assertThat(claimCount).as("every enqueued task claimed exactly once").hasSize(tasks);
        assertThat(claimCount.values()).allMatch(n -> n == 1);
        assertThat(tokens).as("one distinct fencing token per claim").hasSize(tasks);
        assertThat(tokens.values()).allMatch(n -> n == 1);
    }

    @Test
    void dropWorkerReclaim_reclaimsAfterLeaseAndKillsOldToken() throws Exception {
        final String queue = "q.drop";
        Storage s = store();
        s.enqueue(newPendingTask("drop", queue));

        // A worker claims, then "crashes" — it never reports an outcome.
        Claimed dropped = claimOne(s, queue, SHORT_LEASE);

        Thread.sleep(EXPIRE_WAIT_MS); // wait past the lease for crash recovery
        Claimed reclaimed = claimOne(s, queue, LONG_LEASE);
        assertThat(reclaimed.token()).isNotEqualTo(dropped.token());

        assertStaleToken(s, "drop", dropped.token()); // the dropped token is dead
        // The reclaiming worker's token still works.
        s.complete("drop", reclaimed.token(), retryAttempt(2, "done"));
    }

    // --- invariant 2: fencing -----------------------------------------------

    @Test
    void fencing_staleTokenRejectedAndChangesNothing() throws Exception {
        final String queue = "q.fence";
        Storage s = store();
        s.enqueue(newPendingTask("t", queue));

        Claimed first = claimOne(s, queue, SHORT_LEASE);
        // A token that was never minted is stale from the start.
        assertStaleToken(s, "t", ClaimToken.of(first.token().value() + "-never-issued"));

        Thread.sleep(EXPIRE_WAIT_MS); // let the lease lapse and reclaim
        Claimed second = claimOne(s, queue, LONG_LEASE);
        assertThat(second.token()).isNotEqualTo(first.token());

        Envelope before = s.get("t");
        assertStaleToken(s, "t", first.token());
        Envelope after = s.get("t");
        assertThat(fingerprint(after)).isEqualTo(fingerprint(before));
        assertThat(after.status()).isEqualTo(Status.IN_FLIGHT); // the live second claim

        // The live (second) token still resolves the task.
        s.complete("t", second.token(), retryAttempt(2, "done"));
    }

    // --- invariant 3: lease recovery counts ---------------------------------

    @Test
    void leaseRecoveryCounts_appendsLeaseExpiredAtomically() throws Exception {
        final String queue = "q.lease";
        Storage s = store();
        s.enqueue(newPendingTask("t", queue));

        Claimed first = claimOne(s, queue, SHORT_LEASE);
        assertThat(first.task().attempts()).isNull();

        Thread.sleep(EXPIRE_WAIT_MS);
        Claimed reclaimed = claimOne(s, queue, LONG_LEASE);

        assertThat(reclaimed.task().attempts()).hasSize(1);
        Attempt a = reclaimed.task().attempts().get(0);
        assertThat(a.outcome()).isEqualTo(Outcome.LEASE_EXPIRED);
        assertThat(a.error()).isNotNull();
        assertThat(a.error().type()).isEqualTo("rdq.LeaseExpired");
        assertThat(reclaimed.task().attemptCount()).isEqualTo(1);

        // The count is durable, not just present on the returned Claimed.
        Envelope got = s.get("t");
        assertThat(got.attemptCount()).isEqualTo(1);
        assertThat(got.attempts()).hasSize(1);
    }

    // --- invariant 4: atomic transitions ------------------------------------

    @Test
    void atomicTransitions_everyMutationIsSelfConsistent() throws Exception {
        final String queue = "q.atomic";
        Storage s = store();
        s.enqueue(newPendingTask("t", queue));

        assertConsistent(s, "t", Status.PENDING);

        Claimed c = claimOne(s, queue, LONG_LEASE);
        assertConsistent(s, "t", Status.IN_FLIGHT);

        Instant nextAt = Instant.now().minusSeconds(1);
        s.reschedule("t", c.token(), retryAttempt(1, "boom"), nextAt);
        Envelope got = assertConsistent(s, "t", Status.PENDING);
        assertThat(got.attemptCount()).isEqualTo(1);
        assertThat(got.attempts()).hasSize(1);

        c = claimOne(s, queue, LONG_LEASE);
        s.complete("t", c.token(), retryAttempt(2, "ok"));
        got = assertConsistent(s, "t", Status.SUCCEEDED);
        assertThat(got.attempts()).hasSize(2);

        // The lease-reclaim append (invariant 3) is atomic with the re-lease.
        s.enqueue(newPendingTask("u", queue));
        claimOne(s, queue, SHORT_LEASE);
        Thread.sleep(EXPIRE_WAIT_MS);
        Claimed reclaimed = claimOne(s, queue, LONG_LEASE);
        assertThat(reclaimed.task().status()).isEqualTo(Status.IN_FLIGHT);
        assertThat(reclaimed.task().attempts()).hasSize(1);
    }

    // --- invariant 5: idempotent enqueue ------------------------------------

    @Test
    void idempotentEnqueue_sameQueueNoOp_crossQueueConflict() {
        Storage s = store();
        Envelope task = newPendingTask("dup", "q1");

        s.enqueue(task);
        s.enqueue(task); // duplicate admit (same queue) is a no-op

        // Advance state, then re-enqueue: the no-op must not reset it.
        Claimed c = claimOne(s, "q1", LONG_LEASE);
        s.reschedule("dup", c.token(), retryAttempt(1, "boom"), Instant.now().minusSeconds(1));
        s.enqueue(task);
        Envelope got = s.get("dup");
        assertThat(got.attemptCount()).isEqualTo(1);
        assertThat(got.attempts()).hasSize(1);

        // The same id in a different queue is a conflict, not a silent no-op.
        assertThatExceptionOfType(IdConflictException.class)
            .isThrownBy(() -> s.enqueue(newPendingTask("dup", "q2")));
    }

    // --- invariant 6: lossless envelope round-trip --------------------------

    @Test
    void losslessRoundTrip_pendingFixturesAreByteStable() {
        String[] fixtures = {"envelope_full.json", "unknown_fields.json", "lease_expired.json"};
        boolean sawUnknownFields = false;
        for (String name : fixtures) {
            byte[] want = TestPostgres.readFixture(name);
            Envelope env = EnvelopeCodec.decode(want);
            assertThat(env.status()).as("fixture %s is PENDING", name).isEqualTo(Status.PENDING);

            reset(); // fresh schema per fixture
            Storage s = store();
            s.enqueue(env);
            Envelope got = s.get(env.id());
            byte[] round = EnvelopeCodec.encode(got);
            assertThat(new String(round, StandardCharsets.UTF_8))
                .as("round-trip of %s is byte-stable", name)
                .isEqualTo(new String(want, StandardCharsets.UTF_8));

            if (!env.unknownFields().isEmpty()) {
                sawUnknownFields = true;
            }
        }
        assertThat(sawUnknownFields).as("residual preservation exercised").isTrue();
    }

    // --- invariant 7: redrive resets, history persists ----------------------

    @Test
    void redrive_resetsCountsPreservesHistoryAndReclaims() {
        final String queue = "q.redrive";
        Storage s = store();

        s.enqueue(newPendingTask("t", queue));
        Claimed c = claimOne(s, queue, LONG_LEASE);
        s.reschedule("t", c.token(), retryAttempt(1, "boom"), Instant.now().minusSeconds(1));
        c = claimOne(s, queue, LONG_LEASE);
        s.deadLetter("t", c.token(), retryAttempt(2, "boom"));

        Envelope before = s.get("t");
        assertThat(before.attemptCount()).isEqualTo(2);
        assertThat(before.attempts()).hasSize(2);

        int n = s.redrive(queue, Selector.byIds(List.of("t")));
        assertThat(n).isEqualTo(1);

        Envelope after = s.get("t");
        assertThat(after.status()).isEqualTo(Status.PENDING);
        assertThat(after.attemptCount()).isZero();
        assertThat(after.redriveCount()).isEqualTo(before.redriveCount() + 1);
        assertThat(after.attempts()).hasSize(2); // history kept
        assertThat(after.nextAttemptAt()).isNotNull();

        // It has left the DLQ and is claimable again.
        DlqPage page = s.dlqList(queue, DlqFilter.none(), Page.first());
        assertThat(page.tasks()).isEmpty();
        assertThat(claimOne(s, queue, LONG_LEASE).task().id()).isEqualTo("t");
    }

    // --- regression: LEASE_EXPIRED attempt_no after redrive -----------------

    /**
     * Regression for the LEASE_EXPIRED attempt_no collision: after a redrive
     * (attempt_count=0, history 1..N preserved), a subsequent lease expiry on the
     * redriven task must derive attempt_no from the history sequence (MAX+1), not
     * from the budget counter (attempt_count+1=1), which would collide with the
     * pre-redrive attempt_no=1 and cause a UNIQUE(task_id,attempt_no) violation
     * that permanently wedges the task.
     */
    @Test
    void redriveLeaseExpired_reclaim_noUniqueViolation() throws Exception {
        final String queue = "q.redrive.lease";
        Storage s = store();

        // Drive the task through two attempts into the DLQ.
        // History: attempt_no 1 (RETRYABLE_FAILURE), attempt_no 2 (PERMANENT_FAILURE).
        s.enqueue(newPendingTask("rl", queue));
        Claimed c = claimOne(s, queue, LONG_LEASE);
        s.reschedule("rl", c.token(), retryAttempt(1, "e1"), Instant.now().minusSeconds(1));
        c = claimOne(s, queue, LONG_LEASE);
        s.deadLetter("rl", c.token(), retryAttempt(2, "e2"));

        // Redrive: attempt_count resets to 0, history (attempt_no 1 and 2) preserved.
        int n = s.redrive(queue, Selector.byIds(List.of("rl")));
        assertThat(n).isEqualTo(1);

        // Claim with a short lease that expires nearly immediately.
        claimOne(s, queue, SHORT_LEASE);

        // Wait past the lease window.
        Thread.sleep(EXPIRE_WAIT_MS);

        // Reclaim. Before the fix: attempt_count=0 → INSERT attempt_no=1 → UNIQUE
        // violation → StorageException. After the fix: MAX(2)+1=3 → succeeds.
        Claimed reclaimed = claimOne(s, queue, LONG_LEASE);

        List<Attempt> history = reclaimed.task().attempts();
        assertThat(history).as("history: 2 pre-redrive + 1 LEASE_EXPIRED").hasSize(3);

        Attempt leaseExpired = history.get(2);
        assertThat(leaseExpired.attemptNo())
            .as("LEASE_EXPIRED attempt_no must be history-based (3), not budget-based (1)")
            .isEqualTo(3);
        assertThat(leaseExpired.outcome()).isEqualTo(Outcome.LEASE_EXPIRED);
    }

    // --- invariant 8: stable pagination -------------------------------------

    @Test
    void stablePagination_neitherSkipsNorDuplicatesUnderConcurrentInserts() {
        final String queue = "q.page";
        Storage s = store();

        List<String> original = List.of("d1", "d2", "d3", "d4", "d5");
        for (String id : original) {
            driveToDlq(s, id, queue, "boom");
        }

        List<String> seen = new ArrayList<>();
        String cursor = "";
        int pages = 0;
        while (true) {
            DlqPage page = s.dlqList(queue, DlqFilter.none(), new Page(2, cursor));
            for (Envelope e : page.tasks()) {
                seen.add(e.id());
            }
            pages++;
            if (page.isLast()) {
                break;
            }
            // A fresh arrival after the first page must not skip or duplicate the
            // entries already established before the cursor.
            if (pages == 1) {
                driveToDlq(s, "late", queue, "boom");
            }
            cursor = page.nextCursor();
            if (pages > 10) {
                fail("pagination did not terminate");
            }
        }

        assertThat(seen).as("no id appears on more than one page").doesNotHaveDuplicates();
        assertThat(seen).as("no original entry skipped across pages").containsAll(original);
    }

    // --- shared builders & drivers ------------------------------------------

    private static Instant pastDue() {
        return Instant.now().minusSeconds(1);
    }

    private static Envelope newPendingTask(String id, String queue) {
        Instant due = pastDue();
        return Envelope.builder()
            .envelopeVersion(1)
            .id(id)
            .queue(queue)
            .handlerRef("h.process")
            .payload(("payload-" + id).getBytes(StandardCharsets.UTF_8))
            .payloadContentType("application/octet-stream")
            .status(Status.PENDING)
            .nextAttemptAt(due)
            .createdAt(due)
            .build();
    }

    private static Attempt retryAttempt(int no, String errType) {
        Instant now = Instant.now();
        return Attempt.builder()
            .attemptNo(no)
            .startedAt(now)
            .finishedAt(now)
            .outcome(Outcome.RETRYABLE_FAILURE)
            .error(ErrorInfo.builder().type(errType).message("boom").build())
            .build();
    }

    private static Claimed claimOne(Storage s, String queue, Duration lease) {
        List<Claimed> claimed = s.claimDue(queue, 10, lease);
        assertThat(claimed).as("claimDue(%s) returns exactly one task", queue).hasSize(1);
        return claimed.get(0);
    }

    private static void driveToDlq(Storage s, String id, String queue, String errType) {
        s.enqueue(newPendingTask(id, queue));
        Claimed c = claimOne(s, queue, LONG_LEASE);
        s.deadLetter(id, c.token(), retryAttempt(1, errType));
    }

    /** Asserts every post-claim mutation rejects {@code token} with StaleClaim. */
    private static void assertStaleToken(Storage s, String id, ClaimToken token) {
        Attempt att = retryAttempt(99, "stale.attempt");
        assertThatExceptionOfType(StaleClaimException.class)
            .isThrownBy(() -> s.reschedule(id, token, att, Instant.now()));
        assertThatExceptionOfType(StaleClaimException.class)
            .isThrownBy(() -> s.complete(id, token, att));
        assertThatExceptionOfType(StaleClaimException.class)
            .isThrownBy(() -> s.deadLetter(id, token, att));
        assertThatExceptionOfType(StaleClaimException.class)
            .isThrownBy(() -> s.extendLease(id, token, Duration.ofMinutes(1)));
    }

    /** A compact summary of the mutable fields a stale mutation must never touch. */
    private static String fingerprint(Envelope e) {
        int attempts = e.attempts() == null ? 0 : e.attempts().size();
        return "status=" + e.status() + " attempts=" + attempts
            + " attempt_count=" + e.attemptCount() + " redrive_count=" + e.redriveCount()
            + " next=" + e.nextAttemptAt() + " lease=" + e.leaseExpiresAt();
    }

    private static Envelope assertConsistent(Storage s, String id, Status want) {
        Envelope got = s.get(id);
        assertThat(got.status()).isEqualTo(want);
        switch (want) {
            case PENDING -> {
                assertThat(got.nextAttemptAt()).as("PENDING has next_attempt_at").isNotNull();
                assertThat(got.leaseExpiresAt()).as("PENDING carries no lease").isNull();
            }
            case IN_FLIGHT -> assertThat(got.leaseExpiresAt()).as("IN_FLIGHT has a lease").isNotNull();
            case SUCCEEDED, DEAD -> {
                assertThat(got.leaseExpiresAt()).as("terminal carries no lease").isNull();
                assertThat(got.nextAttemptAt()).as("terminal has no next_attempt_at").isNull();
            }
            default -> { /* no extra checks */ }
        }
        return got;
    }
}
