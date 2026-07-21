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
import io.github.srjn45.rdq.client.envelope.Outcome;
import io.github.srjn45.rdq.client.envelope.Status;
import io.github.srjn45.rdq.worker.spi.Capabilities;
import io.github.srjn45.rdq.worker.spi.Claimed;
import io.github.srjn45.rdq.worker.spi.DlqFilter;
import io.github.srjn45.rdq.worker.spi.DlqPage;
import io.github.srjn45.rdq.worker.spi.Page;
import io.github.srjn45.rdq.worker.spi.Selector;
import io.github.srjn45.rdq.worker.spi.StaleClaimException;
import io.github.srjn45.rdq.worker.spi.Stats;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Exercises the operational and DLQ-filter paths the correctness-invariant suite
 * ({@link StorageComplianceTest}) does not directly cover &mdash; the happy-path
 * {@code extendLease}, {@code stats}, {@code purge}, {@code purgeSucceeded},
 * {@code capabilities}, and {@link DlqFilter} pushdown &mdash; so a SQL typo in
 * any of them surfaces in CI. Skipped (not failed) when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class OpsTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(TestPostgres.IMAGE);

    private static DataSource dataSource;

    private static final Duration LONG_LEASE = Duration.ofSeconds(10);

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

    @Test
    void extendLease_renewsLiveClaimAndRejectsStaleToken() {
        Storage s = store();
        s.enqueue(pending("t", "q.ops"));
        Claimed c = claimOne(s, "q.ops");
        Instant before = s.get("t").leaseExpiresAt();

        assertThatCode(() -> s.extendLease("t", c.token(), Duration.ofSeconds(30)))
            .doesNotThrowAnyException();
        Instant after = s.get("t").leaseExpiresAt();
        assertThat(after).isAfterOrEqualTo(before);

        assertThatExceptionOfType(StaleClaimException.class)
            .isThrownBy(() -> s.extendLease("t", stale(c), Duration.ofSeconds(30)));
    }

    @Test
    void stats_countsPendingInFlightAndDlqDepth() {
        Storage s = store();
        s.enqueue(pending("p1", "q.stats"));
        s.enqueue(pending("p2", "q.stats"));
        s.enqueue(pending("f1", "q.stats"));
        claimOne(s, "q.stats"); // one becomes IN_FLIGHT
        driveToDlq(s, "d1", "q.stats", "boom");

        Stats stats = s.stats("q.stats");
        assertThat(stats.pending()).isEqualTo(2);
        assertThat(stats.inFlight()).isEqualTo(1);
        assertThat(stats.dlqDepth()).isEqualTo(1);
        assertThat(stats.oldestPendingAge()).isGreaterThanOrEqualTo(Duration.ZERO);
    }

    @Test
    void purge_removesSelectedDlqTasks() {
        Storage s = store();
        driveToDlq(s, "d1", "q.purge", "boom");
        driveToDlq(s, "d2", "q.purge", "boom");

        int purged = s.purge("q.purge", Selector.byIds(List.of("d1")));
        assertThat(purged).isEqualTo(1);

        DlqPage page = s.dlqList("q.purge", DlqFilter.none(), Page.first());
        assertThat(page.tasks()).extracting(Envelope::id).containsExactly("d2");
    }

    @Test
    void purgeSucceeded_removesAgedOutSuccesses() throws InterruptedException {
        Storage s = store();
        s.enqueue(pending("ok", "q.ttl"));
        Claimed c = claimOne(s, "q.ttl");
        s.complete("ok", c.token(), attempt(1, Outcome.SUCCESS, null));
        Thread.sleep(20);

        // Nothing older than an instant before the completion.
        assertThat(s.purgeSucceeded("q.ttl", Instant.now().minusSeconds(60))).isZero();
        // Everything completed before now is purged.
        assertThat(s.purgeSucceeded("q.ttl", Instant.now())).isEqualTo(1);
        assertThat(s.stats("q.ttl").pending()).isZero();
    }

    @Test
    void dlqList_pushesDownErrorTypeAndHandlerFilters() {
        Storage s = store();
        driveToDlq(s, "a", "q.filter", "billing.CardDeclined");
        driveToDlq(s, "b", "q.filter", "net.Timeout");

        DlqPage byType = s.dlqList("q.filter",
            DlqFilter.builder().errorType("billing.CardDeclined").build(), Page.first());
        assertThat(byType.tasks()).extracting(Envelope::id).containsExactly("a");

        DlqPage withAttempts = s.dlqList("q.filter",
            DlqFilter.builder().errorType("net.Timeout").includeAttempts(true).build(), Page.first());
        assertThat(withAttempts.tasks()).hasSize(1);
        assertThat(withAttempts.tasks().get(0).attempts()).hasSize(1);
    }

    @Test
    void redrive_byFilterSelectsMatchingTasks() {
        Storage s = store();
        driveToDlq(s, "a", "q.rd", "billing.CardDeclined");
        driveToDlq(s, "b", "q.rd", "net.Timeout");

        int n = s.redrive("q.rd",
            Selector.byFilter(DlqFilter.builder().errorType("billing.CardDeclined").build()));
        assertThat(n).isEqualTo(1);

        // 'a' left the DLQ and is claimable; 'b' stays dead.
        assertThat(s.get("a").status()).isEqualTo(Status.PENDING);
        assertThat(s.dlqList("q.rd", DlqFilter.none(), Page.first()).tasks())
            .extracting(Envelope::id).containsExactly("b");
    }

    @Test
    void purge_byFilterAndEmptySelector() {
        Storage s = store();
        driveToDlq(s, "a", "q.pf", "billing.CardDeclined");
        driveToDlq(s, "b", "q.pf", "net.Timeout");

        // Empty selector short-circuits to zero without touching anything.
        assertThat(s.purge("q.pf", Selector.none())).isZero();
        assertThat(s.redrive("q.pf", Selector.none())).isZero();

        int n = s.purge("q.pf",
            Selector.byFilter(DlqFilter.builder().handlerRef("h.process").build()));
        assertThat(n).isEqualTo(2); // both share handler_ref
        assertThat(s.stats("q.pf").dlqDepth()).isZero();
    }

    @Test
    void capabilities_advertisesFilterPushdownOnly() {
        Capabilities caps = store().capabilities();
        assertThat(caps.filterPushdown()).isTrue();
        assertThat(caps.notifyDue()).isFalse();
        assertThat(caps.batchEnqueue()).isFalse();
    }

    // --- helpers -------------------------------------------------------------

    private static Envelope pending(String id, String queue) {
        Instant due = Instant.now().minusSeconds(1);
        return Envelope.builder()
            .envelopeVersion(1).id(id).queue(queue).handlerRef("h.process")
            .payload(("p-" + id).getBytes(StandardCharsets.UTF_8))
            .payloadContentType("application/octet-stream")
            .status(Status.PENDING).nextAttemptAt(due).createdAt(due).build();
    }

    private static Attempt attempt(int no, Outcome outcome, String errType) {
        Instant now = Instant.now();
        Attempt.Builder b = Attempt.builder()
            .attemptNo(no).startedAt(now).finishedAt(now).outcome(outcome);
        if (errType != null) {
            b.error(ErrorInfo.builder().type(errType).message("boom").build());
        }
        return b.build();
    }

    private static Claimed claimOne(Storage s, String queue) {
        List<Claimed> claimed = s.claimDue(queue, 10, LONG_LEASE);
        assertThat(claimed).hasSize(1);
        return claimed.get(0);
    }

    private static void driveToDlq(Storage s, String id, String queue, String errType) {
        s.enqueue(pending(id, queue));
        Claimed c = claimOne(s, queue);
        s.deadLetter(id, c.token(), attempt(1, Outcome.RETRYABLE_FAILURE, errType));
    }

    private static io.github.srjn45.rdq.worker.spi.ClaimToken stale(Claimed c) {
        return io.github.srjn45.rdq.worker.spi.ClaimToken.of(c.token().value() + "-stale");
    }
}
