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

package io.github.srjn45.rdq.worker.engine;

import io.github.srjn45.rdq.client.envelope.Attempt;
import io.github.srjn45.rdq.client.envelope.Envelope;
import io.github.srjn45.rdq.client.envelope.Status;
import io.github.srjn45.rdq.worker.spi.Capabilities;
import io.github.srjn45.rdq.worker.spi.Claimed;
import io.github.srjn45.rdq.worker.spi.ClaimToken;
import io.github.srjn45.rdq.worker.spi.DlqFilter;
import io.github.srjn45.rdq.worker.spi.DlqPage;
import io.github.srjn45.rdq.worker.spi.Page;
import io.github.srjn45.rdq.worker.spi.Selector;
import io.github.srjn45.rdq.worker.spi.StaleClaimException;
import io.github.srjn45.rdq.worker.spi.Stats;
import io.github.srjn45.rdq.worker.spi.Storage;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Minimal in-memory {@link Storage} for unit tests — no Docker required. Covers
 * the paths exercised by {@link Worker} unit tests. Thread-safe via {@code
 * synchronized} methods.
 */
final class MemStorage implements Storage {

    private static final class Row {
        Envelope env;
        String liveToken;
        final List<Attempt> attempts = new ArrayList<>();
    }

    private final Map<String, Row> rows = new LinkedHashMap<>();

    @Override
    public synchronized void enqueue(Envelope task) {
        rows.computeIfAbsent(task.id(), k -> {
            Row r = new Row();
            r.env = task;
            return r;
        });
    }

    @Override
    public synchronized List<Claimed> claimDue(String queue, int limit, Duration lease) {
        Instant now = Instant.now();
        List<Claimed> result = new ArrayList<>();
        for (Row row : rows.values()) {
            if (result.size() >= limit) break;
            Envelope e = row.env;
            if (!queue.equals(e.queue())) continue;
            if (e.status() != Status.PENDING) continue;
            Instant due = e.nextAttemptAt();
            if (due != null && due.isAfter(now)) continue;
            // Claim it
            String token = UUID.randomUUID().toString();
            row.liveToken = token;
            row.env = rebuildAs(e, Status.IN_FLIGHT, e.attemptCount(),
                null, now.plus(lease), row.attempts);
            result.add(new Claimed(row.env, ClaimToken.of(token)));
        }
        return result;
    }

    @Override
    public synchronized void extendLease(String id, ClaimToken token, Duration lease) {
        Row row = requireLive(id, token);
        Envelope e = row.env;
        row.env = rebuildAs(e, Status.IN_FLIGHT, e.attemptCount(),
            null, Instant.now().plus(lease), row.attempts);
    }

    @Override
    public synchronized void reschedule(String id, ClaimToken token, Attempt attempt, Instant nextAt) {
        Row row = requireLive(id, token);
        row.liveToken = null;
        row.attempts.add(attempt);
        Envelope e = row.env;
        row.env = rebuildAs(e, Status.PENDING, e.attemptCount() + 1,
            nextAt, null, row.attempts);
    }

    @Override
    public synchronized void complete(String id, ClaimToken token, Attempt attempt) {
        Row row = requireLive(id, token);
        row.liveToken = null;
        row.attempts.add(attempt);
        Envelope e = row.env;
        row.env = rebuildAs(e, Status.SUCCEEDED, e.attemptCount() + 1,
            null, null, row.attempts);
    }

    @Override
    public synchronized void deadLetter(String id, ClaimToken token, Attempt attempt) {
        Row row = requireLive(id, token);
        row.liveToken = null;
        row.attempts.add(attempt);
        Envelope e = row.env;
        row.env = rebuildAs(e, Status.DEAD, e.attemptCount() + 1,
            null, null, row.attempts);
    }

    @Override
    public DlqPage dlqList(String queue, DlqFilter filter, Page page) {
        return new DlqPage(Collections.emptyList(), "");
    }

    @Override
    public synchronized Envelope get(String id) {
        Row row = rows.get(id);
        if (row == null) throw new IllegalArgumentException("not found: " + id);
        return row.env;
    }

    @Override
    public int redrive(String queue, Selector selector) {
        return 0;
    }

    @Override
    public int purge(String queue, Selector selector) {
        return 0;
    }

    @Override
    public Stats stats(String queue) {
        return new Stats(0, 0, 0, Duration.ZERO);
    }

    @Override
    public int purgeSucceeded(String queue, Instant olderThan) {
        return 0;
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(false, false, false);
    }

    private Row requireLive(String id, ClaimToken token) {
        Row row = rows.get(id);
        if (row == null || !token.value().equals(row.liveToken)) {
            throw new StaleClaimException();
        }
        return row;
    }

    private static Envelope rebuildAs(Envelope e, Status status, int attemptCount,
                                      Instant nextAt, Instant leaseExpiresAt,
                                      List<Attempt> attempts) {
        return Envelope.builder()
            .envelopeVersion(e.envelopeVersion())
            .id(e.id())
            .queue(e.queue())
            .handlerRef(e.handlerRef())
            .handlerVersion(e.handlerVersion())
            .payload(e.payload())
            .payloadContentType(e.payloadContentType())
            .status(status)
            .attemptCount(attemptCount)
            .redriveCount(e.redriveCount())
            .nextAttemptAt(nextAt)
            .leaseExpiresAt(leaseExpiresAt)
            .createdAt(e.createdAt())
            .attempts(attempts.isEmpty() ? null : new ArrayList<>(attempts))
            .build();
    }

    static Envelope pendingTask(String id, String queue, String handlerRef) {
        Instant past = Instant.now().minusMillis(1);
        return Envelope.builder()
            .envelopeVersion(1)
            .id(id)
            .queue(queue)
            .handlerRef(handlerRef)
            .payload(new byte[]{1, 2, 3})
            .payloadContentType("application/octet-stream")
            .status(Status.PENDING)
            .nextAttemptAt(past)
            .createdAt(past)
            .build();
    }
}
