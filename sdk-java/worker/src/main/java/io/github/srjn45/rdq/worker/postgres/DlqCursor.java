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

import io.github.srjn45.rdq.worker.spi.StaleCursorException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

/**
 * The keyset-pagination cursor for {@link PostgresStorage#dlqList} (design 02
 * &sect;3 invariant 8). A cursor packs a {@code (dead_lettered_at, id)} position
 * into an opaque, URL-safe token; paging returns entries strictly after it.
 * Because a fresh dead-letter always sorts at or after {@code now()}, it can
 * never perturb a page that predates the cursor &mdash; no skips, no duplicates
 * under concurrent inserts.
 *
 * <p>The token is tagged with {@link #PREFIX} so a foreign or corrupt cursor is
 * rejected with {@link StaleCursorException} rather than silently mis-paging.
 * The instant is stored as epoch-nanos; {@code rdq_dlq_task} timestamps are
 * microsecond-precise, so the value round-trips exactly.
 */
final class DlqCursor {

    private static final String PREFIX = "pgdlq1:";

    private final Instant deadLetteredAt;
    private final String id;

    private DlqCursor(Instant deadLetteredAt, String id) {
        this.deadLetteredAt = deadLetteredAt;
        this.id = id;
    }

    Instant deadLetteredAt() {
        return deadLetteredAt;
    }

    String id() {
        return id;
    }

    /** Encodes a {@code (deadLetteredAt, id)} position into an opaque cursor. */
    static String encode(Instant deadLetteredAt, String id) {
        long nanos = deadLetteredAt.getEpochSecond() * 1_000_000_000L + deadLetteredAt.getNano();
        String raw = PREFIX + nanos + ":" + id;
        return Base64.getUrlEncoder().withoutPadding()
            .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Resolves {@code cursor} to its threshold, or returns {@code null} for the
     * empty cursor (start from the beginning).
     *
     * @throws StaleCursorException if the cursor does not decode to a valid token
     */
    static DlqCursor decode(String cursor) {
        if (cursor == null || cursor.isEmpty()) {
            return null;
        }
        byte[] rawBytes;
        try {
            rawBytes = Base64.getUrlDecoder().decode(cursor);
        } catch (IllegalArgumentException ex) {
            throw new StaleCursorException();
        }
        String raw = new String(rawBytes, StandardCharsets.UTF_8);
        if (!raw.startsWith(PREFIX)) {
            throw new StaleCursorException();
        }
        String rest = raw.substring(PREFIX.length());
        int sep = rest.indexOf(':');
        if (sep < 0) {
            throw new StaleCursorException();
        }
        long nanos;
        try {
            nanos = Long.parseLong(rest.substring(0, sep));
        } catch (NumberFormatException ex) {
            throw new StaleCursorException();
        }
        Instant at = Instant.ofEpochSecond(
            Math.floorDiv(nanos, 1_000_000_000L), Math.floorMod(nanos, 1_000_000_000L));
        return new DlqCursor(at, rest.substring(sep + 1));
    }
}
