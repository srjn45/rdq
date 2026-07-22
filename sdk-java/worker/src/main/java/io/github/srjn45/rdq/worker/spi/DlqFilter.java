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

import java.time.Instant;

/**
 * Narrows a {@link Storage#dlqList}/{@link Storage#redrive}/{@link Storage#purge}
 * selection over the dead-letter queue (design 02 &sect;2). Unset fields are
 * unconstrained. A backend advertising {@link Capabilities#filterPushdown}
 * evaluates the filter natively; otherwise the engine paginates and filters
 * client-side.
 *
 * <p>The time range is a half-open interval: {@code deadLetteredAfter} is an
 * inclusive lower bound, {@code deadLetteredBefore} an exclusive upper bound;
 * a {@code null} end leaves that side open.
 *
 * @param errorType          matches the type of the final (dead-lettering)
 *                           attempt; {@code null}/blank is unconstrained
 * @param handlerRef         matches the task's {@code handler_ref};
 *                           {@code null}/blank is unconstrained
 * @param deadLetteredAfter  inclusive lower bound on death time, or {@code null}
 * @param deadLetteredBefore exclusive upper bound on death time, or {@code null}
 * @param includeAttempts    request full attempt histories in list results
 *                           (default {@code false}; histories make pages heavy,
 *                           G13). {@link Storage#get} always returns history.
 */
public record DlqFilter(
    String errorType,
    String handlerRef,
    Instant deadLetteredAfter,
    Instant deadLetteredBefore,
    boolean includeAttempts) {

    /** The empty, unconstrained filter (no bounds, attempts excluded). */
    public static DlqFilter none() {
        return new DlqFilter(null, null, null, null, false);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder for a {@link DlqFilter}. */
    public static final class Builder {
        private String errorType;
        private String handlerRef;
        private Instant deadLetteredAfter;
        private Instant deadLetteredBefore;
        private boolean includeAttempts;

        public Builder errorType(String errorType) {
            this.errorType = errorType;
            return this;
        }

        public Builder handlerRef(String handlerRef) {
            this.handlerRef = handlerRef;
            return this;
        }

        public Builder deadLetteredAfter(Instant deadLetteredAfter) {
            this.deadLetteredAfter = deadLetteredAfter;
            return this;
        }

        public Builder deadLetteredBefore(Instant deadLetteredBefore) {
            this.deadLetteredBefore = deadLetteredBefore;
            return this;
        }

        public Builder includeAttempts(boolean includeAttempts) {
            this.includeAttempts = includeAttempts;
            return this;
        }

        public DlqFilter build() {
            return new DlqFilter(
                errorType, handlerRef, deadLetteredAfter, deadLetteredBefore, includeAttempts);
        }
    }
}
