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

import java.time.Duration;

/**
 * A per-queue operational snapshot backing the Prometheus metrics (PRD FR-22).
 *
 * @param pending          count of {@code PENDING} tasks
 * @param inFlight         count of {@code IN_FLIGHT} tasks
 * @param dlqDepth         count of dead-lettered tasks
 * @param oldestPendingAge age of the oldest {@code PENDING} task by the backend's
 *                         clock, or {@link Duration#ZERO} when none is pending
 */
public record Stats(long pending, long inFlight, long dlqDepth, Duration oldestPendingAge) {

    public Stats {
        oldestPendingAge = oldestPendingAge == null ? Duration.ZERO : oldestPendingAge;
    }
}
