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

import io.github.srjn45.rdq.client.envelope.Outcome;

/**
 * The terminal retry/permanent verdict the classification ladder produces for a
 * failed attempt (design 03 &sect;4). Mirrors {@code core/policy.Decision} in Go.
 *
 * <p>{@link #RETRYABLE} schedules another attempt (up to {@code max_attempts});
 * {@link #PERMANENT} dead-letters the task immediately.
 */
public enum Decision {
    RETRYABLE,
    PERMANENT;

    /** Maps this decision to the {@link Outcome} recorded in the attempt envelope. */
    public Outcome toOutcome() {
        return this == PERMANENT ? Outcome.PERMANENT_FAILURE : Outcome.RETRYABLE_FAILURE;
    }
}
