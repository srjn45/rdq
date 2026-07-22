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

/**
 * A classification result: the {@link Decision} the engine acts on, plus the
 * {@link Layer} that produced it. The layer is for observability and tests; the
 * engine acts only on the decision.
 */
public record Verdict(Decision decision, Layer layer) {

    /** Which rung of the precedence ladder produced this verdict (design 03 &sect;4). */
    public enum Layer {
        /** Layer 1: {@link OutcomeMapper} claimed the error (authoritative). */
        OUTCOME_MAPPER,
        /** Layer 2: a {@link Classified} wrapper ({@link PermanentException} / {@link RetryableException}). */
        WRAPPER,
        /** Layer 3: a {@link ClassRule} matched via exception-class hierarchy. */
        CODE_CLASSIFIER,
        /** Layer 4: a config-glob pattern matched the error type string. */
        CONFIG_GLOB,
        /** Layer 5: default &mdash; nothing matched, so the failure is retryable. */
        DEFAULT
    }
}
