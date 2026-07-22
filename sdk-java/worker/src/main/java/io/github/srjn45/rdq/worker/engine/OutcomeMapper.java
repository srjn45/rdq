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

import java.util.Optional;

/**
 * Layer-1 (top) of the classification ladder (design 03 &sect;4, FR-28): a
 * per-queue code hook that inspects a handler exception and either claims it
 * &mdash; returning a non-empty {@link Optional} with a {@link Decision}, which is
 * authoritative and short-circuits every lower layer &mdash; or declines with an
 * empty {@code Optional} to defer to wrapper/code/glob/default layers.
 *
 * <p>Mirrors Go {@code policy.OutcomeMapper}.
 */
@FunctionalInterface
public interface OutcomeMapper {

    /**
     * Inspect {@code ex} and return the forced decision, or an empty optional to
     * pass through to the next layer. A null return is treated as empty.
     */
    Optional<Decision> apply(Exception ex);
}
