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
 * Marker implemented by per-call layer-2 wrappers (design 03 &sect;4, layer 2):
 * {@link PermanentException} and {@link RetryableException}. The classifier finds
 * the outermost {@code Classified} in the exception cause chain and short-circuits
 * to the forced decision, overriding code classifiers and config globs.
 *
 * <p>Custom handler wrappers may implement this interface — it is the layer-2
 * contract, not a closed set. Implementors must also extend {@link RuntimeException}
 * (or a checked exception) so the wrapped cause is reachable via
 * {@link Throwable#getCause()}.
 */
public interface Classified {
    /** The forced {@link Decision} this wrapper applies to its cause. */
    Decision classification();
}
