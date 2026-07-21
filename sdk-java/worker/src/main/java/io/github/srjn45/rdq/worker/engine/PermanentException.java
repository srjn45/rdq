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
 * Layer-2 wrapper that forces {@link Decision#PERMANENT}, dead-lettering the task
 * without further retries, regardless of code classifiers or config globs below it
 * in the precedence ladder (design 03 &sect;4).
 *
 * <p>The wrapped cause is reachable via {@link #getCause()} so the classifier's
 * {@link Classifier#errorType(Throwable)} still derives the type from the inner
 * cause, and code classifiers in lower layers can still inspect it.
 *
 * <p>Mirrors Go {@code policy.Permanent(err)}.
 */
public final class PermanentException extends RuntimeException implements Classified {

    public PermanentException(Throwable cause) {
        super(cause != null ? cause.getMessage() : null, cause);
    }

    public PermanentException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public Decision classification() {
        return Decision.PERMANENT;
    }
}
