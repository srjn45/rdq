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
 * Layer-2 wrapper that forces {@link Decision#RETRYABLE}, overriding code
 * classifiers or config globs that would otherwise mark the failure permanent
 * (design 03 &sect;4).
 *
 * <p>Mirrors Go {@code policy.Retryable(err)}.
 */
public final class RetryableException extends RuntimeException implements Classified {

    public RetryableException(Throwable cause) {
        super(cause != null ? cause.getMessage() : null, cause);
    }

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public Decision classification() {
        return Decision.RETRYABLE;
    }
}
