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

/**
 * The base unchecked exception for all {@link Storage} failures. Specific
 * contract violations are signalled by the subclasses ({@link
 * StaleClaimException}, {@link NotFoundException}, {@link StaleCursorException},
 * {@link IdConflictException}); an unexpected backend fault (a broken connection,
 * a SQL error) surfaces as a plain {@code StorageException} wrapping the cause.
 *
 * <p>Unchecked to match the SDK's codec convention (the envelope codec throws
 * {@code UncheckedIOException}): callers who care about a specific sentinel catch
 * its subclass; the rest let it propagate.
 */
public class StorageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
