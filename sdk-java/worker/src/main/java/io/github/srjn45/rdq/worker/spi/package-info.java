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

/**
 * The storage service-provider interface (design 02): the {@link
 * io.github.srjn45.rdq.worker.spi.Storage} contract every backend implements,
 * plus its value and error types. This is the Java shape of the same FROZEN
 * contract as the Go {@code core/spi.Storage} &mdash; idiomatic naming, identical
 * semantics.
 *
 * <p>The engine (embedded worker or {@code rdq-server}) owns all decisions
 * &mdash; policy, backoff, outcome classification; a backend owns only
 * durability and atomicity. Every mutating method is all-or-nothing, claims are
 * fenced by {@link io.github.srjn45.rdq.worker.spi.ClaimToken}, and the backend
 * owns the clock for due-ness and lease expiry (G9).
 */
package io.github.srjn45.rdq.worker.spi;
