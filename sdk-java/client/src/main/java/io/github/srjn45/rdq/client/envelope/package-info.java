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
 * The rdq wire envelope (design 01): the language-neutral model of a task and
 * its canonical JSON codec.
 *
 * <p>{@link io.github.srjn45.rdq.client.envelope.Envelope} is the shared
 * contract; {@link io.github.srjn45.rdq.client.envelope.EnvelopeCodec} reads
 * and writes it in the canonical form that is byte-compatible with the Go SDK,
 * {@code rdq-server} and the storage plugins &mdash; the same bytes the frozen
 * cross-language compliance fixtures freeze. Serialization is JSON only; there
 * is no Java-native serialization (PRD FR-14).
 */
package io.github.srjn45.rdq.client.envelope;
