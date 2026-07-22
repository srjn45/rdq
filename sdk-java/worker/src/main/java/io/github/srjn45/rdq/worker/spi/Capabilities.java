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
 * Advertises optional backend features (design 02 &sect;2). The engine always
 * works against the mandatory floor of {@link Storage}; capabilities only remove
 * latency or transfer cost, never change correctness.
 *
 * @param notifyDue      the backend can block until a task may be due
 *                       (WaitDue-style push); claims still go through
 *                       {@link Storage#claimDue}. Absent it, the engine polls.
 * @param filterPushdown {@link DlqFilter} is evaluated natively by the backend;
 *                       absent it, the engine paginates and filters client-side.
 * @param batchEnqueue   the backend has a native multi-task enqueue path.
 */
public record Capabilities(boolean notifyDue, boolean filterPushdown, boolean batchEnqueue) {
}
