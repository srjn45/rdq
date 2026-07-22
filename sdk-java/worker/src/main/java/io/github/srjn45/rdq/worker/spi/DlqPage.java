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

import io.github.srjn45.rdq.client.envelope.Envelope;

import java.util.List;

/**
 * One page of {@link Storage#dlqList} results: the entries plus the cursor for
 * the next page. {@code nextCursor} is {@code ""} on the last page (design 02
 * &sect;2, &sect;3 invariant 8).
 *
 * @param tasks      the dead-letter entries on this page, in stable order
 * @param nextCursor the cursor to fetch the following page, or {@code ""} at end
 */
public record DlqPage(List<Envelope> tasks, String nextCursor) {

    public DlqPage {
        tasks = List.copyOf(tasks);
        nextCursor = nextCursor == null ? "" : nextCursor;
    }

    /** True when this is the last page (no further cursor). */
    public boolean isLast() {
        return nextCursor.isEmpty();
    }
}
