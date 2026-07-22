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
 * A pagination request: at most {@code limit} entries starting after
 * {@code after} (design 02 &sect;2). A zero or negative {@code limit} lets the
 * backend choose a default page size; the empty {@code after} cursor starts from
 * the first page.
 *
 * @param limit maximum entries to return, or &le;0 for the backend default
 * @param after an opaque cursor from a prior page, or {@code ""} to start
 */
public record Page(int limit, String after) {

    public Page {
        after = after == null ? "" : after;
    }

    /** The first page with a backend-chosen size. */
    public static Page first() {
        return new Page(0, "");
    }

    /** The first page of at most {@code limit} entries. */
    public static Page ofSize(int limit) {
        return new Page(limit, "");
    }

    /** A page of at most {@code limit} entries starting after {@code cursor}. */
    public Page after(String cursor) {
        return new Page(limit, cursor);
    }
}
