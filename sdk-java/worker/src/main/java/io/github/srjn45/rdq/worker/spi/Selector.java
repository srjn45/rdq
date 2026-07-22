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

import java.util.List;

/**
 * Chooses tasks for {@link Storage#redrive}/{@link Storage#purge}: an explicit
 * id set OR a {@link DlqFilter}, never both (design 02 &sect;2). The empty
 * selector selects nothing.
 *
 * @param ids    explicit task ids, or {@code null} when selecting by filter
 * @param filter a DLQ filter, or {@code null} when selecting by ids
 */
public record Selector(List<String> ids, DlqFilter filter) {

    public Selector {
        ids = ids == null ? null : List.copyOf(ids);
    }

    /** A selector matching exactly the given ids. */
    public static Selector byIds(List<String> ids) {
        return new Selector(ids, null);
    }

    /** A selector matching every DLQ task passing {@code filter}. */
    public static Selector byFilter(DlqFilter filter) {
        return new Selector(null, filter);
    }

    /** The empty selector, which selects nothing. */
    public static Selector none() {
        return new Selector(null, null);
    }

    /** True when this selector matches nothing (no ids and no filter). */
    public boolean selectsNothing() {
        return (ids == null || ids.isEmpty()) && filter == null;
    }
}
