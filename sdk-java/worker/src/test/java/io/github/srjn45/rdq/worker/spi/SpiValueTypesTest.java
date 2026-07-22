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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * Unit tests for the SPI value types &mdash; the small invariants each one
 * enforces (defensive copies, empty-selector detection, cursor/limit defaults).
 * No database needed.
 */
class SpiValueTypesTest {

    @Test
    void claimToken_wrapsAndRejectsNull() {
        assertThat(ClaimToken.of("tok").value()).isEqualTo("tok");
        assertThat(ClaimToken.of("tok")).hasToString("tok");
        assertThatNullPointerException().isThrownBy(() -> ClaimToken.of(null));
    }

    @Test
    void selector_distinguishesIdsFilterAndEmpty() {
        assertThat(Selector.none().selectsNothing()).isTrue();
        assertThat(Selector.byIds(List.of()).selectsNothing()).isTrue();
        assertThat(Selector.byIds(List.of("a")).selectsNothing()).isFalse();
        assertThat(Selector.byFilter(DlqFilter.none()).selectsNothing()).isFalse();

        // Ids are defensively copied.
        List<String> ids = new java.util.ArrayList<>(List.of("a", "b"));
        Selector sel = Selector.byIds(ids);
        ids.add("c");
        assertThat(sel.ids()).containsExactly("a", "b");
    }

    @Test
    void page_appliesCursorAndLimitDefaults() {
        assertThat(Page.first().after()).isEmpty();
        assertThat(Page.first().limit()).isZero();
        assertThat(Page.ofSize(25).limit()).isEqualTo(25);
        assertThat(new Page(10, null).after()).isEmpty();
        assertThat(Page.ofSize(10).after("cur").after()).isEqualTo("cur");
    }

    @Test
    void dlqFilter_builderAndNone() {
        DlqFilter none = DlqFilter.none();
        assertThat(none.errorType()).isNull();
        assertThat(none.includeAttempts()).isFalse();

        DlqFilter built = DlqFilter.builder()
            .errorType("billing.CardDeclined")
            .handlerRef("charge")
            .includeAttempts(true)
            .build();
        assertThat(built.errorType()).isEqualTo("billing.CardDeclined");
        assertThat(built.handlerRef()).isEqualTo("charge");
        assertThat(built.includeAttempts()).isTrue();
    }

    @Test
    void stats_defaultsNullAgeToZero() {
        assertThat(new Stats(1, 2, 3, null).oldestPendingAge()).isEqualTo(Duration.ZERO);
        assertThat(new Stats(1, 2, 3, Duration.ofSeconds(5)).oldestPendingAge())
            .isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void capabilities_carriesFlags() {
        Capabilities caps = new Capabilities(false, true, false);
        assertThat(caps.filterPushdown()).isTrue();
        assertThat(caps.notifyDue()).isFalse();
        assertThat(caps.batchEnqueue()).isFalse();
    }
}
