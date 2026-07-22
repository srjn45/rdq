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
 * The PostgreSQL storage binding: a {@link
 * io.github.srjn45.rdq.worker.spi.Storage} implementation over the SHARED rdq
 * Postgres schema (design 02 &sect;4, design 05 G5).
 *
 * <p>This binding implements the SAME tables ({@code rdq_task},
 * {@code rdq_dlq_task}, {@code rdq_attempt}, {@code rdq_schema_version}) and the
 * SAME claim semantics ({@code FOR UPDATE SKIP LOCKED}, fencing token, lease
 * reclaim) as the Go plugin. The shared schema is a cross-language contract &mdash;
 * it is what makes the cross-language redrive loop work &mdash; so this binding
 * NEVER defines its own schema or migrations. The T2.1 migrations own the DDL;
 * {@link io.github.srjn45.rdq.worker.postgres.PostgresStorage#open} enforces the
 * {@code rdq_schema_version} gate at startup and refuses a mismatched schema.
 */
package io.github.srjn45.rdq.worker.postgres;
