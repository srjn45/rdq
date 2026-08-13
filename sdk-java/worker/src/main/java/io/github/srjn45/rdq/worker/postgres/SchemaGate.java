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

package io.github.srjn45.rdq.worker.postgres;

import io.github.srjn45.rdq.worker.spi.StorageException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The worker's {@code rdq_schema_version} startup gate (design 02 &sect;4,
 * design 05 G5, issue #54).
 *
 * <p>The Postgres schema is a cross-language contract: this Java binding and the
 * Go plugin bind to the SAME task tables ({@code rdq_task} / {@code rdq_dlq_task}
 * / {@code rdq_attempt}). Before an engine touches any row it reads
 * {@code rdq_schema_version} and refuses to run against an unknown (newer) or
 * unmigrated (older) task contract rather than corrupting rows it does not
 * understand.
 *
 * <p>The gate reads {@code task_contract_version}, NOT the overall
 * {@code version}. The overall version is bumped by every migration, including
 * server-only ones (config, audit) that never touch the task tables; gating on
 * it locked workers out of a database the moment the server gained a feature.
 * The task-contract counter advances only when the task tables themselves
 * change, so a worker survives server-only migrations while still refusing a
 * genuinely-changed task schema. Bump {@link #TASK_CONTRACT_VERSION} in lockstep
 * with the Go {@code storage/postgres.TaskContractVersion} whenever a migration
 * changes the task-table contract.
 */
final class SchemaGate {

    /**
     * The task-table contract version this worker binds to. Tracks the Go
     * {@code storage/postgres.TaskContractVersion}; the migrations write this
     * number into {@code rdq_schema_version.task_contract_version}. The
     * {@code SchemaContractLockstepTest} unit test fails the build if this drifts
     * from the Go constant.
     */
    static final int TASK_CONTRACT_VERSION = 1;

    // "42P01" is the SQLSTATE for undefined_table: rdq_schema_version has not
    // been created, so the migrations have not run.
    private static final String UNDEFINED_TABLE = "42P01";
    // "42703" is undefined_column: task_contract_version is absent, i.e. the
    // database is on a pre-0004 schema that predates this gate.
    private static final String UNDEFINED_COLUMN = "42703";

    private SchemaGate() {
    }

    /**
     * Reads {@code rdq_schema_version.task_contract_version} on {@code conn} and
     * verifies this worker may bind to the task tables.
     *
     * @throws SchemaNotInitializedException  if the migrations have not been
     *                                        applied (or predate the task-contract
     *                                        column)
     * @throws SchemaVersionMismatchException if the recorded task contract differs
     */
    static void verify(Connection conn) {
        int contract;
        try (Statement st = conn.createStatement();
            ResultSet rs =
                st.executeQuery("SELECT task_contract_version FROM rdq_schema_version WHERE singleton")) {
            if (!rs.next()) {
                throw new SchemaNotInitializedException();
            }
            contract = rs.getInt(1);
        } catch (SQLException ex) {
            if (UNDEFINED_TABLE.equals(ex.getSQLState()) || UNDEFINED_COLUMN.equals(ex.getSQLState())) {
                throw new SchemaNotInitializedException();
            }
            throw new StorageException("rdq/postgres: reading task-contract version", ex);
        }
        check(contract);
    }

    /**
     * The pure comparison behind {@link #verify}, split out so the gate logic is
     * unit-testable without a database.
     *
     * @throws SchemaVersionMismatchException if {@code databaseContract} differs
     *                                        from {@link #TASK_CONTRACT_VERSION}
     */
    static void check(int databaseContract) {
        if (databaseContract != TASK_CONTRACT_VERSION) {
            throw new SchemaVersionMismatchException(databaseContract, TASK_CONTRACT_VERSION);
        }
    }
}
