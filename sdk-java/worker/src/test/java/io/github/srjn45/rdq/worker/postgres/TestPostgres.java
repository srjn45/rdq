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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test harness for the Postgres binding: a testcontainers Postgres running the
 * FROZEN T2.1 migrations. The Java binding binds to the SAME schema as the Go
 * plugin (design 05 G5), so the migration SQL is read straight from
 * {@code storage/postgres/migrations/} &mdash; it is never forked here.
 */
final class TestPostgres {

    /**
     * A recent Postgres image; {@code gen_random_uuid()} (used by the claim) is
     * built in from PG 13 onward, and the schema uses only core types.
     */
    static final DockerImageName IMAGE = DockerImageName.parse("postgres:16-alpine");

    private TestPostgres() {
    }

    /** A stopped container ready to {@code start()}; the caller owns its lifecycle. */
    static PostgreSQLContainer<?> container() {
        return new PostgreSQLContainer<>(IMAGE);
    }

    /** A {@link DataSource} for {@code container}, which must already be running. */
    static DataSource dataSource(PostgreSQLContainer<?> container) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(container.getJdbcUrl());
        ds.setUser(container.getUsername());
        ds.setPassword(container.getPassword());
        return ds;
    }

    /** Applies the frozen T2.1 up-migration to {@code dataSource} (idempotent DDL, run once). */
    @SuppressFBWarnings(
        value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
        justification = "the migration script is the trusted, frozen T2.1 SQL read from the repo")
    static void applyMigrations(DataSource dataSource) {
        String sql = readMigration();
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            // The migration is a multi-statement script; a plain Statement runs the
            // whole ;-separated batch in one call.
            st.execute(sql);
        } catch (SQLException ex) {
            throw new RuntimeException("applying T2.1 migration", ex);
        }
    }

    /** Empties every rdq table so each test starts from a clean, id-reset schema. */
    static void truncate(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute("TRUNCATE rdq_task, rdq_dlq_task, rdq_attempt RESTART IDENTITY");
        } catch (SQLException ex) {
            throw new RuntimeException("truncating rdq tables", ex);
        }
    }

    /** The frozen T2.1 up-migration SQL, located by walking up from the test cwd. */
    static String readMigration() {
        Path file = repoFile("storage/postgres/migrations/0001_init.up.sql");
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new UncheckedIOException("reading migration " + file, ex);
        }
    }

    /** Reads a frozen envelope fixture by name from {@code core/envelope/testdata/}. */
    static byte[] readFixture(String name) {
        Path file = repoDir("core/envelope/testdata").resolve(name);
        try {
            return Files.readAllBytes(file);
        } catch (IOException ex) {
            throw new UncheckedIOException("reading fixture " + name, ex);
        }
    }

    private static Path repoFile(String relative) {
        Path dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve(relative);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException(
            "could not locate " + relative + " from " + System.getProperty("user.dir"));
    }

    private static Path repoDir(String relative) {
        Path dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve(relative);
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException(
            "could not locate " + relative + " from " + System.getProperty("user.dir"));
    }
}
