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

/**
 * Thrown by {@link PostgresStorage#open} when the database's recorded schema
 * version differs from the version this build understands (design 05 G5). The
 * engine refuses to run rather than corrupting rows it does not understand: a
 * newer database needs a newer engine; an older database needs its migrations
 * run.
 */
public final class SchemaVersionMismatchException extends StorageException {

    private static final long serialVersionUID = 1L;

    private final int databaseVersion;
    private final int engineVersion;

    public SchemaVersionMismatchException(int databaseVersion, int engineVersion) {
        super(message(databaseVersion, engineVersion));
        this.databaseVersion = databaseVersion;
        this.engineVersion = engineVersion;
    }

    /** The schema version recorded in the database. */
    public int databaseVersion() {
        return databaseVersion;
    }

    /** The schema version this build understands. */
    public int engineVersion() {
        return engineVersion;
    }

    private static String message(int dbVersion, int engineVersion) {
        if (dbVersion > engineVersion) {
            return "rdq/postgres: schema version mismatch: database schema version " + dbVersion
                + " is newer than this engine's " + engineVersion + "; upgrade the engine";
        }
        return "rdq/postgres: schema version mismatch: database schema version " + dbVersion
            + " is older than this engine's " + engineVersion + "; run migrations";
    }
}
