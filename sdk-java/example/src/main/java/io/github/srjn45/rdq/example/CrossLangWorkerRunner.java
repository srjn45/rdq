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

package io.github.srjn45.rdq.example;

import io.github.srjn45.rdq.client.envelope.Envelope;
import io.github.srjn45.rdq.worker.engine.Backoff;
import io.github.srjn45.rdq.worker.engine.Handler;
import io.github.srjn45.rdq.worker.engine.HandlerRegistry;
import io.github.srjn45.rdq.worker.engine.QueueSpec;
import io.github.srjn45.rdq.worker.engine.Worker;
import io.github.srjn45.rdq.worker.postgres.PostgresStorage;
import io.github.srjn45.rdq.worker.spi.Storage;
import org.postgresql.ds.PGSimpleDataSource;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Subprocess entry point for the T8.2 cross-language e2e integration test.
 *
 * <p>Runs the real Java {@link Worker} engine against a shared PostgreSQL backend to
 * claim and complete a redriven task — proving full cross-language wire compatibility
 * between the Go API/engine and the Java Worker.  The Worker now correctly separates
 * the attempt budget counter ({@code attemptCount()+1}, for retry-budget and backoff
 * decisions) from the monotonic history sequence ({@code attempts().size()+1}, for the
 * persisted {@code rdq_attempt.attempt_no}) so that a redriven task's preserved
 * history rows do not collide with the fresh-budget attempt (T5.7-class fix).
 *
 * <p><b>Required environment variables:</b>
 * <pre>
 *   RDQ_PG_HOST        PostgreSQL host  (e.g. localhost)
 *   RDQ_PG_PORT        PostgreSQL port  (e.g. 5432)
 *   RDQ_PG_DB          Database name    (e.g. rdq)
 *   RDQ_PG_USER        Database user    (e.g. rdq)
 *   RDQ_PG_PASS        Database password
 *   RDQ_QUEUE          Queue name to claim from
 *   RDQ_HANDLER_REF    Handler reference the task was submitted with
 *   RDQ_TASK_ID        Task ID to wait for
 * </pre>
 *
 * <p>Exits 0 when the task is handled and stored as SUCCEEDED, 1 on timeout or error.
 */
public final class CrossLangWorkerRunner {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    private static final Duration LEASE         = Duration.ofSeconds(10);
    private static final Duration TIMEOUT       = Duration.ofSeconds(90);

    private CrossLangWorkerRunner() {}

    @SuppressWarnings("SystemExitOutsideMain")
    public static void main(String[] args) {
        String host       = requireEnv("RDQ_PG_HOST");
        String port       = requireEnv("RDQ_PG_PORT");
        String db         = requireEnv("RDQ_PG_DB");
        String user       = requireEnv("RDQ_PG_USER");
        String pass       = requireEnv("RDQ_PG_PASS");
        String queue      = requireEnv("RDQ_QUEUE");
        String handlerRef = requireEnv("RDQ_HANDLER_REF");
        String taskId     = requireEnv("RDQ_TASK_ID");

        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl("jdbc:postgresql://" + host + ":" + port + "/" + db);
        ds.setUser(user);
        ds.setPassword(pass);

        Storage store = PostgresStorage.open(ds);

        System.out.printf("[crosslang-runner] connected to postgres %s:%s/%s; "
            + "waiting for task %s on queue %s via real Worker%n",
            host, port, db, taskId, queue);

        // Fires inside handle() when our target task is claimed.  We then stop
        // the worker and join its thread to ensure store.complete() finishes
        // before we exit.
        CountDownLatch done = new CountDownLatch(1);

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(handlerRef, new Handler() {
            @Override
            public String version() { return ""; }

            @Override
            public void handle(Envelope task) {
                System.out.printf("[crosslang-runner] Worker.handle task %s%n", task.id());
                if (taskId.equals(task.id())) {
                    done.countDown();
                }
            }
        });

        QueueSpec spec = QueueSpec.builder(queue)
            .maxAttempts(1)
            .backoff(Backoff.builder().initial(LEASE).build())
            .lease(LEASE)
            .pollInterval(POLL_INTERVAL)
            .build();

        Worker worker = Worker.builder(store, registry)
            .addQueue(spec)
            .sweepInterval(Duration.ZERO)
            .build();

        Thread workerThread = new Thread(() -> {
            try {
                worker.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "rdq-crosslang-worker");
        workerThread.setDaemon(true);
        workerThread.start();

        try {
            boolean ok = done.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            worker.stop();
            // Join the worker thread so store.complete() finishes before we exit.
            workerThread.join(LEASE.toMillis() * 3);
            if (ok) {
                System.out.printf("[crosslang-runner] task %s SUCCEEDED%n", taskId);
                System.exit(0);
            } else {
                System.err.printf("[crosslang-runner] TIMEOUT: task %s did not appear "
                    + "within %s on queue %s%n", taskId, TIMEOUT, queue);
                System.exit(1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[crosslang-runner] interrupted");
            worker.stop();
            System.exit(1);
        }
    }

    private static String requireEnv(String name) {
        String val = System.getenv(name);
        if (val == null || val.isEmpty()) {
            throw new IllegalStateException("Required env var not set: " + name);
        }
        return val;
    }
}
