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

package code.srjn.retry.sync;

import code.srjn.retry.RetryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class RetrySync {

    private static final Logger logger = LoggerFactory.getLogger(RetrySync.class);

    private final RetryConfig config;

    public RetrySync(RetryConfig config) {
        this.config = config;
    }

    public void execute(Runnable func) {
        execute(() -> {
            func.run();
            return null;
        });
    }

    public <T> T execute(Supplier<T> func) {
        int attempt = 0;
        while (true) {
            try {
                return func.get();
            } catch (Exception ex) {
                if (!config.shouldRetry(ex) || ++attempt == config.getAttempts()) {
                    String errMsg = String.format("retry exhausted after %d attempts", attempt);
                    logger.error(errMsg, ex);
                    throw ex;
                }
                String errMsg = String.format("retry attempt %d failed", attempt);
                logger.warn(errMsg, ex);
                try {
                    long delay =
                            (long) (config.getBackoff() * Math.pow(config.getBackoffMultiplier(), attempt - 1));
                    logger.debug("retry backoff for {} ms", delay);
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    logger.warn("retrying without backoff");
                }
            }
        }
    }
}
