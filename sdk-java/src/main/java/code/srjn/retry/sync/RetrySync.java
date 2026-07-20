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
