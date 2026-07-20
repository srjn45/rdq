package code.srjn.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RetryConfig {

    private static final Logger logger = LoggerFactory.getLogger(RetryConfig.class);

    private int attempts;
    private long backoff;
    private int backoffMultiplier;
    private List<Class<? extends Throwable>> retryableExceptions;
    private List<Class<? extends Throwable>> skippableExceptions;

    public int getAttempts() {
        return attempts;
    }

    public long getBackoff() {
        return backoff;
    }

    public int getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public boolean shouldRetry(Throwable ex) {
        boolean retry = retryableExceptions.isEmpty();

        // check if retryable
        for (var retryableException : retryableExceptions) {
            if (retryableException.isAssignableFrom(ex.getClass())) {
                retry = true;
                break;
            }
        }

        // check if should skip
        for (var skippableException : skippableExceptions) {
            if (skippableException.isAssignableFrom(ex.getClass())) {
                retry = false;
                break;
            }
        }

        logger.debug(String.format("func failed with %s exception", retry ? "retryable" : "skippable"), ex);
        return retry;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private static final int DEFAULT_ATTEMPTS = 1;
        private static final long DEFAULT_BACKOFF = 0;
        private static final int DEFAULT_BACKOFF_MULTIPLIER = 1;

        RetryConfig config;

        public Builder() {
            config = new RetryConfig();
            config.attempts = DEFAULT_ATTEMPTS;
            config.backoff = DEFAULT_BACKOFF;
            config.backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
            config.retryableExceptions = new ArrayList<>();
            config.skippableExceptions = new ArrayList<>();
        }

        public Builder attempts(int attempts) {
            config.attempts = attempts;
            return this;
        }

        public Builder backoff(long backoff) {
            config.backoff = backoff;
            return this;
        }

        public Builder backoffMultiplier(int backoffMultiplier) {
            config.backoffMultiplier = backoffMultiplier;
            return this;
        }

        public Builder retryableExceptions(List<Class<? extends Throwable>> retryableExceptions) {
            config.retryableExceptions = retryableExceptions;
            return this;
        }

        public Builder addRetryableException(Class<? extends Throwable> retryableException) {
            config.retryableExceptions.add(retryableException);
            return this;
        }

        public Builder skippableExceptions(List<Class<? extends Throwable>> skippableExceptions) {
            config.skippableExceptions = skippableExceptions;
            return this;
        }

        public Builder addSkippableException(Class<? extends Throwable> skippableException) {
            config.skippableExceptions.add(skippableException);
            return this;
        }

        public RetryConfig build() {
            return config;
        }
    }
}
