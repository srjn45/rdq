package code.srjn.retry.async;

import code.srjn.retry.RetryConfig;
import code.srjn.retry.async.queue.RetryQueue;
import code.srjn.retry.async.registry.FunctionRegistry;
import code.srjn.retry.async.registry.SerializedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryAsync {

    private static final Logger logger = LoggerFactory.getLogger(RetryAsync.class);

    private final RetryConfig config;
    private final RetryQueue queue;
    private final FunctionRegistry functionRegistry;

    public RetryAsync(RetryConfig config, RetryQueue queue, FunctionRegistry functionRegistry) {
        this.config = config;
        this.queue = queue;
        this.functionRegistry = functionRegistry;
    }

    public <T> void execute(SerializedConsumer<T> func, T arg) {
        try {
            functionRegistry.invoke(functionRegistry.getId(func), arg);
        } catch (Exception ex) {
            if (!config.shouldRetry(ex) || config.getAttempts() <= 1) {
                logger.error("func execution failed", ex);
                throw ex;
            }
            logger.warn("func execution failed, pushing to retry queue", ex);
            queue.enqueue(new RetryTask<>(functionRegistry.getId(func), arg, 1));
        }
    }
}
