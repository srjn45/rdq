package code.srjn.retry.async;

import code.srjn.retry.RetryConfig;
import code.srjn.retry.async.queue.InMemoryRetryQueue;
import code.srjn.retry.async.registry.FunctionNotRegisteredException;
import code.srjn.retry.async.registry.FunctionRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetryAsyncTest {

    @Test
    void execute() {
        var registry = new FunctionRegistry();
        registry.register("RetryAsyncTest::staticSuccessFunc", RetryAsyncTest::staticSuccessFunc);
        registry.register("RetryAsyncTest::staticFailureFunc", RetryAsyncTest::staticFailureFunc);

        var queue = new InMemoryRetryQueue();

        RetryAsync retryAsync = new RetryAsync(RetryConfig.builder().attempts(3).build(), queue, registry);

        assertDoesNotThrow(() -> retryAsync.execute(RetryAsyncTest::staticSuccessFunc, "srjn"));

        assertDoesNotThrow(() -> retryAsync.execute(RetryAsyncTest::staticFailureFunc, "other"));
        assertThat(queue.dequeue()).isEqualTo(new RetryTask<>("RetryAsyncTest::staticFailureFunc", "other", 1));

        assertThrows(FunctionNotRegisteredException.class, () -> retryAsync.execute(RetryAsyncTest::staticUnregisteredFunc, "unregistered"));

        TestClass tc = new TestClass();
        registry.register("tc::successFunc", tc::successFunc);
        registry.register("tc::failureFunc", tc::failureFunc);
        assertDoesNotThrow(() -> retryAsync.execute(tc::successFunc, 1));

        assertDoesNotThrow(() -> retryAsync.execute(tc::failureFunc, 0));
        assertThat(queue.dequeue()).isEqualTo(new RetryTask<>("tc::failureFunc", 0, 1));

        assertThrows(FunctionNotRegisteredException.class, () -> retryAsync.execute(tc::unregisteredFunc, 0));
    }

    static void staticSuccessFunc(String name) {
    }

    static void staticFailureFunc(String name) {
        throw new RuntimeException();
    }

    static void staticUnregisteredFunc(String name) {
    }

    static class TestClass {
        void successFunc(int a) {
        }

        void failureFunc(int a) {
            throw new RuntimeException();
        }

        void unregisteredFunc(int a) {
        }
    }

}