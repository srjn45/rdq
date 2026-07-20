package code.srjn.retry.sync;

import code.srjn.retry.RetryConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetrySyncTest {

    private final AtomicInteger counter = new AtomicInteger();

    @Test
    void execute_shouldRetryFuncOnFailure() {
        Instant startTime = Instant.now();
        counter.set(3);
        RetrySync retrySync = new RetrySync(RetryConfig.builder().attempts(3).build());
        String actual = retrySync.execute(() -> hello("srjn"));
        assertThat(actual).isEqualTo("hello srjn");
        Instant endTime = Instant.now();

        long execTime = endTime.toEpochMilli() - startTime.toEpochMilli();
        assertThat(execTime).isLessThan(1000);
    }

    @Test
    void execute_shouldThrowExceptionsWhenRetryExhausts() {
        Instant startTime = Instant.now();
        counter.set(3);
        RetrySync retrySync = new RetrySync(RetryConfig.builder().attempts(2).build());
        assertThrows(RuntimeException.class, () -> retrySync.execute(() -> hello("srjn")));
        Instant endTime = Instant.now();

        long execTime = endTime.toEpochMilli() - startTime.toEpochMilli();
        assertThat(execTime).isLessThan(1000);
    }

    @Test
    void execute_shouldRetryWithLinearBackoff() {
        Instant startTime = Instant.now();
        counter.set(3);
        RetrySync retrySync = new RetrySync(RetryConfig.builder().attempts(3).backoff(100).build());
        String actual = retrySync.execute(() -> hello("srjn"));
        assertThat(actual).isEqualTo("hello srjn");
        Instant endTime = Instant.now();

        long execTime = endTime.toEpochMilli() - startTime.toEpochMilli();
        assertThat(execTime).isGreaterThanOrEqualTo(200);
    }

    @Test
    void execute_shouldRetryWithExponentialBackoff() {
        Instant startTime = Instant.now();
        counter.set(4);
        RetrySync retrySync =
                new RetrySync(
                        RetryConfig.builder().attempts(4).backoff(100).backoffMultiplier(2).build());
        String actual = retrySync.execute(() -> hello("srjn"));
        assertThat(actual).isEqualTo("hello srjn");
        Instant endTime = Instant.now();

        long execTime = endTime.toEpochMilli() - startTime.toEpochMilli();
        assertThat(execTime).isGreaterThanOrEqualTo(700);
    }

    private String hello(String name) {
        if (counter.decrementAndGet() > 0) {
            throw new RuntimeException("something went wrong");
        }
        return String.format("hello %s", name);
    }

    @Test
    void execute_shouldRetryFuncThatReturnsVoidOnFailure() {
        counter.set(3);
        RetrySync retrySync = new RetrySync(RetryConfig.builder().attempts(3).build());
        assertDoesNotThrow(() -> retrySync.execute(() -> {
            noReply(1, 3);
            return null;
        }));
    }

    @Test
    void execute_shouldFailWhenRetryExhaustForVoidFunc() {
        counter.set(4);
        RetrySync retrySync = new RetrySync(RetryConfig.builder().build());
        assertThrows(RuntimeException.class, () -> retrySync.execute(() -> noReply(1, 3)));
    }

    private void noReply(int a, int b) {
        if (counter.decrementAndGet() > 0) {
            throw new RuntimeException("something went wrong");
        }
        System.out.println(a + b);
    }

    @Test
    void execute_shouldNotRetryForSkippableException() {
        RetrySync retrySync = new RetrySync(RetryConfig.builder().attempts(3).addSkippableException(TestSkippableException.class).build());

        counter.set(3);
        assertThrows(TestSkippableException.class, () -> retrySync.execute(this::throwSkippableException));

        counter.set(3);
        assertThrows(TestSkippableSubException.class, () -> retrySync.execute(this::throwSkippableSubException));
    }

    @Test
    void execute_shouldNotRetryForSkippableExceptions() {
        RetrySync retrySync = new RetrySync(RetryConfig.builder().attempts(3).skippableExceptions(List.of(TestSkippableSubException.class)).build());

        counter.set(3);
        assertDoesNotThrow(() -> retrySync.execute(this::throwSkippableException));

        counter.set(3);
        assertThrows(TestSkippableSubException.class, () -> retrySync.execute(this::throwSkippableSubException));
    }

    @Test
    void execute_shouldRetryOnlyRetryableExceptions() {
        RetrySync retrySync = new RetrySync(RetryConfig.builder().attempts(3).retryableExceptions(List.of(TestSkippableSubException.class)).build());

        counter.set(3);
        assertThrows(TestSkippableException.class, () -> retrySync.execute(this::throwSkippableException));

        counter.set(3);
        assertDoesNotThrow(() -> retrySync.execute(this::throwSkippableSubException));
    }

    @Test
    void execute_shouldRetryOnlyRetryableException() {
        RetrySync retrySync = new RetrySync(RetryConfig.builder().attempts(3).addRetryableException(TestSkippableSubException.class).build());

        counter.set(3);
        assertThrows(TestSkippableException.class, () -> retrySync.execute(this::throwSkippableException));

        counter.set(3);
        assertDoesNotThrow(() -> retrySync.execute(this::throwSkippableSubException));
    }

    static class TestSkippableException extends RuntimeException {
    }

    static class TestSkippableSubException extends TestSkippableException {
    }

    private String throwSkippableException() {
        if (counter.decrementAndGet() > 0) {
            throw new TestSkippableException();
        }
        return "success";
    }

    private String throwSkippableSubException() {
        if (counter.decrementAndGet() > 0) {
            throw new TestSkippableSubException();
        }
        return "success";
    }

}
