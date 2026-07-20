package code.srjn.retry.async;

import java.util.Objects;

public class RetryTask<T> {
    private final String funcId;
    private final T arg;
    private final int attempt;

    public RetryTask(String funcId, T arg, int attempt) {
        this.funcId = funcId;
        this.arg = arg;
        this.attempt = attempt;
    }

    public String getFuncId() {
        return funcId;
    }

    public T getArg() {
        return arg;
    }

    public int getAttempt() {
        return attempt;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        RetryTask<?> retryTask = (RetryTask<?>) o;
        return attempt == retryTask.attempt && Objects.equals(funcId, retryTask.funcId) && Objects.equals(arg, retryTask.arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(funcId, arg, attempt);
    }
}
