package code.srjn.retry.async.queue;

import code.srjn.retry.async.RetryTask;

import java.util.ArrayDeque;
import java.util.Queue;

public class InMemoryRetryQueue implements RetryQueue {

    Queue<RetryTask<?>> q = new ArrayDeque<>();

    @Override
    public <T> void enqueue(RetryTask<T> task) {
        q.offer(task);
    }

    @Override
    public <T> RetryTask<T> dequeue() {
        return (RetryTask<T>) q.poll();
    }
}
