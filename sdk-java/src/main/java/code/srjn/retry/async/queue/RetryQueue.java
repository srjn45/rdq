package code.srjn.retry.async.queue;

import code.srjn.retry.async.RetryTask;

public interface RetryQueue {
    <T> void enqueue(RetryTask<T> task);

    <T> RetryTask<T> dequeue();
}
