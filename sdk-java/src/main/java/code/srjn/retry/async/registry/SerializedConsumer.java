package code.srjn.retry.async.registry;

import java.io.Serializable;

@FunctionalInterface
public interface SerializedConsumer<T> extends Serializable {
    void accept(T t);
}
