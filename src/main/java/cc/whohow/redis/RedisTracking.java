package cc.whohow.redis;

import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.RedisConnectionStateListener;

public interface RedisTracking extends AutoCloseable {
    void addListener(RedisConnectionStateListener listener);

    void removeListener(RedisConnectionStateListener listener);

    void addListener(ByteSequence pattern, Listener listener);

    default void addListener(String pattern, Listener listener) {
        addListener(ByteSequence.utf8(pattern), listener);
    }

    void removeListener(ByteSequence pattern, Listener listener);

    default void removeListener(String pattern, Listener listener) {
        removeListener(ByteSequence.utf8(pattern), listener);
    }

    @FunctionalInterface
    interface Listener {
        void onInvalidate(ByteSequence key);
    }
}
