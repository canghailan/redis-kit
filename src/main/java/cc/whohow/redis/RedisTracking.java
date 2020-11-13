package cc.whohow.redis;

import cc.whohow.redis.buffer.ByteSequence;

public interface RedisTracking extends AutoCloseable {
    void addListener(ByteSequence pattern, Listener listener);

    void removeListener(ByteSequence pattern, Listener listener);

    @FunctionalInterface
    interface Listener {
        void onInvalidate(ByteSequence key);
    }
}
