package cc.whohow.redis;

import cc.whohow.redis.util.RedisAtomicLong;
import cc.whohow.redis.util.RedisAtomicReference;

import java.time.Duration;

public interface RedisAtomicFactory {
    RedisAtomicLong newAtomicLong(String name);

    <T> RedisAtomicReference<T> newAtomicReference(String name, Class<T> type);

    <T> RedisAtomicReference<T> newAtomicReference(String name, Class<T> type, Duration ttl);
}
