package cc.whohow.redis;

import cc.whohow.redis.util.RedisAtomicLong;
import cc.whohow.redis.util.RedisAtomicReference;

public interface RedisAtomicFactory {
    RedisAtomicLong newAtomicLong(String name);

    <T> RedisAtomicReference<T> newAtomicReference(String name, Class<T> type);
}
