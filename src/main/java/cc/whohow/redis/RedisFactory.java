package cc.whohow.redis;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.DefaultCodecFactory;
import cc.whohow.redis.util.*;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.function.Function;

public class RedisFactory implements
        RedisClockFactory,
        RedisLockFactory,
        RedisAtomicFactory,
        AutoCloseable {
    protected final Redis redis;
    protected final Function<Class<?>, Codec<?>> codecFactory;

    public RedisFactory(Redis redis) {
        this(redis, new DefaultCodecFactory());
    }

    public RedisFactory(Redis redis, Function<Class<?>, Codec<?>> codecFactory) {
        this.redis = redis;
        this.codecFactory = codecFactory;
    }

    @SuppressWarnings("unchecked")
    public <T> Codec<T> newCodec(Class<T> type) {
        return (Codec<T>) codecFactory.apply(type);
    }

    public Clock clock(ZoneId zone) {
        return new RedisClock(redis, zone);
    }

    public RedisLock newLock(String key, Duration maxLockTime) {
        return new RedisLock(redis, key, maxLockTime);
    }

    public RedisLock newLock(String key, Duration minLockTime, Duration maxLockTime) {
        return new RedisLock(redis, key, minLockTime, maxLockTime);
    }

    @Override
    public RedisAtomicLong newAtomicLong(String key) {
        return new RedisAtomicLong(redis, key);
    }

    @Override
    public <T> RedisAtomicReference<T> newAtomicReference(String name, Class<T> type) {
        return new RedisAtomicReference<>(redis, newCodec(type), name);
    }

    @Override
    public <T> RedisAtomicReference<T> newAtomicReference(String name, Class<T> type, Duration ttl) {
        return new RedisAtomicReference.Expire<>(redis, newCodec(type), name, ttl);
    }

    public <T> RedisList<T> newList(String key, Class<T> type) {
        return new RedisList<>(redis, newCodec(type), key);
    }

    public <T> RedisSet<T> newSet(String key, Class<T> type) {
        return new RedisSet<>(redis, newCodec(type), key);
    }

    public <T> RedisSortedSet<T> newSortedSet(String key, Class<T> type) {
        return new RedisSortedSet<>(redis, newCodec(type), key);
    }

    public <K, V> RedisMap<K, V> newMap(String key, Class<K> keyType, Class<V> valueType) {
        return new RedisMap<>(redis, newCodec(keyType), newCodec(valueType), key);
    }

    public <T> RedisPriorityQueue<T> newPriorityQueue(String name, Class<T> type) {
        return new RedisPriorityQueue<>(redis, newCodec(type), name);
    }

    public <T> RedisDelayQueue<T> newDelayQueue(String name, Class<T> type) {
        return new RedisDelayQueue<>(redis, newCodec(type), name);
    }

    public <T> RedisWindowCounter<T> newWindowCounter(String name, Class<T> type) {
        return new RedisWindowCounter<>(redis, newCodec(type), name);
    }

    public <T> RedisTimeWindowCounter newTimeWindowCounter(String name, Duration accuracy) {
        return new RedisTimeWindowCounter(redis, name, accuracy);
    }

    @Override
    public void close() throws Exception {
    }
}
