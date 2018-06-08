package cc.whohow.redis.util;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;

public class RedisAtomicLong extends Number {
    protected static final RedisCodec<String, Long> codec = null;
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final String key;

    public RedisAtomicLong(RedisCommands<ByteBuffer, ByteBuffer> redis, String key) {
        this.redis = redis;
        this.key = key;
    }

    public long get() {
        return longValue();
    }

    public void set(long newValue) {
        redis.set(key, newValue);
    }

    public long getAndSet(long newValue) {
        return redis.getset(key, newValue);
    }

    public long getAndIncrement() {
        return incrementAndGet() - 1;
    }

    public long getAndDecrement() {
        return decrementAndGet() + 1;
    }

    public long getAndAdd(long delta) {
        return addAndGet(delta) - delta;
    }

    public long incrementAndGet() {
        return redis.incr(key);
    }

    public long decrementAndGet() {
        return redis.decr(key);
    }

    public long addAndGet(long delta) {
        return redis.incrby(key, delta);
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public long longValue() {
        return redis.get(key);
    }

    @Override
    public float floatValue() {
        return (float) longValue();
    }

    @Override
    public double doubleValue() {
        return (double) longValue();
    }

    @Override
    public String toString() {
        return key;
    }
}
