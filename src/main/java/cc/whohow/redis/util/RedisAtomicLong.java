package cc.whohow.redis.util;

import cc.whohow.redis.io.PrimitiveCodec;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;

/**
 * Reids计数器
 */
public class RedisAtomicLong extends Number {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final ByteBuffer key;

    public RedisAtomicLong(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key) {
        this(redis, key, 0);
    }

    public RedisAtomicLong(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, long initialValue) {
        this.redis = redis;
        this.key = key;

        redis.setnx(key.duplicate(), encode(initialValue));
    }

    protected ByteBuffer encode(long value) {
        return PrimitiveCodec.LONG.encode(value);
    }

    protected long decode(ByteBuffer byteBuffer) {
        return PrimitiveCodec.LONG.decode(byteBuffer);
    }

    public long get() {
        return longValue();
    }

    public void set(long newValue) {
        redis.set(key.duplicate(), encode(newValue));
    }

    public long getAndSet(long newValue) {
        return decode(redis.getset(key.duplicate(), encode(newValue)));
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
        return redis.incr(key.duplicate());
    }

    public long decrementAndGet() {
        return redis.decr(key.duplicate());
    }

    public long addAndGet(long delta) {
        return redis.incrby(key.duplicate(), delta);
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public long longValue() {
        return decode(redis.get(key.duplicate()));
    }

    @Override
    public float floatValue() {
        return (float) longValue();
    }

    @Override
    public double doubleValue() {
        return (double) longValue();
    }
}
