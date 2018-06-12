package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;

public class RedisAtomicLong extends Number {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final String id;
    protected final ByteBuffer encodedId;

    public RedisAtomicLong(RedisCommands<ByteBuffer, ByteBuffer> redis, String id) {
        this.redis = redis;
        this.id = id;
        this.encodedId = ByteBuffers.fromUtf8(id);
    }

    public ByteBuffer encodeLong(Long value) {
        return PrimitiveCodec.LONG.encode(value);
    }

    public Long decodeLong(ByteBuffer buffer) {
        return PrimitiveCodec.LONG.decode(buffer);
    }

    public long get() {
        return longValue();
    }

    public void set(long newValue) {
        redis.set(encodedId.duplicate(), encodeLong(newValue));
    }

    public long getAndSet(long newValue) {
        return decodeLong(redis.getset(encodedId.duplicate(), encodeLong(newValue)));
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
        return redis.incr(encodedId.duplicate());
    }

    public long decrementAndGet() {
        return redis.decr(encodedId.duplicate());
    }

    public long addAndGet(long delta) {
        return redis.incrby(encodedId.duplicate(), delta);
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public long longValue() {
        return decodeLong(redis.get(encodedId.duplicate()));
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
        return id;
    }
}
