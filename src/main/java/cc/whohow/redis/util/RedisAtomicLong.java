package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;

import java.nio.charset.StandardCharsets;

public class RedisAtomicLong extends Number {
    protected final Redis redis;
    protected final ByteBuf name;

    public RedisAtomicLong(Redis redis, String name) {
        this.redis = redis;
        this.name = Unpooled.copiedBuffer(name, StandardCharsets.UTF_8).asReadOnly();
    }

    public long get() {
        return longValue();
    }

    public void set(long newValue) {
        redis.execute(RedisCommands.SET, name, newValue);
    }

    public long getAndSet(long newValue) {
        return redis.execute(LongCodec.INSTANCE, RedisCommands.GETSET, name, newValue);
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
        return redis.execute(RedisCommands.INCR, name);
    }

    public long decrementAndGet() {
        return redis.execute(RedisCommands.DECR, name);
    }

    public long addAndGet(long delta) {
        return redis.execute(RedisCommands.INCRBY, name, delta);
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public long longValue() {
        return redis.execute(LongCodec.INSTANCE, RedisCommands.GET, name);
    }

    @Override
    public float floatValue() {
        return (float) longValue();
    }

    @Override
    public double doubleValue() {
        return (double) longValue();
    }

    public String toString() {
        return redis.execute(StringCodec.INSTANCE, RedisCommands.GET, name);
    }
}
