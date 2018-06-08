package cc.whohow.redis.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.redisson.client.codec.LongCodec;
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
        redis.execute(RedisCommands.SET, name.retain(), newValue);
    }

    public long getAndSet(long newValue) {
        return redis.execute(LongCodec.INSTANCE, RedisCommands.GETSET, name.retain(), newValue);
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
        return redis.execute(RedisCommands.INCR, name.retain());
    }

    public long decrementAndGet() {
        return redis.execute(RedisCommands.DECR, name.retain());
    }

    public long addAndGet(long delta) {
        return redis.execute(RedisCommands.INCRBY, name.retain(), delta);
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public long longValue() {
        return redis.execute(LongCodec.INSTANCE, RedisCommands.GET, name.retain());
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
        return "RedisAtomicLong{" +
                "redisClient=" + redis +
                ", name=" + name.toString(StandardCharsets.UTF_8) +
                '}';
    }
}
