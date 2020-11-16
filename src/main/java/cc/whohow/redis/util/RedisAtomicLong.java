package cc.whohow.redis.util;

import cc.whohow.redis.RESP;
import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.lettuce.IntegerOutput;
import cc.whohow.redis.lettuce.VoidOutput;
import io.lettuce.core.protocol.CommandType;


/**
 * Redis计数器
 *
 * @see java.util.concurrent.atomic.AtomicLong
 */
public class RedisAtomicLong extends Number {
    protected final Redis redis;
    protected final ByteSequence key;

    public RedisAtomicLong(Redis redis, String key) {
        this(redis, ByteSequence.utf8(key));
    }

    public RedisAtomicLong(Redis redis, ByteSequence key) {
        this(redis, key, 0);
    }

    public RedisAtomicLong(Redis redis, String key, long initialValue) {
        this(redis, ByteSequence.utf8(key), initialValue);
    }

    public RedisAtomicLong(Redis redis, ByteSequence key, long initialValue) {
        this.redis = redis;
        this.key = key;

        if (initialValue != 0) {
            redis.send(new VoidOutput(), CommandType.SET, key, RESP.b(initialValue), RESP.nx());
        }
    }

    public long get() {
        return longValue();
    }

    public void set(long newValue) {
        redis.send(new VoidOutput(), CommandType.SET, key, RESP.b(newValue));
    }

    public long getAndSet(long newValue) {
        return redis.send(new IntegerOutput(0L), CommandType.GETSET, key, RESP.b(newValue));
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
        return redis.send(new IntegerOutput(), CommandType.INCR, key);
    }

    public long decrementAndGet() {
        return redis.send(new IntegerOutput(), CommandType.DECR, key);
    }

    public long addAndGet(long delta) {
        return redis.send(new IntegerOutput(), CommandType.INCRBY, key, RESP.b(delta));
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public long longValue() {
        return redis.send(new IntegerOutput(0L), CommandType.GET, key);
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
        return key.toString();
    }
}
