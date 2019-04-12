package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.script.RedisScriptCommands;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;

/**
 * Reids计数器
 */
public class RedisAtomicLong extends Number {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final RedisScriptCommands redisScriptCommands;
    protected final ByteBuffer key;

    public RedisAtomicLong(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key) {
        this(redis, key, 0);
    }

    public RedisAtomicLong(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, long initialValue) {
        this.redis = redis;
        this.redisScriptCommands = new RedisScriptCommands(redis);
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

    public long getAndAccumulate(long value, String operator) {
        if ("+".equals(operator)) {
            return getAndAdd(value);
        }
        if ("/".equals(operator)) {
            operator = "//";
        }
        ByteBuffer r = redisScriptCommands.eval("acc", ScriptOutputType.VALUE,
                new ByteBuffer[]{key.duplicate()},
                new ByteBuffer[]{ByteBuffers.fromUtf8(operator), PrimitiveCodec.LONG.encode(value)});
        if (r == null) {
            return 0;
        }
        return PrimitiveCodec.LONG.decode(r);
    }

    public long accumulateAndGet(long value, String operator) {
        long r = getAndAccumulate(value, operator);
        switch (operator) {
            case "+": {
                return r + value;
            }
            case "-": {
                return r - value;
            }
            case "*": {
                return r * value;
            }
            case "/":
            case "//": {
                return r / value;
            }
            case "%": {
                return r % value;
            }
            case "^": {
                while (value-- > 0) {
                    r *= r;
                }
                return r;
            }
            default: {
                return r;
            }
        }
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
