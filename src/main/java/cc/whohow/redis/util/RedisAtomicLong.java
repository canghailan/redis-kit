package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.script.RedisScriptCommands;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;

/**
 * Redis计数器
 *
 * @see java.util.concurrent.atomic.AtomicLong
 */
public class RedisAtomicLong extends Number {
    private static final Logger log = LogManager.getLogger();
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final ByteBuffer key;

    public RedisAtomicLong(RedisCommands<ByteBuffer, ByteBuffer> redis, String key) {
        this(redis, ByteBuffers.fromUtf8(key));
    }

    public RedisAtomicLong(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key) {
        this(redis, key, 0);
    }

    public RedisAtomicLong(RedisCommands<ByteBuffer, ByteBuffer> redis, String key, long initialValue) {
        this(redis, ByteBuffers.fromUtf8(key), initialValue);
    }

    public RedisAtomicLong(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, long initialValue) {
        this.redis = redis;
        this.key = key;

        if (initialValue != 0) {
            log.trace("SET {} {} NX", this, initialValue);
            redis.set(key.duplicate(), encode(initialValue), Lettuce.SET_NX);
        }
    }

    protected ByteBuffer encode(long value) {
        return PrimitiveCodec.LONG.encode(value);
    }

    protected long decode(ByteBuffer byteBuffer) {
        return PrimitiveCodec.LONG.decode(byteBuffer, 0L);
    }

    public long get() {
        return longValue();
    }

    public void set(long newValue) {
        log.trace("set {} {}", this, newValue);
        redis.set(key.duplicate(), encode(newValue));
    }

    public long getAndSet(long newValue) {
        log.trace("getset {} {}", this, newValue);
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
        log.trace("incr {}", this);
        return redis.incr(key.duplicate());
    }

    public long decrementAndGet() {
        log.trace("decr {}", this);
        return redis.decr(key.duplicate());
    }

    public long addAndGet(long delta) {
        log.trace("incrby {} {}", this, delta);
        return redis.incrby(key.duplicate(), delta);
    }

    public long getAndAccumulate(long value, String operator) {
        if ("+".equals(operator)) {
            return getAndAdd(value);
        }
        if ("/".equals(operator)) {
            operator = "//";
        }
        log.trace("eval acc {} {} {}", this, operator, value);
        ByteBuffer r = new RedisScriptCommands(redis).eval("acc", ScriptOutputType.VALUE,
                new ByteBuffer[]{
                        key.duplicate()
                },
                new ByteBuffer[]{
                        ByteBuffers.fromUtf8(operator),
                        PrimitiveCodec.LONG.encode(value)
                });
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
        log.trace("get {}", this);
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

    @Override
    public String toString() {
        return ByteBuffers.toString(key);
    }
}
