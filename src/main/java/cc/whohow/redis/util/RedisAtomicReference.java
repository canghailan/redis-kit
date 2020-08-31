package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.script.RedisScriptCommands;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * Redis原子变量
 *
 * @see java.util.concurrent.atomic.AtomicReference
 */
public class RedisAtomicReference<V> {
    private static final Logger log = LogManager.getLogger();

    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<V> codec;
    protected final ByteBuffer key;

    public RedisAtomicReference(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<V> codec, String key) {
        this(redis, codec, ByteBuffers.fromUtf8(key));
    }

    public RedisAtomicReference(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<V> codec, ByteBuffer key) {
        this.redis = redis;
        this.codec = codec;
        this.key = key;
    }

    protected V decode(ByteBuffer buffer) {
        return codec.decode(buffer);
    }

    protected ByteBuffer encode(V value) {
        return codec.encode(value);
    }

    public boolean exists() {
        log.trace("EXISTS {}", this);
        return redis.exists(key.duplicate()) > 0;
    }

    public V get() {
        return get(null);
    }

    public V get(V defaultValue) {
        log.trace("GET {}", this);
        ByteBuffer encodedValue = redis.get(key.duplicate());
        if (encodedValue == null) {
            return defaultValue;
        } else {
            return decode(encodedValue);
        }
    }

    public void set(V newValue) {
        log.trace("SET {} {}", this, newValue);
        redis.set(key.duplicate(), encode(newValue));
    }

    public void setIfAbsent(V newValue) {
        log.trace("SET {} {} NX", this, newValue);
        redis.set(key.duplicate(), encode(newValue), Lettuce.SET_NX);
    }

    public void setIfPresent(V newValue) {
        log.trace("SET {} {} XX", this, newValue);
        redis.set(key.duplicate(), encode(newValue), Lettuce.SET_XX);
    }

    public boolean compareAndSet(V expect, V update) {
        log.trace("EVAL cas {} {} {}", this, expect, update);
        return new RedisScriptCommands(redis).eval("cas", ScriptOutputType.STATUS,
                new ByteBuffer[]{
                        key.duplicate()
                },
                new ByteBuffer[]{
                        encode(expect),
                        encode(update)
                });
    }

    public boolean compareAndReset(V expect) {
        log.trace("EVAL cad {} {}", this, expect);
        return new RedisScriptCommands(redis).eval("cad", ScriptOutputType.STATUS,
                new ByteBuffer[]{
                        key.duplicate()
                },
                new ByteBuffer[]{
                        encode(expect)
                });
    }

    public void reset() {
        log.trace("DEL {}", this);
        redis.del(key.duplicate());
    }

    public V getAndSet(V newValue) {
        log.trace("GETSET {} {}", this, newValue);
        return decode(redis.getset(key.duplicate(), encode(newValue)));
    }

    public final V getAndUpdate(UnaryOperator<V> updateFunction) {
        V prev, next;
        do {
            prev = get();
            next = updateFunction.apply(prev);
        } while (!compareAndSet(prev, next));
        return prev;
    }

    public final V updateAndGet(UnaryOperator<V> updateFunction) {
        V prev, next;
        do {
            prev = get();
            next = updateFunction.apply(prev);
        } while (!compareAndSet(prev, next));
        return next;
    }

    public final V getAndAccumulate(V x, BinaryOperator<V> accumulatorFunction) {
        V prev, next;
        do {
            prev = get();
            next = accumulatorFunction.apply(prev, x);
        } while (!compareAndSet(prev, next));
        return prev;
    }

    public final V accumulateAndGet(V x, BinaryOperator<V> accumulatorFunction) {
        V prev, next;
        do {
            prev = get();
            next = accumulatorFunction.apply(prev, x);
        } while (!compareAndSet(prev, next));
        return next;
    }

    @Override
    public String toString() {
        return ByteBuffers.toString(key);
    }

    public static class Expire<V> extends RedisAtomicReference<V> {
        private static final Logger log = LogManager.getLogger();

        private final long ttl;

        public Expire(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<V> codec, String key, Duration ttl) {
            super(redis, codec, key);
            this.ttl = ttl.toMillis();
        }

        public Expire(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<V> codec, ByteBuffer key, Duration ttl) {
            super(redis, codec, key);
            this.ttl = ttl.toMillis();
        }

        @Override
        public void set(V newValue) {
            log.trace("SET {} {} PX {}", this, newValue, ttl);
            redis.set(key.duplicate(), encode(newValue), SetArgs.Builder.px(ttl));
        }

        @Override
        public void setIfAbsent(V newValue) {
            log.trace("SET {} {} PX {} NX", this, newValue, ttl);
            redis.set(key.duplicate(), encode(newValue), SetArgs.Builder.px(ttl).nx());
        }

        @Override
        public void setIfPresent(V newValue) {
            log.trace("SET {} {} PX {} XX", this, newValue, ttl);
            redis.set(key.duplicate(), encode(newValue), SetArgs.Builder.px(ttl).xx());
        }

        @Override
        public boolean compareAndSet(V expect, V update) {
            log.trace("EVAL cas {} {} {} PX {}", this, expect, update, ttl);
            return new RedisScriptCommands(redis).eval("cas", ScriptOutputType.STATUS,
                    new ByteBuffer[]{
                            key.duplicate()
                    },
                    new ByteBuffer[]{
                            encode(expect),
                            encode(update),
                            Lettuce.px(),
                            PrimitiveCodec.LONG.encode(ttl)
                    });
        }

        @Override
        public V getAndSet(V newValue) {
            log.trace("EVAL getset {} {} PX {}", this, newValue, ttl);
            return new RedisScriptCommands(redis).eval("getset", ScriptOutputType.VALUE,
                    new ByteBuffer[]{
                            key.duplicate()
                    },
                    new ByteBuffer[]{
                            encode(newValue),
                            Lettuce.px(),
                            PrimitiveCodec.LONG.encode(ttl)
                    });
        }
    }
}
