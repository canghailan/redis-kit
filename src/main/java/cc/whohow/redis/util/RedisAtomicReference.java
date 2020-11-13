package cc.whohow.redis.util;

import cc.whohow.redis.RESP;
import cc.whohow.redis.Redis;
import cc.whohow.redis.RedisScript;
import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.lettuce.DecodeOutput;
import cc.whohow.redis.lettuce.IntegerOutput;
import cc.whohow.redis.lettuce.StatusOutput;
import cc.whohow.redis.lettuce.VoidOutput;
import io.lettuce.core.protocol.CommandType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * Redis原子变量
 *
 * @see java.util.concurrent.atomic.AtomicReference
 */
public class RedisAtomicReference<V> {
    protected final Redis redis;
    protected final Codec<V> codec;
    protected final ByteSequence key;

    public RedisAtomicReference(Redis redis, Codec<V> codec, String key) {
        this(redis, codec, ByteSequence.utf8(key));
    }

    public RedisAtomicReference(Redis redis, Codec<V> codec, ByteSequence key) {
        this.redis = redis;
        this.codec = codec;
        this.key = key;
    }

    protected V decode(ByteBuffer buffer) {
        return codec.decode(buffer);
    }

    protected ByteSequence encode(V value) {
        return codec.encode(value);
    }

    public boolean exists() {
        return redis.send(new IntegerOutput(), CommandType.EXISTS, key) > 0;
    }

    public V get() {
        return get(null);
    }

    public V get(V defaultValue) {
        return redis.send(new DecodeOutput<>(this::decode, defaultValue), CommandType.GET, key);
    }

    public void set(V newValue) {
        redis.send(new VoidOutput(), CommandType.SET, key, encode(newValue));
    }

    public boolean setIfAbsent(V newValue) {
        return RESP.ok(redis.send(new StatusOutput(), CommandType.SET, key, encode(newValue), RESP.nx()));
    }

    public boolean setIfPresent(V newValue) {
        return RESP.ok(redis.send(new StatusOutput(), CommandType.SET, key, encode(newValue), RESP.xx()));
    }

    public boolean compareAndSet(V expect, V update) {
        return RESP.ok(redis.eval(new StatusOutput(),
                RedisScript.get("cas"),
                Collections.singletonList(key),
                Arrays.asList(encode(expect), encode(update))));
    }

    public boolean compareAndReset(V expect) {
        return RESP.ok(redis.eval(new StatusOutput(),
                RedisScript.get("cad"),
                Collections.singletonList(key),
                Collections.singletonList(encode(expect))));
    }

    public void reset() {
        redis.send(new IntegerOutput(), CommandType.DEL, key);
    }

    public V getAndSet(V newValue) {
        return redis.send(new DecodeOutput<>(this::decode), CommandType.GETSET, key, encode(newValue));
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
        return key.toString();
    }

    public static class Expire<V> extends RedisAtomicReference<V> {
        private static final Logger log = LogManager.getLogger();

        private final long ttl;

        public Expire(Redis redis, Codec<V> codec, String key, Duration ttl) {
            super(redis, codec, key);
            this.ttl = ttl.toMillis();
        }

        public Expire(Redis redis, Codec<V> codec, ByteSequence key, Duration ttl) {
            super(redis, codec, key);
            this.ttl = ttl.toMillis();
        }

        @Override
        public void set(V newValue) {
            redis.send(new VoidOutput(), CommandType.SET, key, encode(newValue), RESP.px(), RESP.b(ttl));
        }

        @Override
        public boolean setIfAbsent(V newValue) {
            return RESP.ok(redis.send(new StatusOutput(), CommandType.SET, key, encode(newValue), RESP.px(), RESP.b(ttl), RESP.nx()));
        }

        @Override
        public boolean setIfPresent(V newValue) {
            return RESP.ok(redis.send(new StatusOutput(), CommandType.SET, key, encode(newValue), RESP.px(), RESP.b(ttl), RESP.xx()));
        }

        @Override
        public boolean compareAndSet(V expect, V update) {
            return RESP.ok(redis.eval(new StatusOutput(),
                    RedisScript.get("cas"),
                    Collections.singletonList(key),
                    Arrays.asList(encode(expect), encode(update), RESP.px(), RESP.b(ttl))));
        }

        @Override
        public V getAndSet(V newValue) {
            return redis.eval(new DecodeOutput<>(this::decode),
                    RedisScript.get("getset"),
                    Collections.singletonList(key),
                    Arrays.asList(encode(newValue), RESP.px(), RESP.b(ttl)));
        }
    }
}
