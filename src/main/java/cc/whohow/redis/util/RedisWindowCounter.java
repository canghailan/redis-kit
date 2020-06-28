package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.PrimitiveCodec;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 窗口计数器
 */
public class RedisWindowCounter<W> {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<W> codec;
    protected final ByteBuffer key;

    public RedisWindowCounter(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<W> codec, String key) {
        this(redis, codec, ByteBuffers.fromUtf8(key));
    }

    public RedisWindowCounter(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<W> codec, ByteBuffer key) {
        this.redis = redis;
        this.codec = codec;
        this.key = key;
    }

    protected ByteBuffer encode(W window) {
        return codec.encode(window);
    }

    protected W decode(ByteBuffer byteBuffer) {
        return codec.decode(byteBuffer);
    }

    protected ByteBuffer encodeLong(Long value) {
        return PrimitiveCodec.LONG.encode(value);
    }

    protected Long decodeLong(ByteBuffer byteBuffer) {
        return PrimitiveCodec.LONG.decode(byteBuffer, 0L);
    }

    /**
     * 获取窗口计数值
     */
    public long get(W window) {
        return decodeLong(redis.hget(key.duplicate(), encode(window)));
    }

    /**
     * 获取窗口计数总数
     */
    public long sum(W[] window) {
        return sum(Arrays.stream(window));
    }

    /**
     * 获取窗口计数总数
     */
    public long sum(Iterable<W> window) {
        return sum(StreamSupport.stream(window.spliterator(), false));
    }

    /**
     * 获取窗口计数总数
     */
    public long sum(Stream<W> window) {
        ByteBuffer[] encodedKeys = window
                .map(this::encode)
                .toArray(ByteBuffer[]::new);
        return redis.hmget(key.duplicate(), encodedKeys).stream()
                .map(kv -> kv.getValueOrElse(null))
                .mapToLong(this::decodeLong)
                .sum();
    }

    /**
     * 获取所有窗口计数总数
     */
    public long sum() {
        return redis.hgetall(key.duplicate()).values().stream()
                .mapToLong(this::decodeLong)
                .sum();
    }

    /**
     * 设置计数值
     */
    public void set(W window, long newValue) {
        redis.hset(key.duplicate(), encode(window), encodeLong(newValue));
    }

    public long getAndIncrement(W window) {
        return incrementAndGet(window) - 1;
    }

    public long getAndDecrement(W window) {
        return decrementAndGet(window) + 1;
    }

    public long getAndAdd(W window, long delta) {
        return addAndGet(window, delta) - delta;
    }

    public long incrementAndGet(W window) {
        return addAndGet(window, 1);
    }

    public long decrementAndGet(W window) {
        return addAndGet(window, -1);
    }

    public long addAndGet(W window, long delta) {
        return redis.hincrby(key.duplicate(), encode(window), delta);
    }

    /**
     * 移除窗口计数
     */
    public void removeIf(Predicate<W> predicate) {
        int n = 0;
        ByteBuffer[] batch = new ByteBuffer[100];
        RedisMapIterator iterator = new RedisMapIterator(redis, key.duplicate());
        while (iterator.hasNext()) {
            Map.Entry<ByteBuffer, ByteBuffer> next = iterator.next();
            W window = decode(next.getKey().duplicate());
            if (predicate.test(window)) {
                batch[n++] = next.getKey().duplicate();
            }
            if (n == batch.length) {
                redis.hdel(key.duplicate(), batch);
                n = 0;
            }
        }
        if (n > 0) {
            redis.hdel(key.duplicate(), Arrays.copyOf(batch, n));
        }
    }

    /**
     * 读取所有窗口计数到内存
     */
    public Map<W, Long> get() {
        return redis.hgetall(key.duplicate()).entrySet().stream()
                .collect(Collectors.toMap(
                        e -> decode(e.getKey()),
                        e -> decodeLong(e.getValue()),
                        (a, b) -> b,
                        LinkedHashMap::new));
    }
}
