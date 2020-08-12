package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.util.impl.MappingIterator;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Predicate;

/**
 * 窗口计数器
 */
public class RedisWindowCounter<W>
        extends AbstractRedisHash<W, Long>
        implements Iterable<Map.Entry<W, Long>>, Copyable<Map<W, Long>> {
    private static final Logger log = LogManager.getLogger();

    public RedisWindowCounter(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<W> codec, String key) {
        this(redis, codec, ByteBuffers.fromUtf8(key));
    }

    public RedisWindowCounter(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<W> codec, ByteBuffer key) {
        super(redis, codec, PrimitiveCodec.LONG, key);
    }

    protected Map.Entry<W, Long> decode(Map.Entry<ByteBuffer, ByteBuffer> entry) {
        return new AbstractMap.SimpleImmutableEntry<>(decodeKey(entry.getKey()), decodeValue(entry.getValue()));
    }

    /**
     * 获取窗口计数值
     */
    public long get(W window) {
        return hget(window);
    }

    /**
     * 获取窗口计数总数
     */
    public long sum(W[] window) {
        return sum(Arrays.asList(window));
    }

    /**
     * 获取窗口计数总数
     */
    public long sum(Collection<? extends W> window) {
        return hmget(window)
                .mapToLong(kv -> kv.getValueOrElse(0L))
                .sum();
    }

    /**
     * 获取所有窗口计数总数
     */
    public long sum() {
        return hgetall().values().stream()
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .sum();
    }

    /**
     * 设置计数值
     */
    public void set(W window, long newValue) {
        hset(window, newValue);
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
        log.trace("HINCRBY {} {} {}", this, window, delta);
        return redis.hincrby(hashKey.duplicate(), encodeKey(window), delta);
    }

    /**
     * 移除窗口计数
     */
    public void removeIf(Predicate<W> predicate) {
        int n = 0;
        ByteBuffer[] batch = new ByteBuffer[100];
        RedisMapIterator iterator = new RedisMapIterator(redis, hashKey.duplicate());
        while (iterator.hasNext()) {
            Map.Entry<ByteBuffer, ByteBuffer> next = iterator.next();
            W window = decodeKey(next.getKey().duplicate());
            if (predicate.test(window)) {
                log.trace("HDEL {} {}", this, window);
                batch[n++] = next.getKey().duplicate();
            }
            if (n == batch.length) {
                redis.hdel(hashKey.duplicate(), batch);
                n = 0;
            }
        }
        if (n > 0) {
            redis.hdel(hashKey.duplicate(), Arrays.copyOf(batch, n));
        }
    }

    public void reset() {
        del();
    }

    @Override
    public Iterator<Map.Entry<W, Long>> iterator() {
        return new MappingIterator<>(new RedisMapIterator(redis, hashKey.duplicate()), this::decode);
    }

    /**
     * 读取所有窗口计数到内存
     */
    @Override
    public Map<W, Long> copy() {
        return hgetall();
    }
}
