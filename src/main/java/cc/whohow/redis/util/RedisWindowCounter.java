package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.PrimitiveCodec;

import java.util.*;
import java.util.function.Predicate;

/**
 * 窗口计数器
 */
public class RedisWindowCounter<W>
        extends AbstractRedisHash<W, Long>
        implements Iterable<Map.Entry<W, Long>>, Copyable<Map<W, Long>> {
    public RedisWindowCounter(Redis redis, Codec<W> codec, String key) {
        this(redis, codec, ByteSequence.utf8(key));
    }

    public RedisWindowCounter(Redis redis, Codec<W> codec, ByteSequence key) {
        super(redis, codec, PrimitiveCodec.LONG, key);
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
        return hmget(window).stream()
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
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
        return hincrby(window, delta);
    }

    /**
     * 移除窗口计数
     */
    public void removeIf(Predicate<W> predicate) {
        int batchSize = 100;
        List<W> removeKeys = new ArrayList<>(batchSize);
        for (Map.Entry<W, Long> next : this) {
            if (predicate.test(next.getKey())) {
                removeKeys.add(next.getKey());
            }
            if (removeKeys.size() == batchSize) {
                hdel(removeKeys);
            }
        }
        if (!removeKeys.isEmpty()) {
            hdel(removeKeys);
        }
    }

    public void reset() {
        del();
    }

    @Override
    public Iterator<Map.Entry<W, Long>> iterator() {
        return new RedisIterator<>(new RedisHashScanIterator<>(redis, keyCodec::decode, valueCodec::decode, hashKey));
    }

    /**
     * 读取所有窗口计数到内存
     */
    @Override
    public Map<W, Long> copy() {
        return hgetall();
    }
}
