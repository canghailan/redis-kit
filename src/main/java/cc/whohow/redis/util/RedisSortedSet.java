package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.lettuce.RedisCommandsAdapter;
import cc.whohow.redis.lettuce.RedisValueCodecAdapter;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * 有序集合，按指定值排序
 */
public class RedisSortedSet<E> implements ConcurrentMap<E, Number> {
    protected final RedisCommands<ByteBuffer, E> redis;
    protected final ByteBuffer key;

    public RedisSortedSet(RedisCommands<ByteBuffer, E> redis, ByteBuffer key) {
        this.redis = redis;
        this.key = key;
    }

    public RedisSortedSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(new RedisCommandsAdapter<>(redis, new RedisValueCodecAdapter<>(codec)), ByteBuffers.fromUtf8(key));
    }

    @Override
    public int size() {
        return redis.zcard(key.duplicate()).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        Number number = (Number) value;
        return redis.zcount(key.duplicate(), Range.create(number, number)) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Number get(Object key) {
        return redis.zscore(this.key.duplicate(), (E) key);
    }

    /**
     * @return null/value
     */
    @Override
    public Number put(E key, Number value) {
        if (redis.zadd(this.key.duplicate(), value.doubleValue(), key) > 0) {
            return null;
        }
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Number remove(Object key) {
        redis.zrem(this.key.duplicate(), (E) key);
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void putAll(Map<? extends E, ? extends Number> m) {
        ScoredValue[] encodedScoredValues = m.entrySet().stream()
                .map(e -> ScoredValue.fromNullable(e.getValue().doubleValue(), e.getKey()))
                .toArray(ScoredValue[]::new);
        redis.zadd(key.duplicate(), encodedScoredValues);
    }

    @Override
    public void clear() {
        redis.del(key.duplicate());
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Set<E> keySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Collection<Number> values() {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Set<Entry<E, Number>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Number getOrDefault(Object key, Number defaultValue) {
        Number value = get(key);
        return value == null ? defaultValue : value;
    }

    @Override
    public Number putIfAbsent(E key, Number value) {
        redis.zadd(this.key.duplicate(), Lettuce.Z_ADD_NX, value.doubleValue(), key);
        return null;
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public boolean replace(E key, Number oldValue, Number newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Number replace(E key, Number value) {
        redis.zadd(this.key.duplicate(), Lettuce.Z_ADD_XX, value.doubleValue(), key);
        return null;
    }
}
