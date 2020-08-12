package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.util.impl.*;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * 有序集合，按指定值排序
 */
public class RedisSortedSet<E>
        extends AbstractRedisSortedSet<E>
        implements ConcurrentMap<E, Number>, Copyable<Map<E, Number>> {
    public RedisSortedSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(redis, codec, ByteBuffers.fromUtf8(key));
    }

    public RedisSortedSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, ByteBuffer key) {
        super(redis, codec, key);
    }

    public ScoredValue<ByteBuffer> encode(Map.Entry<E, Number> entry) {
        return ScoredValue.fromNullable(entry.getValue().doubleValue(), encode(entry.getKey()));
    }

    protected Map.Entry<E, Number> decode(ScoredValue<ByteBuffer> scoredValue) {
        return new AbstractMap.SimpleImmutableEntry<>(decode(scoredValue.getValue()), scoredValue.getScore());
    }

    protected Map.Entry<E, Number> toEntry(ScoredValue<E> scoredValue) {
        return new AbstractMap.SimpleImmutableEntry<>(scoredValue.getValue(), scoredValue.getScore());
    }

    @Override
    public int size() {
        return zcard().intValue();
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
        return zcount((Number) value, (Number) value) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Number get(Object key) {
        return zscore((E) key);
    }

    /**
     * @return null
     * @see AbstractRedisSortedSet#zadd(java.lang.Number, java.lang.Object)
     */
    @Override
    public Number put(E key, Number value) {
        zadd(value, key);
        return null;
    }

    /**
     * @return null
     * @see AbstractRedisSortedSet#zrem(java.lang.Object)
     */
    @Override
    @SuppressWarnings("unchecked")
    public Number remove(Object key) {
        zrem((E) key);
        return null;
    }

    @Override
    public void putAll(Map<? extends E, ? extends Number> m) {
        zadd(m);
    }

    @Override
    public void clear() {
        del();
    }

    @Override
    public Set<E> keySet() {
        return new ConcurrentMapKeySet<E>(this) {
            @Override
            public Object[] toArray() {
                return RedisSortedSet.this.zrange(0, -1)
                        .toArray();
            }

            @Override
            @SuppressWarnings("SuspiciousToArrayCall")
            public <T> T[] toArray(T[] a) {
                return RedisSortedSet.this.zrange(0, -1)
                        .toArray(ArrayType.of(a)::newInstance);
            }
        };
    }

    @Override
    public Collection<Number> values() {
        return new ConcurrentMapValueCollection<Number>(this) {
            @Override
            public Object[] toArray() {
                return RedisSortedSet.this.zrangeWithScores(0, -1)
                        .map(ScoredValue::getScore)
                        .toArray();
            }

            @Override
            @SuppressWarnings("SuspiciousToArrayCall")
            public <T> T[] toArray(T[] a) {
                return RedisSortedSet.this.zrangeWithScores(0, -1)
                        .map(ScoredValue::getScore)
                        .toArray(ArrayType.of(a)::newInstance);
            }
        };
    }

    @Override
    public Set<Entry<E, Number>> entrySet() {
        return new ConcurrentMapEntrySet<E, Number>(this) {
            @Override
            public Iterator<Entry<E, Number>> iterator() {
                return new MappingIterator<>(
                        new RedisSortedSetIterator(redis, sortedSetKey.duplicate()), RedisSortedSet.this::decode);
            }

            @Override
            public Object[] toArray() {
                return RedisSortedSet.this.zrangeWithScores(0, -1)
                        .map(RedisSortedSet.this::toEntry)
                        .toArray();
            }

            @Override
            @SuppressWarnings("SuspiciousToArrayCall")
            public <T> T[] toArray(T[] a) {
                return RedisSortedSet.this.zrangeWithScores(0, -1)
                        .map(RedisSortedSet.this::toEntry)
                        .toArray(ArrayType.of(a)::newInstance);
            }
        };
    }

    @Override
    public Number getOrDefault(Object key, Number defaultValue) {
        Number value = get(key);
        return value == null ? defaultValue : value;
    }

    /**
     * @return null
     * @see AbstractRedisSortedSet#zaddnx(java.lang.Number, java.lang.Object)
     */
    @Override
    public Number putIfAbsent(E key, Number value) {
        zaddnx(value, key);
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

    /**
     * @return null
     * @see AbstractRedisSortedSet#zaddxx(java.lang.Number, java.lang.Object)
     */
    @Override
    public Number replace(E key, Number value) {
        zaddxx(value, key);
        return null;
    }

    @Override
    public Map<E, Number> copy() {
        return zrangeWithScores(0, -1)
                .collect(Collectors.toMap(
                        ScoredValue::getValue,
                        ScoredValue::getScore,
                        (a, b) -> b,
                        LinkedHashMap::new));
    }
}
