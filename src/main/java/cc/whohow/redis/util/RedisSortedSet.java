package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.codec.Codec;
import io.lettuce.core.ScoredValue;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * 有序集合
 */
public class RedisSortedSet<E>
        extends AbstractRedisSortedSet<E>
        implements ConcurrentMap<E, Number>, Copyable<Map<E, Number>> {
    public RedisSortedSet(Redis redis, Codec<E> codec, String key) {
        this(redis, codec, ByteSequence.utf8(key));
    }

    public RedisSortedSet(Redis redis, Codec<E> codec, ByteSequence key) {
        super(redis, codec, key);
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
            public Iterator<E> iterator() {
                return new MappingIterator<>(new RedisIterator<>(new RedisSortedSetScanIterator<>(redis, codec::decode, sortedSetKey)), ScoredValue::getValue);
            }

            @Override
            public Object[] toArray() {
                return RedisSortedSet.this.zrange(0, -1)
                        .toArray();
            }

            @Override
            @SuppressWarnings("SuspiciousToArrayCall")
            public <T> T[] toArray(T[] a) {
                return RedisSortedSet.this.zrange(0, -1)
                        .toArray(a);
            }
        };
    }

    @Override
    public Collection<Number> values() {
        return new ConcurrentMapValueCollection<Number>(this) {
            @Override
            public Iterator<Number> iterator() {
                return new MappingIterator<>(new RedisIterator<>(new RedisSortedSetScanIterator<>(redis, codec::decode, sortedSetKey)), ScoredValue::getScore);
            }

            @Override
            public Object[] toArray() {
                return RedisSortedSet.this.zrangeWithScoresAsMap(0, -1)
                        .values()
                        .toArray();
            }

            @Override
            @SuppressWarnings("SuspiciousToArrayCall")
            public <T> T[] toArray(T[] a) {
                return RedisSortedSet.this.zrangeWithScoresAsMap(0, -1)
                        .values()
                        .toArray(a);
            }
        };
    }

    @Override
    public Set<Entry<E, Number>> entrySet() {
        return new ConcurrentMapEntrySet<E, Number>(this) {
            @Override
            public Iterator<Entry<E, Number>> iterator() {
                return new MappingIterator<>(new RedisIterator<>(new RedisSortedSetScanIterator<>(redis, codec::decode, sortedSetKey)), RedisSortedSet.this::toEntry);
            }

            @Override
            public Object[] toArray() {
                return RedisSortedSet.this.zrangeWithScoresAsMap(0, -1)
                        .entrySet()
                        .toArray();
            }

            @Override
            @SuppressWarnings("SuspiciousToArrayCall")
            public <T> T[] toArray(T[] a) {
                return RedisSortedSet.this.zrangeWithScoresAsMap(0, -1)
                        .entrySet()
                        .toArray(a);
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
        return zrangeWithScoresAsMap(0, -1);
    }
}
