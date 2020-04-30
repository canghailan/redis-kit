package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.util.impl.ConcurrentMapEntrySet;
import cc.whohow.redis.util.impl.ConcurrentMapKeySet;
import cc.whohow.redis.util.impl.ConcurrentMapValueCollection;
import cc.whohow.redis.util.impl.MappingIterator;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 有序集合，按指定值排序
 */
public class RedisSortedSet<E> implements ConcurrentMap<E, Number>, Supplier<Map<E, Number>> {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<E> codec;
    protected final ByteBuffer key;

    public RedisSortedSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String key) {
        this(redis, codec, ByteBuffers.fromUtf8(key));
    }

    public RedisSortedSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, ByteBuffer key) {
        this.redis = redis;
        this.codec = codec;
        this.key = key;
    }

    protected ByteBuffer encode(E value) {
        return codec.encode(value);
    }

    protected E decode(ByteBuffer byteBuffer) {
        return codec.decode(byteBuffer);
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
        return redis.zscore(this.key.duplicate(), encode((E) key));
    }

    /**
     * @return null/value
     */
    @Override
    public Number put(E key, Number value) {
        if (redis.zadd(this.key.duplicate(), value.doubleValue(), encode(key)) > 0) {
            return null;
        }
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Number remove(Object key) {
        redis.zrem(this.key.duplicate(), encode((E) key));
        return null;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void putAll(Map<? extends E, ? extends Number> m) {
        ScoredValue[] encodedScoredValues = m.entrySet().stream()
                .map(e -> ScoredValue.fromNullable(e.getValue().doubleValue(), encode(e.getKey())))
                .toArray(ScoredValue[]::new);
        redis.zadd(key.duplicate(), encodedScoredValues);
    }

    @Override
    public void clear() {
        redis.del(key.duplicate());
    }

    @Override
    public Set<E> keySet() {
        return new ConcurrentMapKeySet<E>(this) {
            @Override
            public Object[] toArray() {
                return get().keySet().toArray();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                return get().keySet().toArray(a);
            }
        };
    }

    @Override
    public Collection<Number> values() {
        return new ConcurrentMapValueCollection<Number>(this) {
            @Override
            public Object[] toArray() {
                return get().values().toArray();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                return get().values().toArray(a);
            }
        };
    }

    @Override
    public Set<Entry<E, Number>> entrySet() {
        return new ConcurrentMapEntrySet<E, Number>(this) {
            @Override
            public Iterator<Entry<E, Number>> iterator() {
                return new MappingIterator<>(
                        new RedisSortedSetIterator(redis, key.duplicate()),
                        (value) -> new AbstractMap.SimpleImmutableEntry<>(decode(value.getValue()), value.getScore()));
            }

            @Override
            public Object[] toArray() {
                return get().entrySet().toArray();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                return get().entrySet().toArray(a);
            }
        };
    }

    @Override
    public Number getOrDefault(Object key, Number defaultValue) {
        Number value = get(key);
        return value == null ? defaultValue : value;
    }

    @Override
    public Number putIfAbsent(E key, Number value) {
        redis.zadd(this.key.duplicate(), Lettuce.Z_ADD_NX, value.doubleValue(), encode(key));
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
        redis.zadd(this.key.duplicate(), Lettuce.Z_ADD_XX, value.doubleValue(), encode(key));
        return null;
    }

    @Override
    public Map<E, Number> get() {
        return redis.zrangeWithScores(key.duplicate(), 0, -1).stream()
                .collect(Collectors.toMap(
                        e -> decode(e.getValue()),
                        ScoredValue::getScore,
                        (a, b) -> b,
                        LinkedHashMap::new));
    }
}
