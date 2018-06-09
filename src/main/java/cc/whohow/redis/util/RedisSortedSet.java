package cc.whohow.redis.util;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.StringCodec;
import cc.whohow.redis.lettuce.Lettuce;
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
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<E> codec;
    protected final String id;
    protected final ByteBuffer encodedId;

    public RedisSortedSet(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, String id) {
        this.redis = redis;
        this.codec = codec;
        this.id = id;
        this.encodedId = StringCodec.UTF_8.encode(id);
    }

    public ByteBuffer encode(E value) {
        return codec.encode(value);
    }

    public E decode(ByteBuffer buffer) {
        return codec.decode(buffer);
    }

    @Override
    public int size() {
        return redis.zcard(encodedId.duplicate()).intValue();
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
        return redis.zcount(encodedId.duplicate(), Range.create(number, number)) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Number get(Object key) {
        return redis.zscore(encodedId.duplicate(), encode((E) key));
    }

    /**
     * @return null
     */
    @Override
    public Number put(E key, Number value) {
        redis.zadd(encodedId.duplicate(), value.doubleValue(), encode(key));
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Number remove(Object key) {
        redis.zrem(encodedId.duplicate(), encode((E) key));
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void putAll(Map<? extends E, ? extends Number> m) {
        ScoredValue[] encodedScoredValues = m.entrySet().stream()
                .map(e -> ScoredValue.fromNullable(e.getValue().doubleValue(), e.getKey()))
                .toArray(ScoredValue[]::new);
        redis.zadd(encodedId.duplicate(), encodedScoredValues);
    }

    @Override
    public void clear() {
        redis.del(encodedId.duplicate());
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
        redis.zadd(encodedId.duplicate(), Lettuce.Z_ADD_NX, value.doubleValue(), encode(key));
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
        redis.zadd(encodedId.duplicate(), Lettuce.Z_ADD_XX, value.doubleValue(), encode(key));
        return null;
    }

    @Override
    public String toString() {
        return id;
    }
}
