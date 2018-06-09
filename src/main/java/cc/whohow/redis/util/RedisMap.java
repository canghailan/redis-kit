package cc.whohow.redis.util;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.StringCodec;
import cc.whohow.redis.lettuce.Lettuce;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 散列表
 */
public class RedisMap<K, V> implements ConcurrentMap<K, V> {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<K> keyCodec;
    protected final Codec<V> valueCodec;
    protected final String id;
    protected final ByteBuffer encodedId;

    public RedisMap(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<K> keyCodec, Codec<V> valueCodec, String id) {
        this.redis = redis;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        this.id = id;
        this.encodedId = StringCodec.UTF_8.encode(id);
    }

    public ByteBuffer encodeKey(K key) {
        return keyCodec.encode(key);
    }

    public K decodeKey(ByteBuffer bytes) {
        return keyCodec.decode(bytes);
    }

    public ByteBuffer encodeValue(V value) {
        return valueCodec.encode(value);
    }

    public V decodeValue(ByteBuffer bytes) {
        return valueCodec.decode(bytes);
    }

    @Override
    public int size() {
        return redis.hlen(encodedId.duplicate()).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(Object key) {
        return redis.hexists(encodedId.duplicate(), encodeKey((K) key));
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        return decodeValue(redis.hget(encodedId.duplicate(), encodeKey((K) key)));
    }

    /**
     * @return null
     */
    @Override
    public V put(K key, V value) {
        redis.hset(encodedId.duplicate(), encodeKey(key), encodeValue(value));
        return null;
    }

    /**
     * @return null
     */
    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        redis.hdel(encodedId.duplicate(), encodeKey((K) key));
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Map<ByteBuffer, ByteBuffer> encodedKeyValues = m.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> encodeKey(e.getKey()),
                        e -> encodeValue(e.getValue())));
        redis.hmset(encodedId.duplicate(), encodedKeyValues);
    }

    @Override
    public void clear() {
        redis.del(encodedId.duplicate());
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getOrDefault(Object key, V defaultValue) {
        ByteBuffer encodedValue = redis.get(encodeKey((K) key));
        return Lettuce.isNil(encodedValue) ? defaultValue : decodeValue(encodedValue);
    }

    /**
     * @return null
     */
    @Override
    public V putIfAbsent(K key, V value) {
        redis.hsetnx(encodedId.duplicate(), encodeKey(key), encodeValue(value));
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
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V replace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return id;
    }
}
