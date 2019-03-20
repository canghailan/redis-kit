package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
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
    protected final ByteBuffer hashKey;

    public RedisMap(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<K> keyCodec, Codec<V> valueCodec, String key) {
        this(redis, keyCodec, valueCodec, ByteBuffers.fromUtf8(key));
    }

    public RedisMap(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<K> keyCodec, Codec<V> valueCodec, ByteBuffer key) {
        this.redis = redis;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        this.hashKey = key;
    }

    public ByteBuffer encodeKey(K key) {
        return keyCodec.encode(key);
    }

    public K decodeKey(ByteBuffer buffer) {
        return keyCodec.decode(buffer);
    }

    public ByteBuffer encodeValue(V value) {
        return valueCodec.encode(value);
    }

    public V decodeValue(ByteBuffer buffer) {
        return valueCodec.decode(buffer);
    }

    @Override
    public int size() {
        return redis.hlen(hashKey.duplicate()).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(Object key) {
        return redis.hexists(hashKey.duplicate(), encodeKey((K) key));
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
        return decodeValue(redis.hget(hashKey.duplicate(), encodeKey((K) key)));
    }

    /**
     * @return null
     */
    @Override
    public V put(K key, V value) {
        redis.hset(hashKey.duplicate(), encodeKey(key), encodeValue(value));
        return null;
    }

    /**
     * @return null
     */
    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        redis.hdel(hashKey.duplicate(), encodeKey((K) key));
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Map<ByteBuffer, ByteBuffer> encodedKeyValues = m.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> encodeKey(e.getKey()),
                        e -> encodeValue(e.getValue())));
        redis.hmset(hashKey.duplicate(), encodedKeyValues);
    }

    @Override
    public void clear() {
        redis.del(hashKey.duplicate());
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
        return ByteBuffers.isEmpty(encodedValue) ? defaultValue : decodeValue(encodedValue);
    }

    /**
     * @return null/value
     */
    @Override
    public V putIfAbsent(K key, V value) {
        if (redis.hsetnx(hashKey.duplicate(), encodeKey(key), encodeValue(value))) {
            return null;
        }
        return value;
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
}
