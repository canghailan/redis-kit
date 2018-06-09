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
 * 键集合
 */
public class RedisKey<V> implements ConcurrentMap<String, V> {
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<V> codec;

    public RedisKey(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<V> codec) {
        this.redis = redis;
        this.codec = codec;
    }

    public ByteBuffer encodeKey(String key) {
        return StringCodec.UTF_8.encode(key);
    }

    public String decodeKey(ByteBuffer buffer) {
        return StringCodec.UTF_8.decode(buffer);
    }

    public ByteBuffer encodeValue(V value) {
        return codec.encode(value);
    }

    public V decodeValue(ByteBuffer buffer) {
        return codec.decode(buffer);
    }

    @Override
    public int size() {
        return redis.dbsize().intValue();
    }

    @Override
    public boolean isEmpty() {
        return !Lettuce.isNil(redis.randomkey());
    }

    @Override
    public boolean containsKey(Object key) {
        return redis.exists(encodeKey((String) key)) > 0;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        return decodeValue(redis.get(encodeKey((String) key)));
    }

    @Override
    public V put(String key, V value) {
        return decodeValue(redis.getset(encodeKey(key), encodeValue(value)));
    }

    public void set(String key, V value) {
        redis.set(encodeKey(key), encodeValue(value));
    }

    /**
     * @return null
     */
    @Override
    public V remove(Object key) {
        redis.del(encodeKey((String) key));
        return null;
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        Map<ByteBuffer, ByteBuffer> encodedKeyValues = m.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> encodeKey(e.getKey()),
                        e -> encodeValue(e.getValue())));
        redis.mset(encodedKeyValues);
    }

    @Override
    public void clear() {
        redis.flushdb();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public Set<String> keySet() {
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
    public Set<Entry<String, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        ByteBuffer encodedValue = redis.get(encodeKey((String) key));
        return Lettuce.isNil(encodedValue) ? defaultValue : decodeValue(encodedValue);
    }

    /**
     * @return null
     */
    @Override
    public V putIfAbsent(String key, V value) {
        redis.set(encodeKey(key), encodeValue(value), Lettuce.SET_NX);
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
    public boolean replace(String key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return null
     */
    @Override
    public V replace(String key, V value) {
        redis.set(encodeKey(key), encodeValue(value), Lettuce.SET_XX);
        return null;
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public void replaceAll(BiFunction<? super String, ? super V, ? extends V> function) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V computeIfAbsent(String key, Function<? super String, ? extends V> mappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V computeIfPresent(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V compute(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException UnsupportedOperation
     */
    @Override
    public V merge(String key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }
}
