package cc.whohow.redis.util;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.StringCodec;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.lettuce.RedisCodecAdapter;
import cc.whohow.redis.lettuce.RedisCommandsAdapter;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 键集合
 */
public class RedisKey<V> implements ConcurrentMap<String, V> {
    protected final RedisCommands<String, V> redis;

    public RedisKey(RedisCommands<String, V> redis) {
        this.redis = redis;
    }

    public RedisKey(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<V> codec) {
        this(new RedisCommandsAdapter<>(redis, new RedisCodecAdapter<>(new StringCodec(), codec)));
    }

    @Override
    public int size() {
        return redis.dbsize().intValue();
    }

    @Override
    public boolean isEmpty() {
        return redis.randomkey() != null;
    }

    @Override
    public boolean containsKey(Object key) {
        return redis.exists((String) key) > 0;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        return redis.get((String) key);
    }

    @Override
    public V put(String key, V value) {
        return redis.getset(key, value);
    }

    public void set(String key, V value) {
        redis.set(key, value);
    }

    /**
     * @return null
     */
    @Override
    public V remove(Object key) {
        redis.del((String) key);
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void putAll(Map<? extends String, ? extends V> m) {
        redis.mset((Map<String, V>) m);
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

    /**
     * @return null/value
     */
    @Override
    public V putIfAbsent(String key, V value) {
        if (Lettuce.ok(redis.set(key, value, Lettuce.SET_NX))) {
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
    public boolean replace(String key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return null/value
     */
    @Override
    public V replace(String key, V value) {
        if (Lettuce.ok(redis.set(key, value, Lettuce.SET_XX))) {
            return null;
        }
        return value;
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
