package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.codec.Codec;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 散列表
 */
public class RedisMap<K, V>
        extends AbstractRedisHash<K, V>
        implements ConcurrentMap<K, V>, Copyable<Map<K, V>> {
    public RedisMap(Redis redis, Codec<K> keyCodec, Codec<V> valueCodec, String key) {
        this(redis, keyCodec, valueCodec, ByteSequence.utf8(key));
    }

    public RedisMap(Redis redis, Codec<K> keyCodec, Codec<V> valueCodec, ByteSequence key) {
        super(redis, keyCodec, valueCodec, key);
    }

    @Override
    public int size() {
        return hlen().intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(Object key) {
        return hexists((K) key) > 0;
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
        return hget((K) key);
    }

    /**
     * @return null
     */
    @Override
    public V put(K key, V value) {
        hset(key, value);
        return null;
    }

    /**
     * @return null
     */
    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        hdel((K) key);
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        hmset(m);
    }

    @Override
    public void clear() {
        del();
    }

    @Override
    public Set<K> keySet() {
        return new ConcurrentMapKeySet<K>(this) {
            @Override
            public Object[] toArray() {
                return RedisMap.this.hgetall().keySet().toArray();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                return RedisMap.this.hgetall().keySet().toArray(a);
            }
        };
    }

    @Override
    public Collection<V> values() {
        return new ConcurrentMapValueCollection<V>(this) {
            @Override
            public Object[] toArray() {
                return RedisMap.this.hgetall().values().toArray();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                return RedisMap.this.hgetall().values().toArray(a);
            }
        };
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new ConcurrentMapEntrySet<K, V>(this) {
            @Override
            public Iterator<Entry<K, V>> iterator() {
                return new RedisIterator<>(new RedisHashScanIterator<>(redis, keyCodec::decode, valueCodec::decode, hashKey));
            }

            @Override
            public Object[] toArray() {
                return RedisMap.this.hgetall().entrySet().toArray();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                return RedisMap.this.hgetall().entrySet().toArray(a);
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getOrDefault(Object key, V defaultValue) {
        V value = hget((K) key);
        return value == null ? defaultValue : value;
    }

    /**
     * @return null
     */
    @Override
    public V putIfAbsent(K key, V value) {
        hsetnx(key, value);
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
    public Map<K, V> copy() {
        return hgetall();
    }
}
