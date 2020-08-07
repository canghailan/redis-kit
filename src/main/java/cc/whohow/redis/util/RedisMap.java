package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.util.impl.ConcurrentMapEntrySet;
import cc.whohow.redis.util.impl.ConcurrentMapKeySet;
import cc.whohow.redis.util.impl.ConcurrentMapValueCollection;
import cc.whohow.redis.util.impl.MappingIterator;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 散列表
 */
public class RedisMap<K, V> implements ConcurrentMap<K, V>, Supplier<Map<K, V>> {
    private static final Logger log = LogManager.getLogger();
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

    public Map.Entry<K, V> decode(Map.Entry<ByteBuffer, ByteBuffer> buffer) {
        return new AbstractMap.SimpleImmutableEntry<>(decodeKey(buffer.getKey()), decodeValue(buffer.getValue()));
    }

    @Override
    public int size() {
        if (log.isTraceEnabled()) {
            log.trace("HLEN {}", toString());
        }
        return redis.hlen(hashKey.duplicate()).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(Object key) {
        if (log.isTraceEnabled()) {
            log.trace("HEXISTS {} {}", toString(), key);
        }
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
        if (log.isTraceEnabled()) {
            log.trace("HGET {} {}", toString(), key);
        }
        return decodeValue(redis.hget(hashKey.duplicate(), encodeKey((K) key)));
    }

    /**
     * @return null
     */
    @Override
    public V put(K key, V value) {
        if (log.isTraceEnabled()) {
            log.trace("HSET {} {} {}", toString(), key, value);
        }
        redis.hset(hashKey.duplicate(), encodeKey(key), encodeValue(value));
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

    public long hdel(K key) {
        if (log.isTraceEnabled()) {
            log.trace("HDEL {} {}", toString(), key);
        }
        return redis.hdel(hashKey.duplicate(), encodeKey(key));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        if (log.isTraceEnabled()) {
            log.trace("HMSET {} {}", toString(), m);
        }
        Map<ByteBuffer, ByteBuffer> encodedKeyValues = m.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> encodeKey(e.getKey()),
                        e -> encodeValue(e.getValue())));
        redis.hmset(hashKey.duplicate(), encodedKeyValues);
    }

    @Override
    public void clear() {
        if (log.isTraceEnabled()) {
            log.trace("DEL {}", toString());
        }
        redis.del(hashKey.duplicate());
    }

    @Override
    public Set<K> keySet() {
        return new ConcurrentMapKeySet<>(this);
    }

    @Override
    public Collection<V> values() {
        return new ConcurrentMapValueCollection<>(this);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new ConcurrentMapEntrySet<K, V>(this) {
            @Override
            public Iterator<Entry<K, V>> iterator() {
                return new MappingIterator<>(new RedisMapIterator(redis, hashKey.duplicate()), RedisMap.this::decode);
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getOrDefault(Object key, V defaultValue) {
        if (log.isTraceEnabled()) {
            log.trace("HGET {} {}", toString(), key);
        }
        ByteBuffer encodedValue = redis.hget(hashKey.duplicate(), encodeKey((K) key));
        return encodedValue == null ? defaultValue : decodeValue(encodedValue);
    }

    /**
     * @return null/value
     */
    @Override
    public V putIfAbsent(K key, V value) {
        if (hsetnx(key, value)) {
            return value;
        } else {
            return null;
        }
    }

    public boolean hsetnx(K key, V value) {
        if (log.isTraceEnabled()) {
            log.trace("HSETNX {} {} {}", toString(), key, value);
        }
        return redis.hsetnx(hashKey.duplicate(), encodeKey(key), encodeValue(value));
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
    public Map<K, V> get() {
        if (log.isTraceEnabled()) {
            log.trace("HGETALL {}", toString());
        }
        return redis.hgetall(hashKey.duplicate()).entrySet().stream()
                .collect(Collectors.toMap(
                        e -> decodeKey(e.getKey()),
                        e -> decodeValue(e.getValue()),
                        (a, b) -> b,
                        LinkedHashMap::new));
    }

    @Override
    public String toString() {
        return ByteBuffers.toString(hashKey);
    }
}
