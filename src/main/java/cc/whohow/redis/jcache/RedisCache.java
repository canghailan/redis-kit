package cc.whohow.redis.jcache;

import cc.whohow.redis.PooledRedisConnection;
import cc.whohow.redis.Redis;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.ScanCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.decoder.ListScanResult;
import org.redisson.client.protocol.decoder.ScanObjectEntry;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;

/**
 * 普通Redis缓存，不支持过期时间
 */
public class RedisCache<K, V> implements Cache<K, V> {
    protected final String name;
    protected final Redis redis;
    protected final ByteBuf cacheKey;
    protected final Codec keyCodec;
    protected final Codec valueCodec;

    public RedisCache(String name, Redis redis, Codec keyCodec, Codec valueCodec) {
        this.name = name;
        this.redis = redis;
        this.cacheKey = Unpooled.copiedBuffer(name, StandardCharsets.UTF_8).writeByte(':').asReadOnly();
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    public ByteBuf encodeKey(K key) {
        try {
            return cacheKey.copy().writeBytes(keyCodec.getValueEncoder().encode(key));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ByteBuf encodeValue(V value) {
        try {
            return valueCodec.getValueEncoder().encode(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected Map<K, V> toMap(Iterable<? extends K> keys, Iterable<? extends V> values) {
        Map<K, V> map = new LinkedHashMap<>();
        Iterator<? extends V> value = values.iterator();
        for (K key : keys) {
            if (value.hasNext()) {
                map.put(key, value.next());
            } else {
                throw new IllegalStateException();
            }
        }
        return map;
    }

    @Override
    public V get(K key) {
        ByteBuf encodedKey = encodeKey(key);
        try (PooledRedisConnection connection = redis.getPooledConnection()) {
            return connection.execute(valueCodec, RedisCommands.GET, encodedKey);
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        Object[] encodedKeys = keys.stream().map(this::encodeKey).toArray();
        try (PooledRedisConnection connection = redis.getPooledConnection()) {
            List<? extends V> values = connection.execute(valueCodec, RedisCommands.MGET, encodedKeys);
            return toMap(keys, values);
        }
    }

    @Override
    public boolean containsKey(K key) {
        ByteBuf encodedKey = encodeKey(key);
        try (PooledRedisConnection connection = redis.getPooledConnection()) {
            return connection.execute(RedisCommands.EXISTS, encodedKey);
        }
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(K key, V value) {
        ByteBuf encodedKey = encodeKey(key);
        ByteBuf encodedValue = encodeValue(value);
        try (PooledRedisConnection connection = redis.getPooledConnection()) {
            connection.execute(RedisCommands.SET, encodedKey, encodedValue);
        }
    }

    @Override
    public V getAndPut(K key, V value) {
        ByteBuf encodedKey = encodeKey(key);
        ByteBuf encodedValue = encodeValue(value);
        try (PooledRedisConnection connection = redis.getPooledConnection()) {
            return connection.execute(valueCodec, RedisCommands.GETSET, encodedKey, encodedValue);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        Object[] encodedKeyValues = map.entrySet().stream()
                .flatMap(e -> Stream.of(encodeKey(e.getKey()), encodeValue(e.getValue())))
                .toArray();
        try (PooledRedisConnection connection = redis.getPooledConnection()) {
            connection.execute(RedisCommands.MSET, encodedKeyValues);
        }
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        ByteBuf encodedKey = encodeKey(key);
        ByteBuf encodedValue = encodeValue(value);
        try (PooledRedisConnection connection = redis.getPooledConnection()) {
            return connection.execute(RedisCommands.SETPXNX, encodedKey, encodedValue, "NX");
        }
    }

    @Override
    public boolean remove(K key) {
        ByteBuf encodedKey = encodeKey(key);
        try (PooledRedisConnection connection = redis.getPooledConnection()) {
            return connection.execute(RedisCommands.DEL, encodedKey) == 1L;
        }
    }

    @Override
    public boolean remove(K key, V oldValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V getAndRemove(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V value) {
        ByteBuf encodedKey = encodeKey(key);
        ByteBuf encodedValue = encodeValue(value);
        try (PooledRedisConnection connection = redis.getPooledConnection()) {
            return connection.execute(RedisCommands.SETPXNX, encodedKey, encodedValue, "XX");
        }
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        Object[] encodedKeys = keys.stream().map(this::encodeKey).toArray();
        try (PooledRedisConnection connection = redis.getPooledConnection()) {
            connection.execute(RedisCommands.DEL, encodedKeys);
        }
    }

    @Override
    public void removeAll() {
        Codec codec = new ScanCodec(StringCodec.INSTANCE);
        ByteBuf pattern = cacheKey.copy().writeByte('*');
        try (PooledRedisConnection connection = redis.getPooledConnection()) {
            Long pos = 0L;
            do {
                ListScanResult<ScanObjectEntry> result = connection.execute(codec,
                        RedisCommands.SCAN, pos, "MATCH", pattern, "COUNT", 100);
                connection.execute(RedisCommands.DEL, result.getValues().stream().map(ScanObjectEntry::getBuf).toArray());
                pos = result.getPos();
            } while (pos != 0);
        }
    }

    @Override
    public void clear() {
        removeAll();
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return null;
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CacheManager getCacheManager() {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return null;
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        throw new UnsupportedOperationException();
    }
}
