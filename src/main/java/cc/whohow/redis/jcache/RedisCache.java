package cc.whohow.redis.jcache;

import cc.whohow.redis.Redis;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.jcache.processor.CacheMutableEntry;
import cc.whohow.redis.jcache.processor.EntryProcessorResultWrapper;
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
    protected final RedisCacheManager cacheManager;
    protected final RedisCacheConfiguration<K, V> configuration;
    protected final Redis redis;
    protected final String name;
    protected final ByteBuf keyPrefix;
    protected final Codec keyCodec;
    protected final Codec valueCodec;

    public RedisCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration, Redis redis) {
        if (configuration.getName() == null ||
                configuration.getKeyCodec() == null ||
                configuration.getValueCodec() == null) {
            throw new IllegalArgumentException();
        }
        this.cacheManager = cacheManager;
        this.configuration = configuration;
        this.redis = redis;
        this.name = configuration.getName();
        this.keyPrefix = Unpooled.copiedBuffer(name, StandardCharsets.UTF_8).writeByte(':').asReadOnly();
        this.keyCodec = configuration.getKeyCodec();
        this.valueCodec = configuration.getValueCodec();
    }

    public Codec getKeyCodec() {
        return keyCodec;
    }

    public Codec getValueCodec() {
        return valueCodec;
    }

    public ByteBuf encodeRedisKey(K key) {
        return toRedisKey(encodeKey(key));
    }

    public ByteBuf toRedisKey(ByteBuf key) {
        return Unpooled.wrappedBuffer(keyPrefix, key);
    }

    public ByteBuf encodeKey(K key) {
        try {
            return keyCodec.getValueEncoder().encode(key);
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
        return redis.execute(valueCodec, RedisCommands.GET, encodeRedisKey(key));
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        Object[] encodedKeys = keys.stream().map(this::encodeRedisKey).toArray();
        List<? extends V> values = redis.execute(valueCodec, RedisCommands.MGET, encodedKeys);
        return toMap(keys, values);
    }

    @Override
    public boolean containsKey(K key) {
        return redis.execute(RedisCommands.EXISTS, encodeRedisKey(key));
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(K key, V value) {
        redis.execute(RedisCommands.SET, encodeRedisKey(key), encodeValue(value));
    }

    @Override
    public V getAndPut(K key, V value) {
        return redis.execute(valueCodec, RedisCommands.GETSET, encodeRedisKey(key), encodeValue(value));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        Object[] encodedKeyValues = map.entrySet().stream()
                .flatMap(e -> Stream.of(encodeRedisKey(e.getKey()), encodeValue(e.getValue())))
                .toArray();
        redis.execute(RedisCommands.MSET, encodedKeyValues);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return redis.execute(RedisCommands.SETPXNX, encodeRedisKey(key), encodeValue(value), "NX");
    }

    @Override
    public boolean remove(K key) {
        return redis.execute(RedisCommands.DEL, encodeRedisKey(key)) == 1L;
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
        return redis.execute(RedisCommands.SETPXNX, encodeRedisKey(key), encodeValue(value), "XX");
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        redis.execute(RedisCommands.DEL, keys.stream().map(this::encodeRedisKey).toArray());
    }

    @Override
    public void removeAll() {
        Codec codec = new ScanCodec(StringCodec.INSTANCE);
        ByteBuf pattern = keyPrefix.copy().writeByte('*');
        Long pos = 0L;
        do {
            ListScanResult<ScanObjectEntry> result = redis.execute(
                    codec, RedisCommands.SCAN, pos, "MATCH", pattern, "COUNT", 100);
            if (!result.getValues().isEmpty()) {
                redis.execute(RedisCommands.DEL, result.getValues().stream().map(ScanObjectEntry::getBuf).toArray());
            }
            pos = result.getPos();
        } while (pos != 0);
    }

    @Override
    public void clear() {
        removeAll();
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(configuration)) {
            return clazz.cast(configuration);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        return entryProcessor.process(new CacheMutableEntry<>(this, key), arguments);
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        Map<K, EntryProcessorResult<T>> results = new LinkedHashMap<>();
        for (K key : keys) {
            try {
                results.put(key, new EntryProcessorResultWrapper<>(invoke(key, entryProcessor, arguments)));
            } catch (RuntimeException e) {
                results.put(key, new EntryProcessorResultWrapper<>(new EntryProcessorException(e)));
            }
        }
        return results;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        throw new UnsupportedOperationException();
    }
}