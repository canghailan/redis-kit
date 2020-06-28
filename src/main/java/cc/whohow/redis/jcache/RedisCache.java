package cc.whohow.redis.jcache;

import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.jcache.processor.EntryProcessorResultWrapper;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.util.RedisKeyIterator;
import cc.whohow.redis.util.impl.MappingIterator;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 普通Redis缓存，不支持过期时间
 */
public class RedisCache<K, V> implements Cache<K, V> {
    private static final Logger log = LogManager.getLogger();
    protected final RedisCacheManager cacheManager;
    protected final RedisCacheConfiguration<K, V> configuration;
    protected final RedisCodec<K, V> codec;
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final CacheStats cacheStats = new CacheStats();

    public RedisCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration) {
        Objects.requireNonNull(configuration.getName());
        this.cacheManager = cacheManager;
        this.configuration = configuration;
        this.codec = cacheManager.newRedisCacheCodec(configuration);
        this.redis = cacheManager.getRedisCommands();
    }

    public RedisCodec<K, V> getCodec() {
        return codec;
    }

    @Override
    public V get(K key) {
        ByteBuffer encodedValue = redis.get(codec.encodeKey(key));
        if (encodedValue != null) {
            cacheStats.cacheHit(1);
            return codec.decodeValue(encodedValue);
        } else {
            cacheStats.cacheMiss(1);
            return codec.decodeValue(null);
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        if (keys.isEmpty()) {
            return Collections.emptyMap();
        }
        ByteBuffer[] encodedKeys = keys.stream()
                .map(key -> (K) key)
                .map(codec::encodeKey)
                .peek(Buffer::mark)
                .toArray(ByteBuffer[]::new);
        return redis.mget(encodedKeys).stream()
                .peek(kv -> kv.getKey().reset())
                .peek(kv -> {
                    if (kv.hasValue()) {
                        cacheStats.cacheHit(1);
                    } else {
                        cacheStats.cacheMiss(1);
                    }
                })
                .collect(Collectors.toMap(
                        kv -> codec.decodeKey(kv.getKey()),
                        kv -> codec.decodeValue(kv.getValueOrElse(null))));
    }

    @Override
    public boolean containsKey(K key) {
        return redis.exists(codec.encodeKey(key)) > 0;
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(K key, V value) {
        redis.set(codec.encodeKey(key), codec.encodeValue(value));
        cacheStats.cachePut(1);
    }

    @Override
    public V getAndPut(K key, V value) {
        ByteBuffer encodedValue = redis.getset(codec.encodeKey(key), codec.encodeValue(value));
        if (encodedValue != null) {
            cacheStats.cacheHit(1);
            return codec.decodeValue(encodedValue);
        } else {
            cacheStats.cacheMiss(1);
            return codec.decodeValue(null);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        if (map.isEmpty()) {
            return;
        }
        Map<ByteBuffer, ByteBuffer> encodedKeyValues = map.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> codec.encodeKey(e.getKey()),
                        e -> codec.encodeValue(e.getValue())));
        redis.mset(encodedKeyValues);
        cacheStats.cachePut(map.size());
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        boolean ok = Lettuce.ok(redis.set(codec.encodeKey(key), codec.encodeValue(value), Lettuce.SET_NX));
        if (ok) {
            cacheStats.cachePut(1);
        }
        return ok;
    }

    @Override
    public boolean remove(K key) {
        boolean ok = redis.del(codec.encodeKey(key)) > 0;
        if (ok) {
            cacheStats.cacheRemove(1);
        }
        return ok;
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
        boolean ok = Lettuce.ok(redis.set(codec.encodeKey(key), codec.encodeValue(value), Lettuce.SET_XX));
        if (ok) {
            cacheStats.cachePut(1);
        }
        return ok;
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        if (keys.isEmpty()) {
            return;
        }
        ByteBuffer[] encodedKeys = keys.stream()
                .map(key -> (K) key)
                .map(codec::encodeKey)
                .toArray(ByteBuffer[]::new);
        long n = redis.del(encodedKeys);
        cacheStats.cacheRemove((int) n);
    }

    @Override
    public void removeAll() {
        ScanArgs scanArgs = ScanArgs.Builder.matches(configuration.getRedisKeyPattern()).limit(100);
        ScanCursor scanCursor = ScanCursor.INITIAL;
        while (!scanCursor.isFinished()) {
            KeyScanCursor<ByteBuffer> keyScanCursor = redis.scan(scanCursor, scanArgs);
            if (!keyScanCursor.getKeys().isEmpty()) {
                redis.del(keyScanCursor.getKeys().toArray(new ByteBuffer[0]));
                cacheStats.cacheRemove(keyScanCursor.getKeys().size());
            }
            scanCursor = keyScanCursor;
        }
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
        return entryProcessor.process(new MutableCacheEntry<>(this, key), arguments);
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
        return configuration.getName();
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void close() {
        log.info("Close Cache: {}", getName());
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException();
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
        return new MappingIterator<>(new MappingIterator<>(
                new RedisKeyIterator(redis, configuration.getRedisKeyPattern()), codec::decodeKey), this::getEntry);
    }


    @Override
    public String toString() {
        return getName();
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> loader) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CacheValue<V> getValue(K key, Function<V, ? extends CacheValue<V>> factory) {
        ByteBuffer encodedValue = redis.get(codec.encodeKey(key));
        if (encodedValue != null) {
            CacheValue<V> cacheValue = factory.apply(codec.decodeValue(encodedValue));
            cacheStats.cacheHit(1);
            return cacheValue;
        } else {
            cacheStats.cacheMiss(1);
            return null;
        }
    }

    @Override
    public CacheStatisticsMXBean getCacheStatistics() {
        return cacheStats;
    }

    @Override
    public CacheValue<V> getValue(K key) {
        return getValue(key, ImmutableCacheValue::new);
    }

    public Cache.Entry<K, V> getEntry(K key) {
        return new LazyCacheEntry<>(this, key);
    }
}
