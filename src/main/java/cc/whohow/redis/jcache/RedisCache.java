package cc.whohow.redis.jcache;

import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.jcache.processor.EntryProcessorResultWrapper;
import cc.whohow.redis.lettuce.Lettuce;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 普通Redis缓存，不支持过期时间
 */
public class RedisCache<K, V> implements Cache<K, V> {
    protected final RedisCacheManager cacheManager;
    protected final RedisCacheConfiguration<K, V> configuration;
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final RedisCodec<K, V> codec;

    public RedisCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration) {
        Objects.requireNonNull(configuration.getName());
        this.cacheManager = cacheManager;
        this.configuration = configuration;
        this.redis = cacheManager.getRedisCommands();
        this.codec = cacheManager.newRedisCacheCodec(configuration);
    }

    public RedisCodec<K, V> getCodec() {
        return codec;
    }

    @Override
    public void onRedisConnected() {
    }

    @Override
    public void onRedisDisconnected() {
    }

    @Override
    public void onKeyspaceNotification(ByteBuffer key, ByteBuffer message) {
    }

    @Override
    public V get(K key) {
        return codec.decodeValue(redis.get(codec.encodeKey(key)));
    }

    @Override
    public <CV extends CacheValue<V>> CV getValue(K key, Function<V, CV> ofNullable) {
        ByteBuffer encodedValue = redis.get(codec.encodeKey(key));
        if (Lettuce.isNil(encodedValue)) {
            return null;
        }
        return ofNullable.apply(codec.decodeValue(encodedValue));
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        ByteBuffer[] encodedKeys = keys.stream()
                .map(key -> (K) key)
                .map(codec::encodeKey)
                .toArray(ByteBuffer[]::new);
        return redis.mget(encodedKeys).stream()
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
    }

    @Override
    public V getAndPut(K key, V value) {
        return codec.decodeValue(redis.getset(codec.encodeKey(key), codec.encodeValue(value)));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        Map<ByteBuffer, ByteBuffer> encodedKeyValues = map.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> codec.encodeKey(e.getKey()),
                        e -> codec.encodeValue(e.getValue())));
        redis.mset(encodedKeyValues);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return Lettuce.ok(redis.set(codec.encodeKey(key), codec.encodeValue(value), Lettuce.SET_NX));
    }

    @Override
    public boolean remove(K key) {
        return redis.del(codec.encodeKey(key)) > 0;
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
        return Lettuce.ok(redis.set(codec.encodeKey(key), codec.encodeValue(value), Lettuce.SET_XX));
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        ByteBuffer[] encodedKeys = keys.stream()
                .map(key -> (K) key)
                .map(codec::encodeKey)
                .toArray(ByteBuffer[]::new);
        redis.del(encodedKeys);
    }

    @Override
    public void removeAll() {
        ScanArgs scanArgs = ScanArgs.Builder.matches(getName() + ":*").limit(100);
        ScanCursor scanCursor = ScanCursor.INITIAL;
        while (!scanCursor.isFinished()) {
            KeyScanCursor<ByteBuffer> keyScanCursor = redis.scan(scanArgs);
            redis.del(keyScanCursor.getKeys().toArray(new ByteBuffer[0]));
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
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "redisClient=" + redis +
                ", name=" + configuration.getName() +
                ", expiryForUpdate=" + configuration.getExpiryForUpdate() +
                ", expiryForUpdateTimeUnit=" + configuration.getExpiryForUpdateTimeUnit() +
                ", keyTypeCanonicalName=" + Arrays.toString(configuration.getKeyTypeCanonicalName()) +
                ", valueTypeCanonicalName='" + configuration.getValueTypeCanonicalName() + '\'' +
                '}';
    }
}
