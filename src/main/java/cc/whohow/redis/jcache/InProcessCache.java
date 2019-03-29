package cc.whohow.redis.jcache;

import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.jcache.processor.EntryProcessorResultWrapper;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * 内存缓存，基于caffeine实现
 */
public class InProcessCache<K, V> implements Cache<K, V> {
    protected final RedisCacheManager cacheManager;
    protected final RedisCacheConfiguration<K, V> configuration;
    protected final com.github.benmanes.caffeine.cache.Cache<K, V> cache;

    @SuppressWarnings("unchecked")
    public InProcessCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration) {
        this.cacheManager = cacheManager;
        this.configuration = configuration;
        Caffeine caffeine = Caffeine.newBuilder();
        if (configuration.getInProcessCacheMaxEntry() > 0) {
            caffeine.maximumSize(configuration.getInProcessCacheMaxEntry());
        }
        long expiryForUpdate = configuration.getExpiryForUpdate() > 0 ?
                configuration.getExpiryForUpdateTimeUnit().toMillis(configuration.getExpiryForUpdate()) :
                -1;
        long inProcessCacheExpiryForUpdate = configuration.getInProcessCacheExpiryForUpdate() > 0 ?
                configuration.getInProcessCacheExpiryForUpdateTimeUnit().toMillis(configuration.getInProcessCacheExpiryForUpdate()) :
                -1;
        if (0 < inProcessCacheExpiryForUpdate && inProcessCacheExpiryForUpdate < expiryForUpdate) {
            caffeine.expireAfterWrite(
                    configuration.getInProcessCacheExpiryForUpdate(),
                    configuration.getInProcessCacheExpiryForUpdateTimeUnit());
        } else if (expiryForUpdate > 0) {
            caffeine.expireAfterWrite(
                    configuration.getExpiryForUpdate(),
                    configuration.getExpiryForUpdateTimeUnit());
        }
        if (configuration.isStatisticsEnabled()) {
            caffeine.recordStats();
        }
        this.cache = caffeine.build();
    }

    @Override
    public V get(K key) {
        return cache.getIfPresent(key);
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        return cache.getAllPresent(keys);
    }

    @Override
    public boolean containsKey(K key) {
        return cache.asMap().containsKey(key);
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public V getAndPut(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        cache.putAll(map);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(K key) {
        cache.invalidate(key);
        return true;
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
        throw new UnsupportedOperationException();
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        cache.invalidateAll(keys);
    }

    @Override
    public void removeAll() {
        cache.invalidateAll();
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }

    @Override
    @SuppressWarnings("unchecked")
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
        cache.cleanUp();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(cache)) {
            return clazz.cast(cache);
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
        return cache.asMap().entrySet().stream()
                .map(e -> (Entry<K, V>) new ImmutableCacheEntry<>(e.getKey(), e.getValue()))
                .iterator();
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> loader) {
        return cache.get(key, loader);
    }

    @Override
    public CacheValue<V> getValue(K key) {
        return getValue(key, ImmutableCacheValue::new);
    }

    @Override
    public CacheValue<V> getValue(K key, Function<V, ? extends CacheValue<V>> factory) {
        V value = cache.getIfPresent(key);
        return value == null ? null : factory.apply(value);
    }

    @Override
    public void onSynchronization() {
        cache.invalidateAll();
    }

    @Override
    public CacheStatisticsMXBean getCacheStatistics() {
        return new InProcessCacheStatisticsAdapter(cache);
    }

    @Override
    public String toString() {
        return getName();
    }
}
