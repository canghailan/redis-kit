package cc.whohow.redis.jcache;

import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.redisson.jcache.JCacheEntry;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 进程内缓存
 */
public class InProcessCache<K, V> implements Cache<K, V> {
    protected final RedisCacheConfiguration<K, V> configuration;
    protected final com.github.benmanes.caffeine.cache.Cache<K, V> cache;

    @SuppressWarnings("unchecked")
    public InProcessCache(RedisCacheConfiguration<K, V> configuration) {
        this.configuration = configuration;
        Caffeine caffeine = Caffeine.newBuilder();
        if (configuration.getInProcessCacheMaxEntry() > 0) {
            caffeine.maximumSize(configuration.getInProcessCacheMaxEntry());
        }
        if (configuration.getInProcessCacheExpiryForUpdate() > 0) {
            caffeine.expireAfterWrite(
                    configuration.getInProcessCacheExpiryForUpdate(),
                    configuration.getInProcessCacheExpiryForUpdateTimeUnit());
        } else if (configuration.getExpiryForUpdate() > 0) {
            caffeine.expireAfterWrite(
                    configuration.getExpiryForUpdate(),
                    configuration.getExpiryForUpdateTimeUnit());
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
        return cache.getIfPresent(key) != null;
    }

    @Override
    @Deprecated
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    @Deprecated
    public V getAndPut(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        cache.putAll(map);
    }

    @Override
    @Deprecated
    public boolean putIfAbsent(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(K key) {
        cache.invalidate(key);
        return true;
    }

    @Override
    @Deprecated
    public boolean remove(K key, V oldValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public V getAndRemove(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public boolean replace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
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
        return null;
    }

    @Override
    @Deprecated
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public CacheManager getCacheManager() {
        return null;
    }

    @Override
    public void close() {
        cache.cleanUp();
    }

    @Override
    @Deprecated
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
    @Deprecated
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return cache.asMap().entrySet().stream()
                .map(e -> (Entry<K, V>) new JCacheEntry<>(e.getKey(), e.getValue()))
                .iterator();
    }
}
