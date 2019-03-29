package cc.whohow.redis.jcache;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

public class CacheStatisticsProxy<K, V> implements Cache<K, V> {
    protected final Cache<K, V> cache;
    protected final CacheStats cacheStats = new CacheStats();

    public CacheStatisticsProxy(Cache<K, V> cache) {
        this.cache = cache;
    }

    protected boolean isHit(V value) {
        return value != null;
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> loader) {
        CacheStatisticsLoaderProxy<? super K, ? extends V> proxy = new CacheStatisticsLoaderProxy<>(loader);
        long t0 = System.currentTimeMillis();
        V r = cache.get(key, proxy);
        long t1 = System.currentTimeMillis();
        if (proxy.isApplied()) {
            cacheStats.cacheMiss(1, t1 - t0);
            cacheStats.cachePut(1, t1 - t0);
        } else {
            cacheStats.cacheHit(1, t1 - t0);
        }
        return r;
    }

    @Override
    public CacheValue<V> getValue(K key) {
        long t0 = System.currentTimeMillis();
        CacheValue<V> r = cache.getValue(key);
        long t1 = System.currentTimeMillis();
        if (r != null) {
            cacheStats.cacheHit(1, t1 - t0);
        } else {
            cacheStats.cacheMiss(1, t1 - t0);
        }
        return r;
    }

    @Override
    public CacheValue<V> getValue(K key, Function<V, ? extends CacheValue<V>> factory) {
        CacheStatisticsLoaderProxy<V, ? extends CacheValue<V>> proxy = new CacheStatisticsLoaderProxy<>(factory);
        long t0 = System.currentTimeMillis();
        CacheValue<V> r = cache.getValue(key, factory);
        long t1 = System.currentTimeMillis();
        if (proxy.isApplied()) {
            cacheStats.cacheMiss(1, t1 - t0);
            cacheStats.cachePut(1, t1 - t0);
        } else {
            cacheStats.cacheHit(1, t1 - t0);
        }
        return r;
    }

    @Override
    public void onRedisConnected() {
        cache.onRedisConnected();
    }

    @Override
    public void onRedisDisconnected() {
        cache.onRedisDisconnected();
    }

    @Override
    public void onSynchronization() {
        cache.onSynchronization();
    }

    @Override
    public void onKeyspaceNotification(ByteBuffer key, ByteBuffer message) {
        cache.onKeyspaceNotification(key, message);
    }

    @Override
    public CacheStatisticsMXBean getCacheStatistics() {
        return cacheStats;
    }

    @Override
    public V get(K key) {
        long t0 = System.currentTimeMillis();
        V r = cache.get(key);
        long t1 = System.currentTimeMillis();
        if (isHit(r)) {
            cacheStats.cacheHit(1, t1 - t0);
        } else {
            cacheStats.cacheMiss(1, t1 - t0);
        }
        return r;
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        long t0 = System.currentTimeMillis();
        Map<K, V> r = cache.getAll(keys);
        long t1 = System.currentTimeMillis();
        cacheStats.cacheHit(r.size(), t1 - t0);
        cacheStats.cacheMiss(keys.size() - r.size(), t1 - t0);
        return r;
    }

    @Override
    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        long t0 = System.currentTimeMillis();
        cache.loadAll(keys, replaceExistingValues, completionListener);
        long t1 = System.currentTimeMillis();
        cacheStats.cachePut(keys.size(), t1 - t0);
    }

    @Override
    public void put(K key, V value) {
        long t0 = System.currentTimeMillis();
        cache.put(key, value);
        long t1 = System.currentTimeMillis();
        cacheStats.cachePut(1, t1 - t0);
    }

    @Override
    public V getAndPut(K key, V value) {
        long t0 = System.currentTimeMillis();
        V r = cache.getAndPut(key, value);
        long t1 = System.currentTimeMillis();
        if (isHit(r)) {
            cacheStats.cacheHit(1, t1 - t0);
        } else {
            cacheStats.cacheMiss(1, t1 - t0);
        }
        cacheStats.cachePut(1, t1 - t0);
        return r;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        long t0 = System.currentTimeMillis();
        cache.putAll(map);
        long t1 = System.currentTimeMillis();
        cacheStats.cachePut(map.size(), t1 - t0);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        long t0 = System.currentTimeMillis();
        boolean r = cache.putIfAbsent(key, value);
        long t1 = System.currentTimeMillis();
        if (r) {
            cacheStats.cacheMiss(1, t1 - t0);
            cacheStats.cachePut(1, t1 - t0);
        } else {
            cacheStats.cacheHit(1, t1 - t0);
        }
        return r;
    }

    @Override
    public boolean remove(K key) {
        long t0 = System.currentTimeMillis();
        boolean r = cache.remove(key);
        long t1 = System.currentTimeMillis();
        cacheStats.cacheRemove(1, t1 - t0);
        return r;
    }

    @Override
    public boolean remove(K key, V oldValue) {
        long t0 = System.currentTimeMillis();
        boolean r = cache.remove(key, oldValue);
        long t1 = System.currentTimeMillis();
        if (r) {
            cacheStats.cacheHit(1, t1 - t0);
            cacheStats.cacheRemove(1, t1 - t0);
        } else {
            cacheStats.cacheMiss(1, t1 - t0);
        }
        return r;
    }

    @Override
    public V getAndRemove(K key) {
        long t0 = System.currentTimeMillis();
        V r = cache.getAndRemove(key);
        long t1 = System.currentTimeMillis();
        if (isHit(r)) {
            cacheStats.cacheHit(1, t1 - t0);
            cacheStats.cacheRemove(1, t1 - t0);
        } else {
            cacheStats.cacheMiss(1, t1 - t0);
        }
        return r;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        long t0 = System.currentTimeMillis();
        boolean r = cache.replace(key, oldValue, newValue);
        long t1 = System.currentTimeMillis();
        if (r) {
            cacheStats.cacheHit(1, t1 - t0);
            cacheStats.cachePut(1, t1 - t0);
        } else {
            cacheStats.cacheMiss(1, t1 - t0);
        }
        return r;
    }

    @Override
    public boolean replace(K key, V value) {
        long t0 = System.currentTimeMillis();
        boolean r = cache.replace(key, value);
        long t1 = System.currentTimeMillis();
        if (r) {
            cacheStats.cacheHit(1, t1 - t0);
        } else {
            cacheStats.cacheMiss(1, t1 - t0);
        }
        cacheStats.cachePut(1, t1 - t0);
        return r;
    }

    @Override
    public V getAndReplace(K key, V value) {
        long t0 = System.currentTimeMillis();
        V r = cache.getAndReplace(key, value);
        long t1 = System.currentTimeMillis();
        if (isHit(r)) {
            cacheStats.cacheHit(1, t1 - t0);
        } else {
            cacheStats.cacheMiss(1, t1 - t0);
        }
        cacheStats.cachePut(1, t1 - t0);
        return r;
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        long t0 = System.currentTimeMillis();
        cache.removeAll(keys);
        long t1 = System.currentTimeMillis();
        cacheStats.cacheRemove(keys.size(), t1 - t0);
    }

    @Override
    public void removeAll() {
        cache.removeAll();
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return cache.getConfiguration(clazz);
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        return cache.invoke(key, entryProcessor, arguments);
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        return cache.invokeAll(keys, entryProcessor, arguments);
    }

    @Override
    public String getName() {
        return cache.getName();
    }

    @Override
    public CacheManager getCacheManager() {
        return cache.getCacheManager();
    }

    @Override
    public void close() {
        cache.close();
    }

    @Override
    public boolean isClosed() {
        return cache.isClosed();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return cache.unwrap(clazz);
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        cache.registerCacheEntryListener(cacheEntryListenerConfiguration);
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        cache.deregisterCacheEntryListener(cacheEntryListenerConfiguration);
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return cache.iterator();
    }

    @Override
    public void forEach(Consumer<? super Entry<K, V>> action) {
        cache.forEach(action);
    }

    @Override
    public Spliterator<Entry<K, V>> spliterator() {
        return cache.spliterator();
    }
}
