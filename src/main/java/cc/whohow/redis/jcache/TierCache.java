package cc.whohow.redis.jcache;

import cc.whohow.redis.jcache.listener.CacheEntryEventListener;
import org.redisson.jcache.JCacheEntryEvent;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.event.EventType;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 两级缓存
 */
public class TierCache<K, V> implements Cache<K, V> {
    protected final RedisCache<K, V> redisCache;
    protected final TierInProcessCache<K, V> inProcessCache;
    protected final CacheEntryEventListener<K, V> cacheEntryEventListener;

    public TierCache(RedisCache<K, V> redisCache,
                     TierInProcessCache<K, V> inProcessCache,
                     CacheEntryEventListener<K, V> cacheEntryEventListener) {
        this.redisCache = redisCache;
        this.inProcessCache = inProcessCache;
        this.cacheEntryEventListener = cacheEntryEventListener;
    }

    @Override
    public V get(K key) {
        return inProcessCache.get(key, redisCache::get);
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        Map<K, V> keyValues = inProcessCache.getAll(keys);
        if (keyValues.size() == keys.size()) {
            return keyValues;
        }
        return redisCache.getAll(keys);
    }

    @Override
    public boolean containsKey(K key) {
        return inProcessCache.containsKey(key) || redisCache.containsKey(key);
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
//        redisCache.loadAll(keys, replaceExistingValues, completionListener);
//        inProcessCache.invalidateAll(keys);
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(K key, V value) {
        redisCache.put(key, value);
        inProcessCache.invalidate(key);
        updated(key, value);
    }

    @Override
    public V getAndPut(K key, V value) {
        V oldValue = redisCache.getAndPut(key, value);
        inProcessCache.invalidate(key);
        updated(key, value, oldValue);
        return oldValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        redisCache.putAll(map);
        inProcessCache.invalidateAll(map.keySet());
        updated(map);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        if (redisCache.putIfAbsent(key, value)) {
            inProcessCache.invalidate(key);
            updated(key, value);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(K key) {
        if (redisCache.remove(key)) {
            inProcessCache.invalidate(key);
            removed(key);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(K key, V oldValue) {
        if (redisCache.remove(key, oldValue)) {
            inProcessCache.invalidate(key);
            removed(key, oldValue);
            return true;
        }
        return false;
    }

    @Override
    public V getAndRemove(K key) {
        V value = redisCache.getAndRemove(key);
        inProcessCache.invalidate(key);
        removed(key, value);
        return value;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if (redisCache.replace(key, oldValue, newValue)) {
            inProcessCache.invalidate(key);
            updated(key, newValue, oldValue);
            return true;
        }
        return false;
    }

    @Override
    public boolean replace(K key, V value) {
        if (redisCache.replace(key, value)) {
            inProcessCache.invalidate(key);
            updated(key, value);
            return true;
        }
        return false;
    }

    @Override
    public V getAndReplace(K key, V value) {
        V oldValue = redisCache.getAndReplace(key, value);
        inProcessCache.invalidate(key);
        updated(key, value, oldValue);
        return oldValue;
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        redisCache.removeAll(keys);
        inProcessCache.invalidateAll(keys);
        removed(keys);
    }

    @Override
    public void removeAll() {
        redisCache.removeAll();
        inProcessCache.invalidateAll();
        removed();
    }

    @Override
    public void clear() {
        redisCache.clear();
        inProcessCache.invalidateAll();
        removed();
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return null;
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        T result = redisCache.invoke(key, entryProcessor, arguments);
        inProcessCache.invalidate(key);
        return result;
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        Map<K, EntryProcessorResult<T>> result = redisCache.invokeAll(keys, entryProcessor, arguments);
        inProcessCache.invalidateAll(keys);
        return result;
    }

    @Override
    public String getName() {
        return redisCache.getName();
    }

    @Override
    public CacheManager getCacheManager() {
        return null;
    }

    @Override
    public void close() {
        redisCache.close();
        inProcessCache.close();
    }

    @Override
    public boolean isClosed() {
        return redisCache.isClosed();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(redisCache)) {
            return clazz.cast(redisCache);
        }
        if (clazz.isInstance(inProcessCache)) {
            return clazz.cast(inProcessCache);
        }
        return inProcessCache.unwrap(clazz);
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        redisCache.registerCacheEntryListener(cacheEntryListenerConfiguration);
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        redisCache.deregisterCacheEntryListener(cacheEntryListenerConfiguration);
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return redisCache.iterator();
    }

    protected void updated(K key, V value) {
        cacheEntryEventListener.onUpdated(Collections.singleton(
                new JCacheEntryEvent<>(this, EventType.UPDATED, key, value)));
    }

    protected void updated(K key, V value, V oldValue) {
        cacheEntryEventListener.onUpdated(Collections.singleton(
                new JCacheEntryEvent<>(this, EventType.UPDATED, key, value)));
    }

    protected void updated(Map<? extends K, ? extends V> keyValues) {
        cacheEntryEventListener.onUpdated(keyValues.entrySet().stream()
                .map(e -> new JCacheEntryEvent<>(this, EventType.UPDATED, e.getKey(), e.getValue()))
                .collect(Collectors.toSet()));
    }

    protected void removed() {
        cacheEntryEventListener.onUpdated(Collections.singleton(
                new JCacheEntryEvent<>(this, EventType.REMOVED, null, null)));
    }

    protected void removed(K key) {
        cacheEntryEventListener.onUpdated(Collections.singleton(
                new JCacheEntryEvent<>(this, EventType.REMOVED, key, null)));
    }

    protected void removed(Collection<? extends K> key) {
        cacheEntryEventListener.onUpdated(key.stream()
                .map(k -> new JCacheEntryEvent<>(this, EventType.REMOVED, k, null))
                .collect(Collectors.toSet()));
    }

    protected void removed(K key, V oldValue) {
        cacheEntryEventListener.onUpdated(Collections.singleton(
                new JCacheEntryEvent<>(this, EventType.REMOVED, key, null)));
    }
}
