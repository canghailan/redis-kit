package cc.whohow.redis.jcache;

import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * 两级缓存，仅支持 Read Through 模式
 */
public class RedisTierCache<K, V> implements Cache<K, V> {
    protected final RedisCacheManager cacheManager;
    protected final RedisCacheConfiguration<K, V> configuration;
    protected final RedisCache<K, V> redisCache;
    protected final InProcessCache<K, V> inProcessCache;

    public RedisTierCache(RedisCacheManager cacheManager,
                          RedisCacheConfiguration<K, V> configuration) {
        this.cacheManager = cacheManager;
        this.configuration = configuration;
        this.redisCache = cacheManager.newRedisCache(configuration);
        this.inProcessCache = cacheManager.newInProcessCache(configuration);
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
        return redisCache.containsKey(key);
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        redisCache.loadAll(keys, replaceExistingValues, completionListener);
        inProcessCache.removeAll(keys);
    }

    @Override
    public void put(K key, V value) {
        redisCache.put(key, value);
        inProcessCache.remove(key);
    }

    @Override
    public V getAndPut(K key, V value) {
        V oldValue = redisCache.getAndPut(key, value);
        inProcessCache.remove(key);
        return oldValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        redisCache.putAll(map);
        inProcessCache.removeAll(map.keySet());
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        if (redisCache.putIfAbsent(key, value)) {
            inProcessCache.remove(key);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(K key) {
        if (redisCache.remove(key)) {
            inProcessCache.remove(key);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(K key, V oldValue) {
        if (redisCache.remove(key, oldValue)) {
            inProcessCache.remove(key);
            return true;
        }
        return false;
    }

    @Override
    public V getAndRemove(K key) {
        V value = redisCache.getAndRemove(key);
        inProcessCache.remove(key);
        return value;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if (redisCache.replace(key, oldValue, newValue)) {
            inProcessCache.remove(key);
            return true;
        }
        return false;
    }

    @Override
    public boolean replace(K key, V value) {
        if (redisCache.replace(key, value)) {
            inProcessCache.remove(key);
            return true;
        }
        return false;
    }

    @Override
    public V getAndReplace(K key, V value) {
        V oldValue = redisCache.getAndReplace(key, value);
        inProcessCache.remove(key);
        return oldValue;
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        redisCache.removeAll(keys);
        inProcessCache.removeAll(keys);
    }

    @Override
    public void removeAll() {
        redisCache.removeAll();
        inProcessCache.removeAll();
    }

    @Override
    public void clear() {
        redisCache.clear();
        inProcessCache.clear();
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
        T result = redisCache.invoke(key, entryProcessor, arguments);
        inProcessCache.remove(key);
        return result;
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        Map<K, EntryProcessorResult<T>> result = redisCache.invokeAll(keys, entryProcessor, arguments);
        inProcessCache.removeAll(keys);
        return result;
    }

    @Override
    public String getName() {
        return redisCache.getName();
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void close() {
        try {
            redisCache.close();
        } finally {
            inProcessCache.close();
        }
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

    @Override
    public CacheValue<V> getValue(K key, Function<V, ? extends CacheValue<V>> ofNullable) {
        CacheValue<V> value = inProcessCache.getValue(key, ofNullable);
        if (value != null) {
            return value;
        }
        return redisCache.getValue(key, ofNullable);
    }

    @Override
    public void onRedisConnected() {
        inProcessCache.clear();
    }

    @Override
    public void onRedisDisconnected() {
        inProcessCache.clear();
    }

    @Override
    public void onRedisSynchronization() {
        inProcessCache.clear();
    }

    @Override
    public void onKeyspaceNotification(ByteBuffer key, ByteBuffer message) {
        inProcessCache.remove(redisCache.getCodec().decodeKey(key));
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> cacheLoader) {
        return inProcessCache.get(key, (k) -> redisCache.get(k, cacheLoader));
    }

    @Override
    public String toString() {
        return redisCache.toString();
    }
}
