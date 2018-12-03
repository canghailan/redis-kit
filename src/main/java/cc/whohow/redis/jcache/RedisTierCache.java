package cc.whohow.redis.jcache;

import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger log = LogManager.getLogger();

    protected final RedisCacheManager cacheManager;
    protected final RedisCacheConfiguration<K, V> configuration;
    protected final RedisCache<K, V> redisCache;
    protected final com.github.benmanes.caffeine.cache.Cache<K, V> inProcessCache;

    @SuppressWarnings("unchecked")
    public RedisTierCache(RedisCacheManager cacheManager,
                          RedisCacheConfiguration<K, V> configuration) {
        this.cacheManager = cacheManager;
        this.configuration = configuration;
        this.redisCache = cacheManager.newRedisCache(configuration);
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
        this.inProcessCache = caffeine.build();
    }

    @Override
    public V get(K key) {
        return inProcessCache.get(key, redisCache::get);
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        Map<K, V> keyValues = inProcessCache.getAllPresent(keys);
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
        inProcessCache.invalidateAll(keys);
    }

    @Override
    public void put(K key, V value) {
        redisCache.put(key, value);
        inProcessCache.invalidate(key);
    }

    @Override
    public V getAndPut(K key, V value) {
        V oldValue = redisCache.getAndPut(key, value);
        inProcessCache.invalidate(key);
        return oldValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        redisCache.putAll(map);
        inProcessCache.invalidateAll(map.keySet());
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        if (redisCache.putIfAbsent(key, value)) {
            inProcessCache.invalidate(key);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(K key) {
        if (redisCache.remove(key)) {
            inProcessCache.invalidate(key);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(K key, V oldValue) {
        if (redisCache.remove(key, oldValue)) {
            inProcessCache.invalidate(key);
            return true;
        }
        return false;
    }

    @Override
    public V getAndRemove(K key) {
        V value = redisCache.getAndRemove(key);
        inProcessCache.invalidate(key);
        return value;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if (redisCache.replace(key, oldValue, newValue)) {
            inProcessCache.invalidate(key);
            return true;
        }
        return false;
    }

    @Override
    public boolean replace(K key, V value) {
        if (redisCache.replace(key, value)) {
            inProcessCache.invalidate(key);
            return true;
        }
        return false;
    }

    @Override
    public V getAndReplace(K key, V value) {
        V oldValue = redisCache.getAndReplace(key, value);
        inProcessCache.invalidate(key);
        return oldValue;
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        redisCache.removeAll(keys);
        inProcessCache.invalidateAll(keys);
    }

    @Override
    public void removeAll() {
        redisCache.removeAll();
        inProcessCache.invalidateAll();
    }

    @Override
    public void clear() {
        redisCache.clear();
        inProcessCache.invalidateAll();
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
        return cacheManager;
    }

    @Override
    public void close() {
        try {
            redisCache.close();
        } finally {
            inProcessCache.cleanUp();
        }
    }

    @Override
    public boolean isClosed() {
        return redisCache.isClosed();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(redisCache)) {
            return redisCache.unwrap(clazz);
        }
        if (clazz.isInstance(inProcessCache)) {
            return clazz.cast(inProcessCache);
        }
        throw new IllegalArgumentException();
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
    public void onRedisConnected() {
        inProcessCache.invalidateAll();
        log.debug("onRedisConnected invalidateAll");
    }

    @Override
    public void onRedisDisconnected() {
        inProcessCache.invalidateAll();
        log.debug("onRedisDisconnected invalidateAll");
    }

    @Override
    public void onSynchronization() {
        inProcessCache.invalidateAll();
        log.debug("onSynchronization invalidateAll");
    }

    @Override
    public void onKeyspaceNotification(ByteBuffer key, ByteBuffer message) {
        K k = redisCache.getCodec().decodeKey(key);
        inProcessCache.invalidate(k);
        log.debug("onKeyspaceNotification invalidate {}", k);
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> loader) {
        return inProcessCache.get(key, new CacheValueLoader<>(redisCache, loader));
    }

    @Override
    public CacheValue<V> getValue(K key) {
        return getValue(key, ImmutableCacheValue::new);
    }

    @Override
    public CacheValue<V> getValue(K key, Function<V, ? extends CacheValue<V>> factory) {
        CacheValueHolder<K, V> cacheValueHolder = new CacheValueHolder<>(redisCache, factory);
        V value = inProcessCache.get(key, cacheValueHolder);
        if (cacheValueHolder.getValue() != null) {
            return cacheValueHolder.getValue();
        }
        return value == null ? null : factory.apply(value);
    }

    @Override
    public String toString() {
        return redisCache.toString();
    }
}
