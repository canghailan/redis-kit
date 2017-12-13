package cc.whohow.redis.jcache;

import cc.whohow.redis.Redis;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.io.Closeable;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unchecked")
public class RedisCacheManager implements CacheManager {
    protected final RedisCachingProvider cachingProvider;
    protected final Redis redis;
    protected final Map<String, Cache> caches = new ConcurrentHashMap<>();

    public RedisCacheManager(Redis redis) {
        this(new RedisCachingProvider(), redis);
    }

    public RedisCacheManager(RedisCachingProvider cachingProvider, Redis redis) {
        this.cachingProvider = cachingProvider;
        this.cachingProvider.setCacheManager(this);
        this.redis = redis;
    }

    @Override
    public CachingProvider getCachingProvider() {
        return cachingProvider;
    }

    @Override
    public URI getURI() {
        return redis.getUri();
    }

    @Override
    public ClassLoader getClassLoader() {
        return RedisCacheManager.class.getClassLoader();
    }

    @Override
    public Properties getProperties() {
        return null;
    }

    @Override
    public synchronized <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) throws IllegalArgumentException {
        if (caches.containsKey(cacheName)) {
            throw new IllegalStateException();
        }
        Cache<K, V> cache = newCache((RedisCacheConfiguration<K, V>) configuration);
        caches.put(cacheName, cache);
        return cache;
    }

    public <K, V> Cache<K, V> newCache(RedisCacheConfiguration<K, V> configuration) {
        if (configuration.isRedisCacheEnabled()) {
            if (configuration.isInProcessCacheEnabled()) {
                return newTierCache(configuration);
            } else {
                return newRedisCache(configuration);
            }
        } else {
            if (configuration.isInProcessCacheEnabled()) {
                return newInProcessCache(configuration);
            } else {
                throw new IllegalArgumentException();
            }
        }
    }

    public <K, V> RedisCache<K, V> newRedisCache(RedisCacheConfiguration<K, V> configuration) {
        if (configuration.getExpiryForUpdate() > 0) {
            return new RedisExpireCache<>(this, configuration, redis);
        } else {
            return new RedisCache<>(this, configuration, redis);
        }
    }

    public <K, V> InProcessCache<K, V> newInProcessCache(RedisCacheConfiguration<K, V> configuration) {
        return new InProcessCache<>(this, configuration);
    }

    public <K, V> TierCache<K, V> newTierCache(RedisCacheConfiguration<K, V> configuration) {
        return new TierCache<>(this, configuration);
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        return caches.get(cacheName);
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName) {
        return caches.get(cacheName);
    }

    @Override
    public Iterable<String> getCacheNames() {
        return caches.keySet();
    }

    @Override
    public void destroyCache(String cacheName) {
        Cache cache = caches.remove(cacheName);
        if (cache != null) {
            cache.close();
        }
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        caches.values().forEach(this::closeQuietly);
    }

    protected void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable ignore) {
            }
        }
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
}
