package cc.whohow.redis.jcache;

import cc.whohow.redis.Redis;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.jcache.listener.RedisPubSubCacheEntryEventListener;

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
public class JCacheManager implements CacheManager {
    protected JCachingProvider cachingProvider;
    protected final Redis redis;
    protected final Map<String, Cache> caches = new ConcurrentHashMap<>();

    public JCacheManager(JCachingProvider cachingProvider, Redis redis) {
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
        return URI.create("cache:redis");
    }

    @Override
    public ClassLoader getClassLoader() {
        return JCacheManager.class.getClassLoader();
    }

    @Override
    public Properties getProperties() {
        return null;
    }

    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) throws IllegalArgumentException {
        RedisCacheConfiguration<K, V> redisCacheConfiguration = (RedisCacheConfiguration<K, V>) configuration;
        if (redisCacheConfiguration.isRedisCacheEnabled()) {
            if (redisCacheConfiguration.isInProcessCacheEnabled()) {
                return createTierCache(redisCacheConfiguration);
            } else {
                return createRedisCache(redisCacheConfiguration);
            }
        } else {
            if (redisCacheConfiguration.isInProcessCacheEnabled()) {
                return createInProcessCache(redisCacheConfiguration);
            } else {
                throw new IllegalArgumentException();
            }
        }
    }

    public <K, V> RedisCache<K, V> createRedisCache(RedisCacheConfiguration<K, V> configuration) {
        if (configuration.getExpiryForUpdate() > 0) {
            return new RedisExpireCache<>(configuration, redis);
        } else {
            return new RedisCache<>(configuration, redis);
        }
    }

    public <K, V> InProcessCache<K, V> createInProcessCache(RedisCacheConfiguration<K, V> configuration) {
        return new InProcessCache<>(configuration);
    }

    public <K, V> TierCache<K, V> createTierCache(RedisCacheConfiguration<K, V> configuration) {
        return new TierCache<>(
                createRedisCache(configuration),
                new TierInProcessCache<>(configuration),
                new RedisPubSubCacheEntryEventListener<>(configuration, redis));
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
        Cache cache = caches.get(cacheName);
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
