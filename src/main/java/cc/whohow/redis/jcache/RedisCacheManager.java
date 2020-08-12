package cc.whohow.redis.jcache;

import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.util.RedisKeyspaceNotification;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Redis缓存管理器
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class RedisCacheManager implements CacheManager {
    private static final Logger log = LogManager.getLogger();

    protected final RedisURI redisURI;
    protected final RedisClient redisClient;
    protected final RedisKeyspaceNotification redisKeyspaceNotification;
    protected final StatefulRedisConnection<ByteBuffer, ByteBuffer> redisConnection;
    protected final Function<String, RedisCacheConfiguration<?, ?>> redisCacheConfigurationProvider;
    protected final ConcurrentMap<String, Cache> caches = new ConcurrentHashMap<>();

    public RedisCacheManager(RedisClient redisClient,
                             RedisURI redisURI,
                             RedisKeyspaceNotification redisKeyspaceNotification,
                             Function<String, RedisCacheConfiguration<?, ?>> redisCacheConfigurationProvider) {
        this.redisURI = redisURI;
        this.redisClient = redisClient;
        this.redisKeyspaceNotification = redisKeyspaceNotification;
        this.redisCacheConfigurationProvider = redisCacheConfigurationProvider;
        this.redisConnection = redisClient.connect(ByteBufferCodec.INSTANCE, redisURI);
        RedisCachingProvider.getInstance().addCacheManager(this);
    }

    public RedisURI getRedisURI() {
        return redisURI;
    }

    public RedisClient getRedisClient() {
        return redisClient;
    }

    public RedisCommands<ByteBuffer, ByteBuffer> getRedisCommands() {
        return redisConnection.sync();
    }

    public RedisKeyspaceNotification getRedisKeyspaceNotification() {
        return redisKeyspaceNotification;
    }

    @Override
    public CachingProvider getCachingProvider() {
        return RedisCachingProvider.getInstance();
    }

    @Override
    public URI getURI() {
        return URI.create("redis://" + redisURI.getHost() + ":" + redisURI.getPort() + "/" + redisURI.getDatabase());
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
    public synchronized <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(
            String cacheName, C configuration) throws IllegalArgumentException {
        if (caches.containsKey(cacheName)) {
            throw new IllegalStateException();
        }
        Objects.requireNonNull(configuration);
        Cache<K, V> cache = newCache((RedisCacheConfiguration<K, V>) configuration);
        caches.put(cacheName, cache);
        return cache;
    }

    public <K, V> RedisCodec<K, V> newRedisCacheCodec(RedisCacheConfiguration<K, V> configuration) {
        try {
            return configuration.getRedisCacheCodecFactory().newInstance().getCodec(configuration);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new CacheException(e);
        }
    }

    public <K, V> Cache<K, V> newCache(RedisCacheConfiguration<K, V> configuration) {
        if (configuration.isRedisCacheEnabled()) {
            if (configuration.isInProcessCacheEnabled()) {
                return newRedisTierCache(configuration);
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
            return new RedisExpireCache<>(this, configuration);
        } else {
            return new RedisCache<>(this, configuration);
        }
    }

    public <K, V> RedisTierCache<K, V> newRedisTierCache(RedisCacheConfiguration<K, V> configuration) {
        return new RedisTierCache<>(this, configuration);
    }

    public <K, V> InProcessCache<K, V> newInProcessCache(RedisCacheConfiguration<K, V> configuration) {
        return new InProcessCache<>(this, configuration);
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        return caches.get(cacheName);
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName) {
        Cache<K, V> cache = caches.get(cacheName);
        if (cache != null) {
            return cache;
        }
        return (Cache<K, V>) createCache(cacheName, redisCacheConfigurationProvider.apply(cacheName));
    }

    @Override
    public Collection<String> getCacheNames() {
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
        log.info("enableManagement {} {}", cacheName, enabled);
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        log.info("enableStatistics {} {}", cacheName, enabled);
    }

    public Map<String, CacheStatisticsMXBean> getCacheStatistics() {
        Map<String, CacheStatisticsMXBean> cacheStatistics = new HashMap<>();
        for (Map.Entry<String, Cache> e : caches.entrySet()) {
            cacheStatistics.put(e.getKey(), e.getValue().getCacheStatistics());
        }
        return cacheStatistics;
    }

    @Override
    public void close() {
        log.info("close RedisCacheManager: {}", this);
        try {
            for (Cache cache : caches.values()) {
                try {
                    log.debug("close Cache: {}", cache.getName());
                    cache.close();
                } catch (Throwable e) {
                    log.warn("close Cache ERROR", e);
                }
            }
            caches.clear();
        } finally {
            log.debug("close RedisConnection");
            redisConnection.close();
        }
    }

    @Override
    public boolean isClosed() {
        return !redisConnection.isOpen();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public String toString() {
        return getURI().toString();
    }
}
