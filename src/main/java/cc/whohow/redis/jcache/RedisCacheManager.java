package cc.whohow.redis.jcache;

import cc.whohow.redis.Redis;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import org.redisson.client.RedisPubSubConnection;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.protocol.pubsub.PubSubType;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unchecked")
public class RedisCacheManager implements CacheManager, RedisPubSubListener<Object> {
    protected final RedisCachingProvider cachingProvider;
    protected final Redis redis;
    protected final Map<String, Cache> caches = new ConcurrentHashMap<>();
    protected final RedisPubSubConnection subscribeKeyConnection;
    protected final Map<String, RedisPubSubListener> subscribeKeyCaches = new ConcurrentHashMap<>();

    public RedisCacheManager(Redis redis) {
        this(new RedisCachingProvider(), redis);
    }

    public RedisCacheManager(RedisCachingProvider cachingProvider, Redis redis) {
        this.cachingProvider = cachingProvider;
        this.cachingProvider.setCacheManager(this);
        this.redis = redis;
        this.subscribeKeyConnection = redis.getPubSubConnection();
        this.subscribeKeyConnection.addListener(this);
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
        RedisCacheConfiguration<K, V> redisCacheConfiguration = (RedisCacheConfiguration<K, V>) configuration;
        Cache<K, V> cache = newCache(redisCacheConfiguration);
        caches.put(cacheName, cache);
        if (cache instanceof RedisPubSubListener) {
            RedisPubSubListener keyListener = (RedisPubSubListener) cache;
            subscribeKeyCaches.put(redisCacheConfiguration.getName(), keyListener);
            subscribeKeyConnection.subscribe(redisCacheConfiguration.getKeyCodec(), redisCacheConfiguration.getName());
        }
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
    public Collection<String> getCacheNames() {
        return caches.keySet();
    }

    @Override
    public void destroyCache(String cacheName) {
        try {
            if (subscribeKeyCaches.remove(cacheName) != null) {
                subscribeKeyConnection.unsubscribe(cacheName);
            }
        } finally {
            Cache cache = caches.remove(cacheName);
            if (cache != null) {
                cache.close();
            }
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
        try {
            subscribeKeyConnection.closeAsync().syncUninterruptibly();
        } finally {
            for (Cache cache : caches.values()) {
                try {
                    cache.close();
                } catch (Throwable ignore) {
                }
            }
            caches.clear();
            subscribeKeyCaches.clear();
        }
    }

    @Override
    public boolean isClosed() {
        return subscribeKeyConnection.isClosed();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public boolean onStatus(PubSubType type, String name) {
        RedisPubSubListener listener = subscribeKeyCaches.get(name);
        if (listener != null) {
            listener.onStatus(type, name);
        }
        return false;
    }

    @Override
    public void onPatternMessage(String pattern, String name, Object key) {
        RedisPubSubListener listener = subscribeKeyCaches.get(name);
        if (listener != null) {
            listener.onPatternMessage(pattern, name, key);
        }
    }

    @Override
    public void onMessage(String name, Object key) {
        RedisPubSubListener listener = subscribeKeyCaches.get(name);
        if (listener != null) {
            listener.onMessage(name, key);
        }
    }
}
