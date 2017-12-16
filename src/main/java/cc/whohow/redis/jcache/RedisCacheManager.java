package cc.whohow.redis.jcache;

import cc.whohow.redis.Redis;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.pubsub.PubSubType;

import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@SuppressWarnings("unchecked")
public class RedisCacheManager implements CacheManager, RedisPubSubListener<Object> {
    private static final Logger log = LogManager.getLogger();

    protected final String redisCacheManagerChannel = "RedisCacheManager";
    protected final Map<String, Consumer<String>> commands = new HashMap<>();

    protected final Redis redis;
    protected final Map<String, Cache> caches = new ConcurrentHashMap<>();
    protected final Map<String, RedisPubSubListener> keyNotificationListeners = new ConcurrentHashMap<>();

    public RedisCacheManager(Redis redis) {
        this.redis = redis;
        this.initializeCommands();
        this.redis.subscribe(redisCacheManagerChannel, StringCodec.INSTANCE, this);
        RedisCachingProvider.getInstance().addCacheManager(this);
    }

    private void initializeCommands() {
        commands.put(RedisCacheManagerCommand.SYNC.name(), this::onSyncMessage);
    }

    @Override
    public CachingProvider getCachingProvider() {
        return RedisCachingProvider.getInstance();
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

    public <K, V> Cache<K, V> resolveCache(RedisCacheConfiguration configuration) {
        Cache<K, V> cache = caches.get(configuration.getName());
        if (cache == null) {
            cache = createCache(configuration.getName(), configuration);
        }
        log.trace("resolve {}", cache);
        return cache;
    }

    public <K, V> Cache<K, V> createCache(RedisCacheConfiguration configuration) {
        return createCache(configuration.getName(), configuration);
    }

    @Override
    public synchronized <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) throws IllegalArgumentException {
        if (redisCacheManagerChannel.equals(cacheName)) {
            throw new IllegalArgumentException();
        }
        if (caches.containsKey(cacheName)) {
            throw new IllegalStateException();
        }
        RedisCacheConfiguration<K, V> redisCacheConfiguration = (RedisCacheConfiguration<K, V>) configuration;
        Cache<K, V> cache = newCache(redisCacheConfiguration);
        caches.put(cacheName, cache);
        if (cache instanceof RedisPubSubListener) {
            RedisPubSubListener keyListener = (RedisPubSubListener) cache;
            keyNotificationListeners.put(redisCacheConfiguration.getName(), keyListener);
            redis.subscribe(redisCacheConfiguration.getName(), redisCacheConfiguration.getKeyCodec());
        }
        log.trace("create {}", cache);
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
            if (configuration.isKeyNotificationEnabled()) {
                return new RedisKeyNotificationExpireCache<>(this, configuration, redis);
            } else {
                return new RedisExpireCache<>(this, configuration, redis);
            }
        } else {
            if (configuration.isKeyNotificationEnabled()) {
                return new RedisKeyNotificationCache<>(this, configuration, redis);
            } else {
                return new RedisCache<>(this, configuration, redis);
            }
        }
    }

    public <K, V> InProcessCache<K, V> newInProcessCache(RedisCacheConfiguration<K, V> configuration) {
        return new InProcessCache<>(this, configuration);
    }

    public <K, V> RedisTierCache<K, V> newTierCache(RedisCacheConfiguration<K, V> configuration) {
        return new RedisTierCache<>(this, configuration);
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
            if (keyNotificationListeners.remove(cacheName) != null) {
                redis.unsubscribe(cacheName);
            }
        } finally {
            Cache cache = caches.remove(cacheName);
            if (cache != null) {
                cache.close();
            }
        }
        log.trace("destroy {}", cacheName);
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
            keyNotificationListeners.clear();
        } finally {
            for (Cache cache : caches.values()) {
                try {
                    cache.close();
                } catch (Throwable ignore) {
                }
            }
            caches.clear();
        }
        log.trace("close RedisCacheManager");
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

    @Override
    public boolean onStatus(PubSubType type, String channel) {
        log.trace("onStatus {} {}", type, channel);
        if (!redisCacheManagerChannel.equals(channel)) {
            RedisPubSubListener listener = keyNotificationListeners.get(channel);
            if (listener != null) {
                listener.onStatus(type, channel);
            }
        }
        return false;
    }

    @Override
    public void onPatternMessage(String pattern, String channel, Object message) {
        log.trace("onPatternMessage {} {} {}", pattern, channel, message);
        if (redisCacheManagerChannel.equals(channel)) {
            onRedisCacheManagerMessage((String) message);
        } else {
            RedisPubSubListener listener = keyNotificationListeners.get(channel);
            if (listener != null) {
                listener.onPatternMessage(pattern, channel, message);
            }
        }
    }

    @Override
    public void onMessage(String channel, Object message) {
        log.trace("onMessage {} {}", channel, message);
        if (redisCacheManagerChannel.equals(channel)) {
            onRedisCacheManagerMessage((String) message);
        } else {
            RedisPubSubListener listener = keyNotificationListeners.get(channel);
            if (listener != null) {
                listener.onMessage(channel, message);
            }
        }
    }

    public void sendRedisCacheManagerMessage(RedisCacheManagerCommand command, String message) {
        redis.execute(RedisCommands.PUBLISH, redisCacheManagerChannel, command.name() + " " + message);
    }

    public void onRedisCacheManagerMessage(String message) {
        String[] parsed = message.split(" ", 2);
        Consumer<String> handler = commands.get(parsed[0]);
        if (handler != null) {
            handler.accept(parsed[1]);
        }
    }

    public void onSyncMessage(String cacheName) {
        log.trace("SYNC {}", cacheName);
        Cache cache = caches.get(cacheName);
        if (cache != null && cache instanceof RedisTierCache) {
            RedisTierCache redisTierCache = (RedisTierCache) cache;
            redisTierCache.synchronizeAll();
        }
    }

    @Override
    public String toString() {
        return "RedisCacheManager{" +
                "redis=" + redis +
                ", caches=" + caches +
                '}';
    }
}
