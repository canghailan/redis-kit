package cc.whohow.redis.jcache;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis缓存管理器
 * CONFIG GET notify-keyspace-events
 * CONFIG SET notify-keyspace-events AK
 */
@SuppressWarnings("unchecked")
public class RedisCacheManager implements
        CacheManager,
        RedisConnectionStateListener,
        RedisPubSubListener<ByteBuffer, ByteBuffer> {
    protected static final ByteBuffer KEY = ByteBuffers.fromUtf8("_cache_");

    /**
     * Redis URI
     */
    protected final Map<String, Cache> caches = new ConcurrentHashMap<>();
    protected final URI uri;
    protected final RedisURI redisURI;
    protected final RedisClient redisClient;
    protected final StatefulRedisConnection<ByteBuffer, ByteBuffer> redisConnection;
    protected final StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> redisPubSubConnection;
    protected final String keyspace;
    protected final ByteBuffer encodedKeyspace;

    public RedisCacheManager(RedisClient redisClient, RedisURI redisURI) {
        this.uri = redisURI.toURI();
        this.redisURI = redisURI;
        this.redisClient = redisClient;
        this.redisClient.addListener(this);
        this.redisConnection = redisClient.connect(ByteBufferCodec.INSTANCE, redisURI);
        this.redisPubSubConnection = redisClient.connectPubSub(ByteBufferCodec.INSTANCE, redisURI);
        this.redisPubSubConnection.addListener(this);

        this.keyspace = "__keyspace@" + redisURI.getDatabase() + "__:";
        this.encodedKeyspace = ByteBuffers.fromUtf8(keyspace);
        this.redisPubSubConnection.sync().subscribe(KEY.duplicate());
        this.redisPubSubConnection.sync().psubscribe(ByteBuffers.fromUtf8(keyspace + "*"));
        RedisCachingProvider.getInstance().addCacheManager(this);
    }

    @Override
    public CachingProvider getCachingProvider() {
        return RedisCachingProvider.getInstance();
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public ClassLoader getClassLoader() {
        return RedisCacheManager.class.getClassLoader();
    }

    @Override
    public Properties getProperties() {
        return null;
    }

    public synchronized <K, V> Cache<K, V> resolveCache(RedisCacheConfiguration configuration) {
        Cache<K, V> cache = caches.get(configuration.getName());
        if (cache == null) {
            cache = createCache(configuration.getName(), configuration);
        }
        return cache;
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

    public <K, V> InProcessCache<K, V> newInProcessCache(RedisCacheConfiguration<K, V> configuration) {
        return new InProcessCache<>(this, configuration);
    }

    public <K, V> RedisTierCache<K, V> newRedisTierCache(RedisCacheConfiguration<K, V> configuration) {
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
        try {
            for (Cache cache : caches.values()) {
                try {
                    cache.close();
                } catch (Throwable ignore) {
                }
            }
            caches.clear();
        } finally {
            redisConnection.close();
            redisPubSubConnection.close();
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
    public void onRedisConnected(RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
        for (Cache<?, ?> cache : caches.values()) {
            cache.onRedisConnected();
        }
    }

    @Override
    public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
        for (Cache<?, ?> cache : caches.values()) {
            cache.onRedisDisconnected();
        }
    }

    @Override
    public void onRedisExceptionCaught(RedisChannelHandler<?, ?> connection, Throwable cause) {
    }

    @Override
    public void message(ByteBuffer channel, ByteBuffer message) {
        if (isCacheManagerNotification(channel)) {
            onCacheManagerNotification(StandardCharsets.UTF_8.decode(message).toString());
        } else if (isKeyNotification(channel)) {
            onKeyNotification(channel, message);
        }
    }

    @Override
    public void message(ByteBuffer pattern, ByteBuffer channel, ByteBuffer message) {
        if (isKeyNotification(channel)) {
            onKeyNotification(channel, message);
        }
    }

    @Override
    public void subscribed(ByteBuffer channel, long count) {
    }

    @Override
    public void psubscribed(ByteBuffer pattern, long count) {
    }

    @Override
    public void unsubscribed(ByteBuffer channel, long count) {
    }

    @Override
    public void punsubscribed(ByteBuffer pattern, long count) {
    }

    private boolean isCacheManagerNotification(ByteBuffer channel) {
        return ByteBuffers.contentEquals(channel, KEY);
    }

    private void onCacheManagerNotification(String message) {
        if ("sync".equalsIgnoreCase(message)) {
            for (Cache<?, ?> cache : caches.values()) {
                cache.onSynchronization();
            }
        }
    }

    private boolean isKeyNotification(ByteBuffer channel) {
        return ByteBuffers.startsWith(channel, encodedKeyspace);
    }

    private ByteBuffer getKeyFromKeyNotificationChannel(ByteBuffer channel) {
        ByteBuffer key = channel.duplicate();
        key.position(key.position() + encodedKeyspace.remaining());
        return key;
    }

    private String getCacheNameFromKey(ByteBuffer key) {
        ByteBuffer cacheName = key.duplicate();
        cacheName.mark();
        while (cacheName.hasRemaining()) {
            if (cacheName.get() == ':') {
                cacheName.limit(cacheName.position() - 1);
                break;
            }
        }
        cacheName.reset();
        return StandardCharsets.UTF_8.decode(cacheName).toString();
    }

    public void onKeyNotification(ByteBuffer channel, ByteBuffer message) {
        ByteBuffer key = getKeyFromKeyNotificationChannel(channel);
        String cacheName = getCacheNameFromKey(key);
        Cache<?, ?> cache = caches.get(cacheName);
        if (cache != null) {
            cache.onKeyspaceNotification(key, message);
        }
    }

    public RedisCommands<ByteBuffer, ByteBuffer> getRedisCommands() {
        return redisConnection.sync();
    }

    public void enableKeyspaceNotification() {
        RedisCommands<ByteBuffer, ByteBuffer> redis = redisConnection.sync();
        String value = redis.configGet("notify-keyspace-events")
                .getOrDefault("notify-keyspace-events", "");
        if (value.contains("K") && value.contains("A")) {
            return;
        }
        if (value.contains("E")) {
            value = "KEA";
        } else {
            value = "KA";
        }
        redis.configSet("notify-keyspace-events", value);
    }

    @Override
    public String toString() {
        return uri.toString();
    }
}
