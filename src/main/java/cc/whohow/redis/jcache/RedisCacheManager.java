package cc.whohow.redis.jcache;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.JacksonCodec;
import cc.whohow.redis.jcache.codec.ImmutableGeneratedCacheKeyCodecBuilder;
import cc.whohow.redis.jcache.codec.RedisCacheKeyCodec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.lettuce.RedisCodecAdapter;
import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

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
 */
@SuppressWarnings("unchecked")
public class RedisCacheManager implements
        CacheManager,
        RedisConnectionStateListener,
        RedisPubSubListener<ByteBuffer, ByteBuffer> {
    /**
     * Redis URI
     */
    protected final URI uri;
    protected final RedisClient redisClient;
    protected final StatefulRedisConnection<ByteBuffer, ByteBuffer> redisConnection;
    protected final StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> redisPubSubConnection;
    protected final Map<String, Cache> caches = new ConcurrentHashMap<>();
    protected final String keyspace;
    protected final ByteBuffer encodedKeyspace;

    public RedisCacheManager(RedisClient redisClient, RedisURI uri) {
        this.uri = uri.toURI();
        this.redisClient = redisClient;
        this.redisClient.addListener(this);
        this.redisConnection = redisClient.connect(ByteBufferCodec.INSTANCE, uri);
        this.redisPubSubConnection = redisClient.connectPubSub(ByteBufferCodec.INSTANCE, uri);
        this.redisPubSubConnection.addListener(this);

        this.keyspace = "__keyspace@" + uri.getDatabase() + "__:";
        this.encodedKeyspace = ByteBuffers.fromUtf8(keyspace);
        this.redisPubSubConnection.sync().subscribe(ByteBuffers.fromUtf8("RedisCacheManager"));
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
        String separator = configuration.getKeyTypeCanonicalName().length == 0 ? "" : ":";
        Codec<K> keyCodec = (Codec<K>) newKeyCodec(configuration);
        Codec<V> valueCodec = (Codec<V>) newValueCodec(configuration);
        return new RedisCodecAdapter<>(new RedisCacheKeyCodec<>(configuration.getName(), separator, keyCodec), valueCodec);
    }

    private <K, V> Codec<?> newKeyCodec(RedisCacheConfiguration<K, V> configuration) {
        if (configuration.getKeyCodec().isEmpty()) {
            return new ImmutableGeneratedCacheKeyCodecBuilder().build(configuration.getKeyTypeCanonicalName());
        }
        throw new AssertionError("Not Implemented");
    }

    private <K, V> Codec<?> newValueCodec(RedisCacheConfiguration<K, V> configuration) {
        if (configuration.getValueCodec().isEmpty()) {
            return new JacksonCodec<>(configuration.getValueTypeCanonicalName());
        }
        throw new AssertionError("Not Implemented");
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
        return true;
    }

    private void onCacheManagerNotification(String message) {
        if ("sync".equalsIgnoreCase(message)) {
            for (Cache<?, ?> cache : caches.values()) {
                cache.onRedisSynchronization();
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
                cacheName.limit(cacheName.position());
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

    @Override
    public String toString() {
        return uri.toString();
    }
}
