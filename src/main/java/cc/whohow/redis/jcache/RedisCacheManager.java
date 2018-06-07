package cc.whohow.redis.jcache;

import cc.whohow.redis.codec.ByteBufferCodec;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
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

@SuppressWarnings("unchecked")
public class RedisCacheManager implements
        CacheManager,
        RedisConnectionStateListener,
        RedisPubSubListener<ByteBuffer, ByteBuffer> {
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
        this.encodedKeyspace = StandardCharsets.UTF_8.encode(keyspace).asReadOnlyBuffer();
        this.redisPubSubConnection.async().psubscribe(getKeyspaceChannel());
        RedisCachingProvider.getInstance().addCacheManager(this);
    }

    private ByteBuffer getKeyspaceChannel() {
        return StandardCharsets.UTF_8.encode(keyspace + "*").asReadOnlyBuffer();
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

    public <K, V> Cache<K, V> resolveCache(RedisCacheConfiguration configuration) {
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
        RedisCacheConfiguration<K, V> redisCacheConfiguration = (RedisCacheConfiguration<K, V>) configuration;
        Cache<K, V> cache = newCache(redisCacheConfiguration);
        caches.put(cacheName, cache);
        return cache;
    }

    public <K, V> RedisCacheCodec<K, V> newRedisCacheCodec(RedisCacheConfiguration<K, V> configuration) {
        return null;
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
        if (isKeyNotification(channel)) {
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

    private boolean isKeyNotification(ByteBuffer channel) {
        if (channel.remaining() < encodedKeyspace.remaining()) {
            return false;
        }
        for (int i = encodedKeyspace.remaining() - 1; i >= 0; i--) {
            if (channel.get(i) != encodedKeyspace.get(i)) {
                return false;
            }
        }
        return true;
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
                cacheName.reset();
                return StandardCharsets.UTF_8.decode(cacheName).toString();
            }
        }
        return null;
    }

    public void onKeyNotification(ByteBuffer channel, ByteBuffer message) {
        ByteBuffer key = getKeyFromKeyNotificationChannel(channel);
        String cacheName = getCacheNameFromKey(key);
        if (cacheName == null) {
            return;
        }
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
        return "RedisCacheManager{" +
                "redisClient=" + redisClient +
                ", caches=" + caches +
                '}';
    }
}
