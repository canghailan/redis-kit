package cc.whohow.redis.jcache;

import cc.whohow.redis.Redis;
import cc.whohow.redis.client.RedisPipeline;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import io.netty.buffer.ByteBuf;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.misc.RPromise;

import java.util.Map;

/**
 * 支持键通知、支持过期的缓存
 */
public class RedisKeyNotificationExpireCache<K, V> extends RedisKeyNotificationCache<K, V> {
    protected final long ttl;

    public RedisKeyNotificationExpireCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration, Redis redis) {
        super(cacheManager, configuration, redis);
        this.ttl = configuration.getExpiryForUpdateTimeUnit().toMillis(configuration.getExpiryForUpdate());
    }

    @Override
    public void put(K key, V value) {
        ByteBuf encodedKey = encodeKey(key);

        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.SETPXNX, toRedisKey(encodedKey.retain()), encodeValue(value), "PX", ttl);
        pipeline.execute(RedisCommands.PUBLISH, name, encodedKey);
        pipeline.sync();
    }

    @Override
    public V getAndPut(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        ByteBuf encodedKey = encodeKey(key);

        RedisPipeline pipeline = redis.pipeline();
        RPromise<Boolean> r = pipeline.execute(RedisCommands.SETPXNX, toRedisKey(encodedKey.retain()), encodeValue(value), "PX", ttl, "NX");
        pipeline.execute(RedisCommands.PUBLISH, name, encodedKey);
        pipeline.sync();
        return r.getNow();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V value) {
        ByteBuf encodedKey = encodeKey(key);

        RedisPipeline pipeline = redis.pipeline();
        RPromise<Boolean> r = pipeline.execute(RedisCommands.SETPXNX, toRedisKey(encodedKey.retain()), encodeValue(value), "PX", ttl, "XX");
        pipeline.execute(RedisCommands.PUBLISH, name, encodedKey);
        pipeline.sync();
        return r.getNow();
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }
}
