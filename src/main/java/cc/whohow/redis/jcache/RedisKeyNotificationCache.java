package cc.whohow.redis.jcache;

import cc.whohow.redis.Redis;
import cc.whohow.redis.client.RedisPipeline;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.misc.RPromise;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * 支持键通知的缓存
 */
public class RedisKeyNotificationCache<K, V> extends RedisCache<K, V> {
    public RedisKeyNotificationCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration, Redis redis) {
        super(cacheManager, configuration, redis);
    }

    @Override
    public void put(K key, V value) {
        ByteBuf encodedKey = encodeKey(key);

        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.SET, toRedisKey(encodedKey.retain()), encodeValue(value));
        pipeline.execute(RedisCommands.PUBLISH, name, encodedKey);
        pipeline.sync();
    }

    @Override
    public V getAndPut(K key, V value) {
        ByteBuf encodedKey = encodeKey(key);

        RedisPipeline pipeline = redis.pipeline();
        RPromise<V> r = pipeline.execute(valueCodec, RedisCommands.GETSET, toRedisKey(encodedKey.retain()), encodeValue(value));
        pipeline.execute(RedisCommands.PUBLISH, name, encodedKey);
        pipeline.sync();
        return r.getNow();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        ByteBuf[] encodedKeys = encodeKeys(map.keySet());
        ByteBuf[] encodedValues = encodeValues(map.values());
        ByteBuf[] encodedRedisKeyValues = new ByteBuf[encodedKeys.length * 2];
        for (int i = 0; i < encodedKeys.length; i++) {
            encodedRedisKeyValues[i * 2] = toRedisKey(encodedKeys[i].retain());
            encodedRedisKeyValues[i * 2 + 1] = encodedValues[i];
        }

        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MSET, (Object[]) encodedRedisKeyValues);
        for (ByteBuf encodedKey : encodedKeys) {
            pipeline.execute(RedisCommands.PUBLISH, name, encodedKey);
        }
        pipeline.sync();
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        ByteBuf encodedKey = encodeKey(key);

        RedisPipeline pipeline = redis.pipeline();
        RPromise<Boolean> r = pipeline.execute(RedisCommands.SETPXNX, toRedisKey(encodedKey.retain()), encodeValue(value), "NX");
        pipeline.execute(RedisCommands.PUBLISH, name, encodedKey);
        pipeline.sync();
        return r.getNow();
    }

    @Override
    public boolean remove(K key) {
        ByteBuf encodedKey = encodeKey(key);

        RedisPipeline pipeline = redis.pipeline();
        RPromise<Long> r = pipeline.execute(RedisCommands.DEL, toRedisKey(encodedKey.retain()));
        pipeline.execute(RedisCommands.PUBLISH, name, encodedKey);
        pipeline.sync();
        return r.getNow() == 1L;
    }

    @Override
    public boolean remove(K key, V oldValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V getAndRemove(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V value) {
        ByteBuf encodedKey = encodeKey(key);

        RedisPipeline pipeline = redis.pipeline();
        RPromise<Boolean> r = pipeline.execute(RedisCommands.SETPXNX, toRedisKey(encodedKey.retain()), encodeValue(value), "XX");
        pipeline.execute(RedisCommands.PUBLISH, name, encodedKey);
        pipeline.sync();
        return r.getNow();
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        ByteBuf[] encodedKeys = encodeKeys(keys);
        ByteBuf[] encodedRedisKeys = new ByteBuf[encodedKeys.length];
        for (int i = 0; i < encodedKeys.length; i++) {
            encodedRedisKeys[i] = toRedisKey(encodedKeys[i].retain());
        }

        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.DEL, (Object[]) encodedRedisKeys);
        for (ByteBuf encodedKey : encodedKeys) {
            pipeline.execute(RedisCommands.PUBLISH, name, encodedKey);
        }
        pipeline.sync();
    }

    @Override
    public void removeAll() {
        super.removeAll();
        cacheManager.sendRedisCacheManagerMessage("INVALIDATE", configuration.getName());
    }
}
