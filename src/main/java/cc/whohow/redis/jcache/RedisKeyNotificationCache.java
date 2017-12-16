package cc.whohow.redis.jcache;

import cc.whohow.redis.Redis;
import cc.whohow.redis.RedisPipeline;
import cc.whohow.redis.codec.Codecs;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import io.netty.buffer.ByteBuf;
import org.redisson.api.RFuture;
import org.redisson.client.protocol.RedisCommands;

import java.util.Map;
import java.util.Set;

/**
 * 支持键通知的缓存
 */
public class RedisKeyNotificationCache<K, V> extends RedisCache<K, V> {
    public RedisKeyNotificationCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration, Redis redis) {
        super(cacheManager, configuration, redis);
    }

    @Override
    public void put(K key, V value) {
        ByteBuf encodedKey = Codecs.encode(keyCodec, key);

        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.SET, toRedisKey(encodedKey.retain()), Codecs.encode(valueCodec, value));
        pipeline.execute(RedisCommands.PUBLISH, name.retain(), encodedKey);
        pipeline.flush();
    }

    @Override
    public V getAndPut(K key, V value) {
        ByteBuf encodedKey = Codecs.encode(keyCodec, key);

        RedisPipeline pipeline = redis.pipeline();
        RFuture<V> r = pipeline.execute(valueCodec, RedisCommands.GETSET, toRedisKey(encodedKey.retain()), Codecs.encode(valueCodec, value));
        pipeline.execute(RedisCommands.PUBLISH, name.retain(), encodedKey);
        pipeline.flush();
        return r.getNow();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        ByteBuf[] encodedKeys = Codecs.encode(keyCodec, map.keySet());
        ByteBuf[] encodedValues = Codecs.encode(valueCodec, map.values());
        ByteBuf[] encodedRedisKeyValues = new ByteBuf[encodedKeys.length * 2];
        for (int i = 0; i < encodedKeys.length; i++) {
            encodedRedisKeyValues[i * 2] = toRedisKey(encodedKeys[i].retain());
            encodedRedisKeyValues[i * 2 + 1] = encodedValues[i];
        }

        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.MSET, (Object[]) encodedRedisKeyValues);
        for (ByteBuf encodedKey : encodedKeys) {
            pipeline.execute(RedisCommands.PUBLISH, name.retain(), encodedKey);
        }
        pipeline.flush();
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        ByteBuf encodedKey = Codecs.encode(keyCodec, key);

        RedisPipeline pipeline = redis.pipeline();
        RFuture<Boolean> r = pipeline.execute(RedisCommands.SETPXNX, toRedisKey(encodedKey.retain()), Codecs.encode(valueCodec, value), "NX");
        pipeline.execute(RedisCommands.PUBLISH, name.retain(), encodedKey);
        pipeline.flush();
        return r.getNow();
    }

    @Override
    public boolean remove(K key) {
        ByteBuf encodedKey = Codecs.encode(keyCodec, key);

        RedisPipeline pipeline = redis.pipeline();
        RFuture<Long> r = pipeline.execute(RedisCommands.DEL, toRedisKey(encodedKey.retain()));
        pipeline.execute(RedisCommands.PUBLISH, name.retain(), encodedKey);
        pipeline.flush();
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
        ByteBuf encodedKey = Codecs.encode(keyCodec, key);

        RedisPipeline pipeline = redis.pipeline();
        RFuture<Boolean> r = pipeline.execute(RedisCommands.SETPXNX, toRedisKey(encodedKey.retain()), Codecs.encode(valueCodec, value), "XX");
        pipeline.execute(RedisCommands.PUBLISH, name.retain(), encodedKey);
        pipeline.flush();
        return r.getNow();
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        ByteBuf[] encodedKeys = Codecs.encode(keyCodec, keys);
        ByteBuf[] encodedRedisKeys = new ByteBuf[encodedKeys.length];
        for (int i = 0; i < encodedKeys.length; i++) {
            encodedRedisKeys[i] = toRedisKey(encodedKeys[i].retain());
        }

        RedisPipeline pipeline = redis.pipeline();
        pipeline.execute(RedisCommands.DEL, (Object[]) encodedRedisKeys);
        for (ByteBuf encodedKey : encodedKeys) {
            pipeline.execute(RedisCommands.PUBLISH, name.retain(), encodedKey);
        }
        pipeline.flush();
    }

    @Override
    public void removeAll() {
        super.removeAll();
        cacheManager.sendRedisCacheManagerMessage(RedisCacheManagerCommand.SYNC, configuration.getName());
    }
}
