package cc.whohow.redis.jcache;

import cc.whohow.redis.Redis;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommands;

import java.util.Map;

/**
 * 支持过期时间缓存
 */
public class RedisExpireCache<K, V> extends RedisCache<K, V> {
    protected final long ttl;

    public RedisExpireCache(String name, Redis redis, Codec keyCodec, Codec valueCodec, long ttl) {
        super(name, redis, keyCodec, valueCodec);
        this.ttl = ttl;
    }

    @Override
    public void put(K key, V value) {
        redis.execute(RedisCommands.SETPXNX, encodeKey(key), encodeValue(value), "PX", ttl);
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
        return redis.execute(RedisCommands.SETPXNX, encodeKey(key), encodeValue(value), "PX", ttl, "NX");
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V value) {
        return redis.execute(RedisCommands.SETPXNX, encodeKey(key), encodeValue(value), "PX", ttl, "XX");
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }
}
