package cc.whohow.redis.jcache;

import cc.whohow.redis.RESP;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.lettuce.StatusOutput;
import cc.whohow.redis.lettuce.VoidOutput;
import io.lettuce.core.protocol.CommandType;

import java.util.Map;

/**
 * Redis缓存，支持过期时间
 */
public class RedisExpireCache<K, V> extends RedisCache<K, V> {
    protected final ByteSequence ttl;

    public RedisExpireCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration) {
        super(cacheManager, configuration);
        this.ttl = RESP.b(configuration.getExpiryForUpdateTimeUnit().toMillis(configuration.getExpiryForUpdate()));
    }

    @Override
    public void put(K key, V value) {
        redis.send(new VoidOutput(), CommandType.SET, keyCodec.encode(key), valueCodec.encode(value), RESP.px(), ttl);
        cacheStats.cachePut(1);
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
        boolean ok = RESP.ok(redis.send(new StatusOutput(), CommandType.SET, keyCodec.encode(key), valueCodec.encode(value), RESP.px(), ttl, RESP.nx()));
        if (ok) {
            cacheStats.cachePut(1);
        }
        return ok;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V value) {
        boolean ok = RESP.ok(redis.send(new StatusOutput(), CommandType.SET, keyCodec.encode(key), valueCodec.encode(value), RESP.px(), ttl, RESP.xx()));
        if (ok) {
            cacheStats.cachePut(1);
        }
        return ok;
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException();
    }
}
