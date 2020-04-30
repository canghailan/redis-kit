package cc.whohow.redis.jcache;

import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.lettuce.Lettuce;
import io.lettuce.core.SetArgs;

import java.util.Map;

/**
 * Redis缓存，支持过期时间
 */
public class RedisExpireCache<K, V> extends RedisCache<K, V> {
    protected final long ttl;
    protected final SetArgs px;
    protected final SetArgs pxNx;
    protected final SetArgs pxXx;

    public RedisExpireCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration) {
        super(cacheManager, configuration);
        this.ttl = configuration.getExpiryForUpdateTimeUnit().toMillis(configuration.getExpiryForUpdate());
        this.px = SetArgs.Builder.px(ttl);
        this.pxNx = SetArgs.Builder.px(ttl).nx();
        this.pxXx = SetArgs.Builder.px(ttl).xx();
    }

    @Override
    public void put(K key, V value) {
        redis.set(codec.encodeKey(key), codec.encodeValue(value), px);
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
        boolean ok = Lettuce.ok(redis.set(codec.encodeKey(key), codec.encodeValue(value), pxNx));
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
        boolean ok = Lettuce.ok(redis.set(codec.encodeKey(key), codec.encodeValue(value), pxXx));
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
