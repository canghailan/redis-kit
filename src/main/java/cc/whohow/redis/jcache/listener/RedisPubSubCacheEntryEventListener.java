package cc.whohow.redis.jcache.listener;

import cc.whohow.redis.Redis;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;

public class RedisPubSubCacheEntryEventListener<K, V> implements CacheEntryEventListener<K, V> {
    protected final RedisCacheConfiguration<K, V> configuration;
    protected final Redis redis;

    public RedisPubSubCacheEntryEventListener(RedisCacheConfiguration<K, V> configuration, Redis redis) {
        this.configuration = configuration;
        this.redis = redis;
    }

    @Override
    public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {

    }

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {

    }

    @Override
    public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {

    }

    @Override
    public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {

    }
}
