package cc.whohow.redis.jcache;

import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.jcache.listener.CacheEntryEventListener;

import javax.cache.event.*;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 多级缓存之进程内缓存（1级缓存）
 *
 * @param <K>
 * @param <V>
 */
public class TierInProcessCache<K, V> extends InProcessCache<K, V> implements CacheEntryEventListener<K, V> {
    public TierInProcessCache(RedisCacheConfiguration<K, V> configuration) {
        super(configuration);
    }

    public V get(K key, Function<? super K, ? extends V> mapping) {
        return cache.get(key, mapping);
    }

    public void invalidate(K key) {
        cache.invalidate(key);
    }

    public void invalidateAll(Set<? extends K> key) {
        cache.invalidateAll(key);
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
        invalidateAll(getCacheKeys(cacheEntryEvents));
    }

    @Override
    public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
        invalidateAll(getCacheKeys(cacheEntryEvents));
    }

    @Override
    public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
        invalidateAll(getCacheKeys(cacheEntryEvents));
    }

    protected Set<? extends K> getCacheKeys(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) {
        return StreamSupport.stream(cacheEntryEvents.spliterator(), false)
                .map(CacheEntryEvent::getKey)
                .collect(Collectors.toSet());
    }
}
