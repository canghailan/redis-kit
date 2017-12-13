package cc.whohow.redis.jcache.listener;

import javax.cache.event.*;

public interface CacheEntryEventListener<K, V>  extends
        CacheEntryCreatedListener<K, V>,
        CacheEntryExpiredListener<K, V>,
        CacheEntryRemovedListener<K, V>,
        CacheEntryUpdatedListener<K, V> {
    @Override
    default void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {}

    @Override
    default void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
    }

    @Override
    default void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
    }

    @Override
    default void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
    }
}
