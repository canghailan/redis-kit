package cc.whohow.redis.jcache.listener;

import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;

public interface CacheEntryEventListener<K, V>  extends
        CacheEntryCreatedListener<K, V>,
        CacheEntryExpiredListener<K, V>,
        CacheEntryRemovedListener<K, V>,
        CacheEntryUpdatedListener<K, V> {
}
