package cc.whohow.redis.jcache.listener;

import javax.cache.event.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class CacheEntryEventManager<K, V> implements CacheEntryEventListener<K, V> {
    protected final CopyOnWriteArrayList<CacheEntryCreatedListener<K, V>> cacheEntryCreatedListeners = new CopyOnWriteArrayList<>();
    protected final CopyOnWriteArrayList<CacheEntryExpiredListener<K, V>> cacheEntryExpiredListeners = new CopyOnWriteArrayList<>();
    protected final CopyOnWriteArrayList<CacheEntryRemovedListener<K, V>> cacheEntryRemovedListeners = new CopyOnWriteArrayList<>();
    protected final CopyOnWriteArrayList<CacheEntryUpdatedListener<K, V>> cacheEntryUpdatedListeners = new CopyOnWriteArrayList<>();

    public CacheEntryEventManager<K, V> addCacheEntryCreatedListener(CacheEntryCreatedListener<K, V> cacheEntryCreatedListener) {
        cacheEntryCreatedListeners.add(cacheEntryCreatedListener);
        return this;
    }

    public CacheEntryEventManager<K, V> removeCacheEntryCreatedListener(CacheEntryCreatedListener<K, V> cacheEntryCreatedListener) {
        cacheEntryCreatedListeners.remove(cacheEntryCreatedListener);
        return this;
    }

    public CacheEntryEventManager<K, V> addCacheEntryExpiredListener(CacheEntryExpiredListener<K, V> cacheEntryExpiredListener) {
        cacheEntryExpiredListeners.add(cacheEntryExpiredListener);
        return this;
    }

    public CacheEntryEventManager<K, V> removeCacheEntryExpiredListener(CacheEntryExpiredListener<K, V> cacheEntryExpiredListener) {
        cacheEntryExpiredListeners.remove(cacheEntryExpiredListener);
        return this;
    }

    public CacheEntryEventManager<K, V> addCacheEntryRemovedListener(CacheEntryRemovedListener<K, V> cacheEntryRemovedListener) {
        cacheEntryRemovedListeners.add(cacheEntryRemovedListener);
        return this;
    }

    public CacheEntryEventManager<K, V> removeCacheEntryRemovedListener(CacheEntryRemovedListener<K, V> cacheEntryRemovedListener) {
        cacheEntryRemovedListeners.remove(cacheEntryRemovedListener);
        return this;
    }

    public CacheEntryEventManager<K, V> addCacheEntryUpdatedListener(CacheEntryUpdatedListener<K, V> cacheEntryUpdatedListener) {
        cacheEntryUpdatedListeners.add(cacheEntryUpdatedListener);
        return this;
    }

    public CacheEntryEventManager<K, V> removeCacheEntryUpdatedListener(CacheEntryUpdatedListener<K, V> cacheEntryUpdatedListener) {
        cacheEntryUpdatedListeners.remove(cacheEntryUpdatedListener);
        return this;
    }

    @Override
    public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
        for (CacheEntryCreatedListener<K, V> cacheEntryCreatedListener : cacheEntryCreatedListeners) {
            try {
                cacheEntryCreatedListener.onCreated(cacheEntryEvents);
            } catch (CacheEntryListenerException ignore) {
            }
        }
    }

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
        for (CacheEntryExpiredListener<K, V> cacheEntryExpiredListener : cacheEntryExpiredListeners) {
            try {
                cacheEntryExpiredListener.onExpired(cacheEntryEvents);
            } catch (CacheEntryListenerException ignore) {
            }
        }
    }

    @Override
    public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
        for (CacheEntryRemovedListener<K, V> cacheEntryRemovedListener : cacheEntryRemovedListeners) {
            try {
                cacheEntryRemovedListener.onRemoved(cacheEntryEvents);
            } catch (CacheEntryListenerException ignore) {
            }
        }
    }

    @Override
    public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
        for (CacheEntryUpdatedListener<K, V> cacheEntryUpdatedListener : cacheEntryUpdatedListeners) {
            try {
                cacheEntryUpdatedListener.onUpdated(cacheEntryEvents);
            } catch (CacheEntryListenerException ignore) {
            }
        }
    }
}
