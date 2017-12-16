package cc.whohow.redis.spring.cache;

import cc.whohow.redis.jcache.Cache;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * 仅支持Read Through
 */
@SuppressWarnings("unchecked")
public class TransactionCache extends CacheAdapter implements TransactionSynchronization {
    private boolean clear = false;
    private Map transactionCache = Collections.emptyMap();

    public TransactionCache(Cache cache) {
        super(cache);
        TransactionSynchronizationManager.registerSynchronization(this);
        TransactionSynchronizationManager.bindResource(TransactionCache.class, this);
    }

    public static TransactionCache get() {
        return (TransactionCache) TransactionSynchronizationManager.getResource(TransactionCache.class);
    }

    public static void remove() {
        TransactionSynchronizationManager.unbindResource(TransactionCache.class);
    }

    @Override
    public void afterCompletion(int status) {
        try {
            TransactionSynchronizationManager.unbindResource(TransactionCache.class);
        } finally {
            if (clear) {
                cache.clear();
            } else if (!transactionCache.isEmpty()) {
                cache.removeAll(transactionCache.keySet());
            }
        }
    }

    @Override
    public ValueWrapper get(Object key) {
        if (isDirtyKey(key)) {
            return null;
        }
        return super.get(key);
    }

    @Override
    public <T> T get(Object key, Class<T> type) {
        if (isDirtyKey(key)) {
            return null;
        }
        return super.get(key, type);
    }

    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        if (isDirtyKey(key)) {
            try {
                return valueLoader.call();
            } catch (Exception e) {
                throw new ValueRetrievalException(key, valueLoader, e);
            }
        }
        return super.get(key, valueLoader);
    }

    @Override
    public void put(Object key, Object value) {
        writable().put(key, value);
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        writable().put(key, null);
        return super.putIfAbsent(key, value);
    }

    @Override
    public void evict(Object key) {
        writable().put(key, null);
    }

    @Override
    public void clear() {
        clear = true;
    }

    public boolean isDirtyKey(Object key) {
        return clear || transactionCache.containsKey(key);
    }

    private Map writable() {
        if (transactionCache == Collections.emptyMap()) {
            transactionCache = new HashMap();
        }
        return transactionCache;
    }
}
