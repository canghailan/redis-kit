package cc.whohow.redis.spring.cache;

import cc.whohow.redis.jcache.Cache;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.concurrent.Callable;

public class TransactionAwareCacheAdapter extends CacheAdapter {
    public TransactionAwareCacheAdapter(Cache cache) {
        super(cache);
    }

    private TransactionCache getTransactionCache() {
        TransactionCache transactionCache = TransactionCache.get();
        //noinspection ConstantConditions
        if (transactionCache == null) {
            transactionCache = new TransactionCache(cache);
        } else if (transactionCache.cache != cache) {
            TransactionCache.remove();
            transactionCache = new TransactionCache(cache);
        }
        return transactionCache;
    }

    @Override
    public ValueWrapper get(Object key) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            return getTransactionCache().get(key);
        } else {
            return super.get(key);
        }
    }

    @Override
    public <T> T get(Object key, Class<T> type) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            return getTransactionCache().get(key, type);
        } else {
            return super.get(key, type);
        }
    }

    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            return getTransactionCache().get(key, valueLoader);
        } else {
            return super.get(key, valueLoader);
        }
    }

    @Override
    public void put(Object key, Object value) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            getTransactionCache().put(key, value);
        } else {
            super.put(key, value);
        }
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            return getTransactionCache().putIfAbsent(key, value);
        } else {
            return super.putIfAbsent(key, value);
        }
    }

    @Override
    public void evict(Object key) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            getTransactionCache().evict(key);
        } else {
            super.evict(key);
        }
    }

    @Override
    public void clear() {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            getTransactionCache().clear();
        } else {
            super.clear();
        }
    }
}
