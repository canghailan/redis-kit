package cc.whohow.redis.jcache;

import javax.cache.management.CacheStatisticsMXBean;

/**
 * Redis缓存扩展
 */
public interface Cache<K, V> extends javax.cache.Cache<K, V> {
    static <K, V> V loadNull(K key) {
        return null;
    }

    /**
     * 读取/加载缓存值
     */
    default V get(K key, CacheLoader<K, ? extends V> cacheLoader) {
        CacheValue<V> cacheValue = getValue(key, cacheLoader);
        return cacheValue == null ? null : cacheValue.get();
    }

    /**
     * 读取/加载缓存值
     */
    CacheValue<V> getValue(K key, CacheLoader<K, ? extends V> cacheLoader);

    /**
     * 读取缓存值，未命中返回null
     */
    default CacheValue<V> getValue(K key) {
        return getValue(key, Cache::loadNull);
    }

    CacheStatisticsMXBean getCacheStatistics();
}
