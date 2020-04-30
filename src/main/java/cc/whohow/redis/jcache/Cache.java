package cc.whohow.redis.jcache;

import javax.cache.management.CacheStatisticsMXBean;
import java.util.function.Function;

/**
 * Redis缓存扩展
 */
public interface Cache<K, V> extends javax.cache.Cache<K, V> {
    /**
     * 读取/加载缓存值
     */
    V get(K key, Function<? super K, ? extends V> loader);

    /**
     * 读取缓存值，未命中返回null
     */
    CacheValue<V> getValue(K key);

    /**
     * 读取缓存值，未命中返回null
     */
    CacheValue<V> getValue(K key, Function<V, ? extends CacheValue<V>> factory);

    CacheStatisticsMXBean getCacheStatistics();
}
