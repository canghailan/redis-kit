package cc.whohow.redis.jcache;

import javax.cache.integration.CacheLoaderException;
import java.util.Objects;

/**
 * 多级缓存加载器
 */
public class TierCacheLoader<K, V> implements CacheLoader<K, V> {
    protected final Cache<K, V> cache;
    protected final CacheLoader<K, ? extends V> cacheLoader;

    public TierCacheLoader(Cache<K, V> cache, CacheLoader<K, ? extends V> cacheLoader) {
        Objects.requireNonNull(cache);
        Objects.requireNonNull(cacheLoader);
        this.cache = cache;
        this.cacheLoader = cacheLoader;
    }

    @Override
    public V load(K key) throws CacheLoaderException {
        return cache.get(key, cacheLoader);
    }
}
