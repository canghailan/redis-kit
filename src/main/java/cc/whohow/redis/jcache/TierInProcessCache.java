package cc.whohow.redis.jcache;

import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import cc.whohow.redis.jcache.listener.CacheEntryEventListener;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
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
public class TierInProcessCache<K, V> extends InProcessCache<K, V>  {
    public TierInProcessCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration) {
        super(cacheManager, configuration);
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
}
