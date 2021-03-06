package cc.whohow.redis.spring.cache;

import cc.whohow.redis.jcache.RedisCacheManager;
import org.springframework.cache.Cache;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisCacheManagerAdapter implements org.springframework.cache.CacheManager {
    private final RedisCacheManager redisCacheManager;
    private final Map<String, Cache> cacheAdapters = new ConcurrentHashMap<>();

    public RedisCacheManagerAdapter(RedisCacheManager redisCacheManager) {
        this.redisCacheManager = redisCacheManager;
    }

    @Override
    public Cache getCache(String name) {
        return cacheAdapters.computeIfAbsent(name, this::newCacheAdapter);
    }

    private Cache newCacheAdapter(String name) {
        return new CacheAdapter(redisCacheManager.getCache(name));
    }

    @Override
    public Collection<String> getCacheNames() {
        return redisCacheManager.getCacheNames();
    }

    @Override
    public String toString() {
        return redisCacheManager.toString();
    }
}
