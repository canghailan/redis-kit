package cc.whohow.redis.spring.cache;

import cc.whohow.redis.jcache.RedisCacheManager;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import org.springframework.cache.Cache;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unchecked")
public class RedisCacheManagerAdapter implements org.springframework.cache.CacheManager {
    private RedisCacheManager redisCacheManager;
    private Map<String, Cache> cacheAdapters = new ConcurrentHashMap<>();

    public RedisCacheManagerAdapter(RedisCacheManager redisCacheManager) {
        this.redisCacheManager = redisCacheManager;
    }

    @Override
    public Cache getCache(String name) {
        return cacheAdapters.computeIfAbsent(name, this::newCacheAdapter);
    }

    private Cache newCacheAdapter(String name) {
        cc.whohow.redis.jcache.Cache cache = redisCacheManager.getCache(name);
        RedisCacheConfiguration configuration = (RedisCacheConfiguration)
                cache.getConfiguration(RedisCacheConfiguration.class);
        if (configuration.getExConfigurations().contains("TransactionAware")) {
            return new TransactionAwareCacheAdapter(cache);
        } else {
            return new CacheAdapter(cache);
        }
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
