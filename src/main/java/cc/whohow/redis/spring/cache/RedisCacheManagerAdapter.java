package cc.whohow.redis.spring.cache;

import cc.whohow.redis.jcache.RedisCacheManager;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import org.springframework.cache.Cache;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class RedisCacheManagerAdapter implements org.springframework.cache.CacheManager {
    private RedisCacheManager redisCacheManager;
    private Function<String, RedisCacheConfiguration<?, ?>> redisCacheConfigurationProvider;
    private Map<String, Cache> cacheAdapters = new ConcurrentHashMap<>();

    public RedisCacheManagerAdapter(RedisCacheManager redisCacheManager) {
        this.redisCacheManager = redisCacheManager;
    }

    public RedisCacheManagerAdapter(RedisCacheManager redisCacheManager,
                                    Function<String, RedisCacheConfiguration<?, ?>> redisCacheConfigurationProvider) {
        this.redisCacheManager = redisCacheManager;
        this.redisCacheConfigurationProvider = redisCacheConfigurationProvider;
    }

    @Override
    public Cache getCache(String name) {
        return cacheAdapters.computeIfAbsent(name, this::newCacheAdapter);
    }

    private Cache newCacheAdapter(String name) {
        if (redisCacheConfigurationProvider == null) {
            return new CacheAdapter(redisCacheManager.getCache(name));
        } else {
            return new CacheAdapter(redisCacheManager.resolveCache(redisCacheConfigurationProvider.apply(name)));
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
