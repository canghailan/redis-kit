package cc.whohow.redis.spring.cache;

import cc.whohow.redis.jcache.RedisCacheManager;

import java.util.Collection;

public class RedisCacheManagerAdapter implements org.springframework.cache.CacheManager {
    private final RedisCacheManager redisCacheManager;

    public RedisCacheManagerAdapter(RedisCacheManager redisCacheManager) {
        this.redisCacheManager = redisCacheManager;
    }

    @Override
    public org.springframework.cache.Cache getCache(String name) {
        return new CacheAdapter(redisCacheManager.getCache(name));
    }

    @Override
    public Collection<String> getCacheNames() {
        return redisCacheManager.getCacheNames();
    }
}
