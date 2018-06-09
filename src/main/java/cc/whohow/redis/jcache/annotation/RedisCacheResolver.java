package cc.whohow.redis.jcache.annotation;

import cc.whohow.redis.jcache.RedisCachingProvider;
import cc.whohow.redis.jcache.configuration.AnnotationRedisCacheConfiguration;
import cc.whohow.redis.jcache.configuration.MutableRedisCacheConfiguration;

import javax.cache.Cache;
import javax.cache.annotation.CacheInvocationContext;
import javax.cache.annotation.CacheMethodDetails;
import javax.cache.annotation.CacheResolver;
import java.lang.annotation.Annotation;

@SuppressWarnings("unchecked")
public class RedisCacheResolver implements CacheResolver {
    private final Cache cache;

    public RedisCacheResolver(CacheMethodDetails<? extends Annotation> cacheMethodDetails) {
        this.cache = RedisCachingProvider.getInstance().getCacheManager().resolveCache(
                new MutableRedisCacheConfiguration(new AnnotationRedisCacheConfiguration(cacheMethodDetails)));
    }

    @Override
    public <K, V> Cache<K, V> resolveCache(CacheInvocationContext<? extends Annotation> cacheInvocationContext) {
        return cache;
    }
}
