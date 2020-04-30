package cc.whohow.redis.jcache.annotation;

import cc.whohow.redis.jcache.RedisCachingProvider;
import cc.whohow.redis.jcache.configuration.AnnotationRedisCacheConfiguration;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;

import javax.cache.Cache;
import javax.cache.annotation.CacheInvocationContext;
import javax.cache.annotation.CacheMethodDetails;
import javax.cache.annotation.CacheResolver;
import java.lang.annotation.Annotation;

@SuppressWarnings({"unchecked", "rawtypes"})
public class RedisCacheResolver implements CacheResolver {
    private final Cache cache;

    public RedisCacheResolver(CacheMethodDetails<? extends Annotation> cacheMethodDetails) {
        RedisCacheConfiguration redisCacheConfiguration = new AnnotationRedisCacheConfiguration(cacheMethodDetails);
        this.cache = RedisCachingProvider.getInstance().getCacheManager().createCache(
                redisCacheConfiguration.getName(), redisCacheConfiguration);
    }

    @Override
    public <K, V> Cache<K, V> resolveCache(CacheInvocationContext<? extends Annotation> cacheInvocationContext) {
        return cache;
    }
}
