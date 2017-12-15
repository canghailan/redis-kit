package cc.whohow.redis.jcache.annotation;

import cc.whohow.redis.jcache.RedisCachingProvider;
import cc.whohow.redis.jcache.configuration.AnnotationRedisCacheConfiguration;

import javax.cache.Cache;
import javax.cache.annotation.CacheInvocationContext;
import javax.cache.annotation.CacheMethodDetails;
import javax.cache.annotation.CacheResolver;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class RedisCacheResolver implements CacheResolver {
    private final CacheMethodDetails<? extends Annotation> cacheMethodDetails;

    public RedisCacheResolver(CacheMethodDetails<? extends Annotation> cacheMethodDetails) {
        this.cacheMethodDetails = cacheMethodDetails;
    }

    @Override
    public <K, V> Cache<K, V> resolveCache(CacheInvocationContext<? extends Annotation> cacheInvocationContext) {
        Method method = cacheInvocationContext.getMethod();
        RedisCacheDefaults redisCacheDefaults = method.getAnnotation(RedisCacheDefaults.class);
        if (redisCacheDefaults == null) {
            throw new IllegalStateException();
        }
        return RedisCachingProvider.getInstance().getCacheManager().resolveCache(
                new AnnotationRedisCacheConfiguration(cacheMethodDetails.getMethod(), redisCacheDefaults));
    }
}
