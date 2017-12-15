package cc.whohow.redis.jcache.annotation;

import javax.cache.annotation.CacheMethodDetails;
import javax.cache.annotation.CacheResolver;
import javax.cache.annotation.CacheResolverFactory;
import javax.cache.annotation.CacheResult;
import java.lang.annotation.Annotation;

public class RedisCacheResolverFactory implements CacheResolverFactory {
    @Override
    public CacheResolver getCacheResolver(CacheMethodDetails<? extends Annotation> cacheMethodDetails) {
        return new RedisCacheResolver(cacheMethodDetails);
    }

    @Override
    public CacheResolver getExceptionCacheResolver(CacheMethodDetails<CacheResult> cacheMethodDetails) {
        throw new UnsupportedOperationException();
    }
}
