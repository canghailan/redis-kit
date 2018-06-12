package cc.whohow.redis.jcache.annotation;

import cc.whohow.redis.jcache.ImmutableGeneratedCacheKey;

import javax.cache.annotation.CacheInvocationParameter;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.GeneratedCacheKey;
import java.lang.annotation.Annotation;
import java.util.Arrays;

/**
 * CacheKeyGenerator默认实现
 */
public class RedisCacheKeyGenerator implements CacheKeyGenerator {
    @Override
    public GeneratedCacheKey generateCacheKey(CacheKeyInvocationContext<? extends Annotation> cacheKeyInvocationContext) {
        CacheInvocationParameter[] keyParameters = cacheKeyInvocationContext.getKeyParameters();
        switch (keyParameters.length) {
            case 1: {
                return ImmutableGeneratedCacheKey.of(keyParameters[0].getValue());
            }
            case 0: {
                return ImmutableGeneratedCacheKey.empty();
            }
            default: {
                return ImmutableGeneratedCacheKey.of(
                        Arrays.stream(cacheKeyInvocationContext.getKeyParameters())
                                .map(CacheInvocationParameter::getValue)
                                .toArray());
            }
        }
    }
}
