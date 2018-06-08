package cc.whohow.redis.jcache.annotation;

import cc.whohow.redis.jcache.ImmutableGeneratedCacheKey;

import javax.cache.annotation.CacheInvocationParameter;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.GeneratedCacheKey;
import java.lang.annotation.Annotation;
import java.util.Arrays;

public class RedisCacheKeyGenerator implements CacheKeyGenerator {
    @Override
    public GeneratedCacheKey generateCacheKey(CacheKeyInvocationContext<? extends Annotation> cacheKeyInvocationContext) {
        CacheInvocationParameter[] keyParameters = cacheKeyInvocationContext.getKeyParameters();
        if (keyParameters.length == 1) {
            return ImmutableGeneratedCacheKey.of(keyParameters[0].getValue());
        } else if (keyParameters.length == 0) {
            return ImmutableGeneratedCacheKey.empty();
        } else {
            return ImmutableGeneratedCacheKey.of(
                    Arrays.stream(cacheKeyInvocationContext.getKeyParameters())
                            .map(CacheInvocationParameter::getValue)
                            .toArray());
        }
    }
}
