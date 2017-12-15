package cc.whohow.redis.jcache.annotation;

import javax.cache.annotation.CacheInvocationParameter;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.GeneratedCacheKey;
import java.lang.annotation.Annotation;
import java.util.Arrays;

public class RedisCacheKeyGenerator implements CacheKeyGenerator {
    @Override
    public GeneratedCacheKey generateCacheKey(CacheKeyInvocationContext<? extends Annotation> cacheKeyInvocationContext) {
        return GeneratedKey.of(Arrays.stream(cacheKeyInvocationContext.getKeyParameters()).map(CacheInvocationParameter::getValue).toArray());
    }
}
