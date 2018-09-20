package cc.whohow.redis.spring.cache;

import cc.whohow.redis.jcache.ImmutableCacheValue;
import org.springframework.cache.Cache;

public class CacheValueWrapper<V> extends ImmutableCacheValue<V> implements Cache.ValueWrapper {
    protected CacheValueWrapper(V value) {
        super(value);
    }
}
