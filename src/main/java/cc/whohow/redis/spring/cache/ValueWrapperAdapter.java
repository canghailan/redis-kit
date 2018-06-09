package cc.whohow.redis.spring.cache;

import cc.whohow.redis.jcache.ImmutableCacheValue;
import org.springframework.cache.Cache;

public class ValueWrapperAdapter extends ImmutableCacheValue<Object> implements Cache.ValueWrapper {
    private static final ValueWrapperAdapter NULL = new ValueWrapperAdapter(null);

    private ValueWrapperAdapter(Object value) {
        super(value);
    }

    public static ValueWrapperAdapter ofNullable(Object value) {
        return value == null ? NULL : new ValueWrapperAdapter(value);
    }
}
