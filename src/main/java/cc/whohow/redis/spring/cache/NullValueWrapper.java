package cc.whohow.redis.spring.cache;

import org.springframework.cache.Cache;

public class NullValueWrapper implements Cache.ValueWrapper {
    public static final NullValueWrapper INSTANCE = new NullValueWrapper();

    @Override
    public Object get() {
        return null;
    }
}
