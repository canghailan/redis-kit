package cc.whohow.redis.spring.cache;

import org.springframework.cache.Cache;

import java.util.Optional;

public class ValueWrapperAdapter implements Cache.ValueWrapper {
    private final Optional optional;

    public ValueWrapperAdapter(Optional optional) {
        this.optional = optional;
    }

    @Override
    public Object get() {
        return optional.orElse(null);
    }
}
