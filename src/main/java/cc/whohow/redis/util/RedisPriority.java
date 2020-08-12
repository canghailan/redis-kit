package cc.whohow.redis.util;

import java.util.AbstractMap;

public class RedisPriority<E> extends AbstractMap.SimpleImmutableEntry<E, Number> {
    public RedisPriority(E key, Number value) {
        super(key, value);
    }
}
