package cc.whohow.redis.jcache;

import java.util.function.Supplier;

public interface CacheValue<V> extends Supplier<V> {
    int hashCode();

    boolean equals(Object o);
}
