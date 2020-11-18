package cc.whohow.redis.jcache;

import java.util.Objects;

/**
 * 缓存值（不可变）
 */
public class ImmutableCacheValue<V> implements CacheValue<V> {
    private static final ImmutableCacheValue<?> EMPTY = new ImmutableCacheValue<>(null);
    protected final V value;

    public ImmutableCacheValue(V value) {
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    public static <T> ImmutableCacheValue<T> empty() {
        return (ImmutableCacheValue<T>) EMPTY;
    }

    @Override
    public V get() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ImmutableCacheValue) {
            ImmutableCacheValue<?> that = (ImmutableCacheValue<?>) o;
            return Objects.equals(that.value, this.value);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toString(value);
    }
}
