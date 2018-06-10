package cc.whohow.redis.jcache;

import java.util.Objects;

public class ImmutableCacheValue<V> implements CacheValue<V> {
    private static final ImmutableCacheValue NULL = new ImmutableCacheValue<>(null);
    protected final V value;

    protected ImmutableCacheValue(V value) {
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ImmutableCacheValue<V> ofNullable(V value) {
        return value == null ? NULL : new ImmutableCacheValue<>(value);
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
