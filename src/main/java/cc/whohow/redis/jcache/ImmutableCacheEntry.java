package cc.whohow.redis.jcache;

import java.util.Objects;

/**
 * 缓存键值对（不可变）
 */
public class ImmutableCacheEntry<K, V> implements Cache.Entry<K, V> {
    private final K key;
    private final V value;

    public ImmutableCacheEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ImmutableCacheEntry) {
            ImmutableCacheEntry<?, ?> that = (ImmutableCacheEntry<?, ?>) o;
            return Objects.equals(that.key, this.key) && Objects.equals(that.value, this.value);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key) * 31 + Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return key + ":" + value;
    }
}
