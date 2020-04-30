package cc.whohow.redis.jcache;

import javax.cache.Cache;

public class LazyCacheEntry<K, V> implements Cache.Entry<K, V> {
    private final Cache<K, V> cache;
    private final K key;

    public LazyCacheEntry(Cache<K, V> cache, K key) {
        this.cache = cache;
        this.key = key;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return cache.get(key);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException();
    }
}
