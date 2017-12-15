package cc.whohow.redis.jcache.processor;

import javax.cache.Cache;
import javax.cache.processor.MutableEntry;

public class CacheMutableEntry<K, V> implements MutableEntry<K, V> {
    protected final Cache<K, V> cache;
    protected final K key;

    public CacheMutableEntry(Cache<K, V> cache, K key) {
        this.cache = cache;
        this.key = key;
    }

    @Override
    public boolean exists() {
        return cache.containsKey(key);
    }

    @Override
    public void remove() {
        cache.remove(key);
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
    public void setValue(V value) {
        cache.put(key, value);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException();
    }
}
