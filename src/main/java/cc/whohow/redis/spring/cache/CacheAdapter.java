package cc.whohow.redis.spring.cache;

import cc.whohow.redis.jcache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import java.util.Optional;
import java.util.concurrent.Callable;

@SuppressWarnings("unchecked")
public class CacheAdapter implements org.springframework.cache.Cache {
    protected final Cache cache;

    public CacheAdapter(Cache cache) {
        this.cache = cache;
    }

    @Override
    public String getName() {
        return cache.getName();
    }

    @Override
    public Object getNativeCache() {
        return cache;
    }

    @Override
    public ValueWrapper get(Object key) {
        Object value = cache.get(key);
        if (value != null) {
            return new SimpleValueWrapper(value);
        }
        return cache.containsKey(key) ? NullValueWrapper.INSTANCE : null;
    }

    @Override
    public <T> T get(Object key, Class<T> type) {
        return (T) cache.get(key);
    }

    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        return (T) cache.get(key, new ValueLoaderAdapter(key, valueLoader));
    }

    @Override
    public void put(Object key, Object value) {
        cache.put(key, value);
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void evict(Object key) {
        cache.remove(key);
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public String toString() {
        return cache.toString();
    }
}
