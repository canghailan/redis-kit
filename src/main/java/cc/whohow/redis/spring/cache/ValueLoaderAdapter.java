package cc.whohow.redis.spring.cache;

import cc.whohow.redis.jcache.CacheLoader;
import org.springframework.cache.Cache;

import javax.cache.integration.CacheLoaderException;
import java.util.Objects;
import java.util.concurrent.Callable;

@SuppressWarnings("unchecked")
public class ValueLoaderAdapter<K, V> implements CacheLoader<K, V> {
    private final Object key;
    private final Callable<?> valueLoader;

    public ValueLoaderAdapter(Object key, Callable<?> valueLoader) {
        this.key = key;
        this.valueLoader = valueLoader;
    }

    @Override
    public V load(K key) throws CacheLoaderException {
        try {
            if (Objects.equals(this.key, key)) {
                return (V) valueLoader.call();
            }
        } catch (Exception e) {
            throw new Cache.ValueRetrievalException(key, valueLoader, e);
        }
        throw new IllegalStateException();
    }
}
