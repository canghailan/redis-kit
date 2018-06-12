package cc.whohow.redis.spring.cache;

import org.springframework.cache.Cache;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class ValueLoaderAdapter<K, V> implements Function<K, V> {
    private final Object key;
    private final Callable<?> valueLoader;

    public ValueLoaderAdapter(Object key, Callable<?> valueLoader) {
        this.key = key;
        this.valueLoader = valueLoader;
    }

    @Override
    public V apply(K k) {
        try {
            if (Objects.equals(k, key)) {
                return (V) valueLoader.call();
            }
        } catch (Exception e) {
            throw new Cache.ValueRetrievalException(key, valueLoader, e);
        }
        throw new IllegalStateException();
    }
}
