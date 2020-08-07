package cc.whohow.redis.jcache;

import javax.cache.integration.CacheLoaderException;
import java.util.Map;

@FunctionalInterface
public interface CacheLoader<K, V> extends javax.cache.integration.CacheLoader<K, V> {
    @Override
    default Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
        throw new UnsupportedOperationException();
    }
}
