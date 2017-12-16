package cc.whohow.redis.jcache;

import java.util.function.Function;

public interface Cache<K, V> extends javax.cache.Cache<K, V> {
    V get(K key, Function<? super K, ? extends V> cacheLoader);
}
