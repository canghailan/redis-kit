package cc.whohow.redis.jcache;

import java.nio.ByteBuffer;
import java.util.function.Function;

public interface Cache<K, V> extends javax.cache.Cache<K, V> {
    CacheValue<V> getValue(K key, Function<V, ? extends CacheValue<V>> ofNullable);

    default CacheValue<V> getValue(K key) {
        return getValue(key, ImmutableCacheValue::ofNullable);
    }

    default V get(K key, Function<? super K, ? extends V> cacheLoader) {
        CacheValue<V> cacheValue = getValue(key);
        if (cacheValue != null) {
            return cacheValue.get();
        }
        V value = cacheLoader.apply(key);
        put(key, value);
        return value;
    }

    default void onRedisConnected() {
    }

    default void onRedisDisconnected() {
    }

    default void onKeyspaceNotification(ByteBuffer key, ByteBuffer message) {
    }
}
