package cc.whohow.redis.jcache;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Redis缓存扩展
 */
public interface Cache<K, V> extends javax.cache.Cache<K, V> {
    /**
     * 读取缓存值
     */
    <T> T getValue(K key, Function<V, T> ofNullable);

    /**
     * 读取缓存值
     */
    default CacheValue<V> getValue(K key) {
        return getValue(key, ImmutableCacheValue::ofNullable);
    }

    /**
     * 读取/加载缓存值
     */
    default V get(K key, Function<? super K, ? extends V> cacheLoader) {
        CacheValue<V> cacheValue = getValue(key);
        if (cacheValue != null) {
            return cacheValue.get();
        }
        V value = cacheLoader.apply(key);
        put(key, value);
        return value;
    }

    /**
     * Redis连接成功回调
     */
    default void onRedisConnected() {
    }

    /**
     * Redis连接丢失回调
     */
    default void onRedisDisconnected() {
    }

    /**
     * Redis数据同步
     */
    default void onSynchronization() {
    }

    /**
     * Redis键通知回调
     */
    default void onKeyspaceNotification(ByteBuffer key, ByteBuffer message) {
    }
}
