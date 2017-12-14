package cc.whohow.redis.jcache;

import cc.whohow.redis.Redis;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import org.redisson.client.protocol.RedisCommands;

import java.util.Map;
import java.util.Set;

/**
 * 支持键通知的缓存
 * TODO 使用pipeline优化
 */
public class RedisKeyNotificationCache<K, V> extends RedisCache<K, V> {
    public RedisKeyNotificationCache(RedisCacheManager cacheManager, RedisCacheConfiguration<K, V> configuration, Redis redis) {
        super(cacheManager, configuration, redis);
    }

    @Override
    public void put(K key, V value) {
        super.put(key, value);
    }

    @Override
    public V getAndPut(K key, V value) {
        return super.getAndPut(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        super.putAll(map);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return super.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(K key) {
        return super.remove(key);
    }

    @Override
    public boolean remove(K key, V oldValue) {
        return super.remove(key, oldValue);
    }

    @Override
    public V getAndRemove(K key) {
        return super.getAndRemove(key);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return super.replace(key, oldValue, newValue);
    }

    @Override
    public boolean replace(K key, V value) {
        return super.replace(key, value);
    }

    @Override
    public V getAndReplace(K key, V value) {
        return super.getAndReplace(key, value);
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        super.removeAll(keys);
    }

    @Override
    public void clear() {
        super.clear();
    }

    @Override
    public void removeAll() {
        super.removeAll();
        redis.execute(RedisCommands.PUBLISH, name, "*");
    }
}
