package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.jcache.CacheValue;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class RedisCacheCodec<K, V> implements RedisCodec<K, V> {
    private RedisCacheKeyCodec<K> cacheKeyCodec;
    private RedisCacheValueCodec<V> cacheValueCodec;

    public String getCacheName() {
        return cacheKeyCodec.getCacheName();
    }

    public String getSeparator() {
        return cacheKeyCodec.getSeparator();
    }

    @Override
    public K decodeKey(ByteBuffer bytes) {
        return cacheKeyCodec.decode(bytes);
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return cacheValueCodec.decode(bytes);
    }

    public CacheValue<V> decodeCacheValue(ByteBuffer bytes, Function<V, ? extends CacheValue<V>> ofNullable) {
        return cacheValueCodec.decodeCacheValue(bytes, ofNullable);
    }

    @Override
    public ByteBuffer encodeKey(K key) {
        return cacheKeyCodec.encode(key);
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        return cacheValueCodec.encode(value);
    }
}
