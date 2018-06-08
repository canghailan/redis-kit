package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.Codec;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;

public class RedisCacheCodec<K, V> implements RedisCodec<K, V> {
    private final RedisCacheKeyCodec<K> redisCacheKeyCodec;
    private final Codec<V> valueCodec;

    public RedisCacheCodec(RedisCacheKeyCodec<K> redisCacheKeyCodec, Codec<V> valueCodec) {
        this.redisCacheKeyCodec = redisCacheKeyCodec;
        this.valueCodec = valueCodec;
    }

    public String getCacheName() {
        return redisCacheKeyCodec.getCacheName();
    }

    public String getSeparator() {
        return redisCacheKeyCodec.getSeparator();
    }

    public ByteBuffer getKeyPrefix() {
        return redisCacheKeyCodec.getKeyPrefix();
    }

    @Override
    public K decodeKey(ByteBuffer bytes) {
        return redisCacheKeyCodec.decode(bytes);
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return valueCodec.decode(bytes);
    }

    @Override
    public ByteBuffer encodeKey(K key) {
        return redisCacheKeyCodec.encode(key);
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        return valueCodec.encode(value);
    }
}
