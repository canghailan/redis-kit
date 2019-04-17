package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.Codec;

import java.nio.ByteBuffer;

public class RedisCacheValueCodec<V> implements Codec<V> {
    private final Codec<V> valueCodec;

    public RedisCacheValueCodec(Codec<V> valueCodec) {
        this.valueCodec = valueCodec;
    }

    @Override
    public ByteBuffer encode(V value) {
        return valueCodec.encode(value);
    }

    @Override
    public V decode(ByteBuffer buffer) {
        return valueCodec.decode(buffer);
    }
}
