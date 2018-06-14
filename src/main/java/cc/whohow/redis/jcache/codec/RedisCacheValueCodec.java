package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.jcache.CacheValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.Function;

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

    public CacheValue<V> decodeCacheValue(ByteBuffer buffer, Function<V, ? extends CacheValue<V>> ofNullable) {
        if (buffer == null) {
            return null;
        }
        return ofNullable.apply(valueCodec.decode(buffer));
    }

    @Override
    public void encode(V value, OutputStream stream) throws IOException {
        valueCodec.encode(value, stream);
    }

    @Override
    public V decode(InputStream stream) throws IOException {
        if (stream == null) {
            return null;
        }
        return valueCodec.decode(stream);
    }
}
