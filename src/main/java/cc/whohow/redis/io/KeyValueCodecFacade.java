package cc.whohow.redis.io;

import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;

public class KeyValueCodecFacade<K, V> implements KeyValueCodec<K, V>, RedisCodec<K, V> {
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;

    public KeyValueCodecFacade(Codec<K> keyCodec, Codec<V> valueCodec) {
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    @Override
    public K decodeKey(ByteBuffer bytes) {
        return keyCodec.decode(bytes);
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return valueCodec.decode(bytes);
    }

    @Override
    public ByteBuffer encodeKey(K key) {
        return keyCodec.encode(key);
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        return valueCodec.encode(value);
    }
}
