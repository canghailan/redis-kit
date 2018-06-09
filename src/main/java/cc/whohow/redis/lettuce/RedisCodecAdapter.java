package cc.whohow.redis.lettuce;

import cc.whohow.redis.io.Codec;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;

public class RedisCodecAdapter<K, V> implements RedisCodec<K, V> {
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;

    public RedisCodecAdapter(Codec<K> keyCodec, Codec<V> valueCodec) {
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    public Codec<K> getKeyCodec() {
        return keyCodec;
    }

    public Codec<V> getValueCodec() {
        return valueCodec;
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
