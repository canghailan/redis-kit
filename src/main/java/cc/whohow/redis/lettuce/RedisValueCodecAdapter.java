package cc.whohow.redis.lettuce;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;

public class RedisValueCodecAdapter<V> implements RedisCodec<ByteBuffer, V> {
    private final Codec<V> valueCodec;

    public RedisValueCodecAdapter(Codec<V> valueCodec) {
        this.valueCodec = valueCodec;
    }

    public Codec<V> getValueCodec() {
        return valueCodec;
    }

    @Override
    public ByteBuffer decodeKey(ByteBuffer bytes) {
        return ByteBuffers.copy(bytes);
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return valueCodec.decode(bytes);
    }

    @Override
    public ByteBuffer encodeKey(ByteBuffer key) {
        return key;
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        return valueCodec.encode(value);
    }
}