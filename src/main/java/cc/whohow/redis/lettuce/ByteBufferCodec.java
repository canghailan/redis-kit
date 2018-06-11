package cc.whohow.redis.lettuce;

import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;

public class ByteBufferCodec implements RedisCodec<ByteBuffer, ByteBuffer> {
    public static final ByteBufferCodec INSTANCE = new ByteBufferCodec();

    @Override
    public ByteBuffer decodeKey(ByteBuffer bytes) {
        return bytes;
    }

    @Override
    public ByteBuffer decodeValue(ByteBuffer bytes) {
        return bytes;
    }

    @Override
    public ByteBuffer encodeKey(ByteBuffer key) {
        return key;
    }

    @Override
    public ByteBuffer encodeValue(ByteBuffer value) {
        return value;
    }
}