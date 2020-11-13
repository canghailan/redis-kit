package cc.whohow.redis.io;

import cc.whohow.redis.buffer.ByteSequence;

import java.nio.ByteBuffer;

public class ByteBufferCodec implements Codec<ByteBuffer> {
    private static final ByteBufferCodec INSTANCE = new ByteBufferCodec();

    public static ByteBufferCodec get() {
        return INSTANCE;
    }

    @Override
    public ByteSequence encode(ByteBuffer value) {
        return ByteSequence.of(value);
    }

    @Override
    public ByteBuffer decode(ByteSequence buffer) {
        return buffer.toByteBuffer();
    }

    @Override
    public ByteBuffer decode(byte... buffer) {
        return ByteBuffer.wrap(buffer);
    }

    @Override
    public ByteBuffer decode(ByteBuffer buffer) {
        return buffer;
    }
}
