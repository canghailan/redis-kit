package cc.whohow.redis.io;

import java.nio.ByteBuffer;

public class ByteBufferCodec implements Codec<ByteBuffer> {
    @Override
    public ByteBuffer encode(ByteBuffer value) {
        return value;
    }

    @Override
    public ByteBuffer decode(ByteBuffer buffer) {
        return buffer;
    }
}
