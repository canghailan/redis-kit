package cc.whohow.redis.io;

import java.nio.ByteBuffer;

public class ByteBufferCodec implements Codec<ByteBuffer> {
    public static final ByteBufferCodec INSTANCE = new ByteBufferCodec();

    private ByteBufferCodec() {}

    @Override
    public ByteBuffer encode(ByteBuffer value) {
        return value;
    }

    @Override
    public ByteBuffer decode(ByteBuffer bytes) {
        return bytes;
    }
}
