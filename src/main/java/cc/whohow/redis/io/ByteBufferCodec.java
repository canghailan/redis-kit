package cc.whohow.redis.io;

import java.nio.ByteBuffer;

public class ByteBufferCodec extends AbstractCodec<ByteBuffer> {
    public ByteBufferCodec() {
    }

    @Override
    public ByteBuffer encode(ByteBuffer value) {
        record(value);
        return value;
    }

    @Override
    public ByteBuffer decode(ByteBuffer buffer) {
        return buffer;
    }
}
