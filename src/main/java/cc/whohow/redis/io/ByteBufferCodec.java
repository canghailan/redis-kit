package cc.whohow.redis.io;

import java.nio.ByteBuffer;

public class ByteBufferCodec extends AbstractCodec<ByteBuffer> {
    public ByteBufferCodec() {
    }

    @Override
    public ByteBuffer encodeToByteBuffer(ByteBuffer value) {
        return value;
    }

    @Override
    public ByteBuffer decodeByteBuffer(ByteBuffer buffer) {
        return buffer;
    }
}
