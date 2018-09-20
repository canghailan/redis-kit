package cc.whohow.redis.io;

import java.nio.ByteBuffer;

public class ByteBufferCodec extends AbstractBufferCodec<ByteBuffer> {
    public ByteBufferCodec() {
        super(new BufferAllocationPredictor());
    }

    @Override
    public ByteBuffer encode(ByteBuffer value) {
        return value;
    }

    @Override
    public ByteBuffer decode(ByteBuffer buffer) {
        return buffer;
    }
}
