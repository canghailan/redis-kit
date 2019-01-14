package cc.whohow.redis.io;

import java.nio.ByteBuffer;

public class ByteBufferCodec extends AbstractBufferCodec<ByteBuffer> {
    public ByteBufferCodec() {
        this(new BufferAllocationPredictor(16, 256));
    }

    public ByteBufferCodec(BufferAllocationPredictor predictor) {
        super(predictor);
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
