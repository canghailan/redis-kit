package cc.whohow.redis.bytes;

import java.nio.ByteBuffer;
import java.util.PrimitiveIterator;

public class ByteIterator implements PrimitiveIterator.OfInt {
    protected final ByteBuffer byteBuffer;

    public ByteIterator(byte... array) {
        this(array, 0, array.length);
    }

    public ByteIterator(byte[] array, int offset, int length) {
        this.byteBuffer = ByteBuffer.wrap(array, offset, length);
    }

    public ByteIterator(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer.duplicate();
    }

    @Override
    public int nextInt() {
        return byteBuffer.get() & 0xff;
    }

    @Override
    public boolean hasNext() {
        return byteBuffer.hasRemaining();
    }
}
