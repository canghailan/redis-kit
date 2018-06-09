package cc.whohow.redis.io;

import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {
    protected ByteBuffer byteBuffer;

    public ByteBufferOutputStream(int size) {
        this.byteBuffer = ByteBuffer.allocate(size);
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    private void ensureRemaining(int minRemaining) {
        if (byteBuffer.remaining() < minRemaining) {
            int growth = byteBuffer.capacity();
            if (growth < minRemaining) {
                growth = minRemaining;
            }
            byteBuffer = copyOf(byteBuffer, byteBuffer.capacity() + growth);
        }
    }

    private ByteBuffer copyOf(ByteBuffer byteBuffer, int capacity) {
        byteBuffer.flip();
        return ByteBuffer.allocate(capacity).put(byteBuffer);
    }

    public void write(ByteBuffer b) {
        byteBuffer.put(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        ensureRemaining(len);
        byteBuffer.put(b, off, len);
    }

    @Override
    public void write(int b) {
        byteBuffer.put((byte) b);
    }
}
