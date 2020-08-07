package cc.whohow.redis.io;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ByteBufferOutputStream extends OutputStream implements WritableByteChannel {
    protected ByteBuffer byteBuffer;

    public ByteBufferOutputStream() {
        this(32);
    }

    public ByteBufferOutputStream(int size) {
        this(ByteBuffer.allocate(size));
    }

    public ByteBufferOutputStream(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public synchronized int position() {
        return byteBuffer.position();
    }

    protected synchronized void ensureCapacity(int minCapacity) {
        if (byteBuffer.capacity() < minCapacity) {
            int newCapacity = Integer.max(minCapacity, byteBuffer.capacity() * 2);
            byteBuffer = ByteBuffers.resize(byteBuffer, newCapacity);
        }
    }

    public synchronized int write(ByteBuffer b) {
        int bytes = b.remaining();
        ensureCapacity(position() + bytes);
        byteBuffer.put(b);
        return bytes;
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) {
        ensureCapacity(position() + len);
        byteBuffer.put(b, off, len);
    }

    @Override
    public synchronized void write(int b) {
        ensureCapacity(position() + 1);
        byteBuffer.put((byte) b);
    }

    @Override
    public boolean isOpen() {
        return true;
    }
}
