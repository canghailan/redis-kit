package cc.whohow.redis.io;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class ByteBufferInputStream extends InputStream implements ReadableByteChannel {
    private final ByteBuffer byteBuffer;

    public ByteBufferInputStream(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) {
        if (byteBuffer.hasRemaining()) {
            int bytes = Integer.min(len, byteBuffer.remaining());
            byteBuffer.get(b, off, bytes);
            return bytes;
        } else {
            return -1;
        }
    }

    @Override
    public synchronized long skip(long n) {
        int bytes = (int) Long.min(n, byteBuffer.remaining());
        byteBuffer.position(byteBuffer.position() + bytes);
        return bytes;
    }

    @Override
    public synchronized int available() {
        return byteBuffer.remaining();
    }

    @Override
    public synchronized void mark(int limit) {
        byteBuffer.mark();
    }

    @Override
    public synchronized void reset() {
        byteBuffer.reset();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    /**
     * @see java.io.ByteArrayInputStream#read()
     */
    @Override
    public synchronized int read() {
        return byteBuffer.hasRemaining() ? byteBuffer.get() & 0xff : -1;
    }

    @Override
    public synchronized int read(ByteBuffer dst) {
        if (byteBuffer.hasRemaining()) {
            int bytes = Integer.min(dst.remaining(), byteBuffer.remaining());
            ByteBuffer src = byteBuffer.duplicate();
            src.limit(src.position() + bytes);
            dst.put(src);
            byteBuffer.position(byteBuffer.position() + bytes);
            return bytes;
        } else {
            return -1;
        }
    }

    @Override
    public boolean isOpen() {
        return true;
    }
}
