package cc.whohow.redis.io;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class ByteBufferInputStream extends InputStream implements ReadableByteChannel {
    private ByteBuffer byteBuffer;

    public ByteBufferInputStream(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (byteBuffer.hasRemaining()) {
            if (len > byteBuffer.remaining()) {
                len = byteBuffer.remaining();
            }
            byteBuffer.get(b, off, len);
            return len;
        }
        return -1;
    }

    @Override
    public long skip(long n) {
        if (n > byteBuffer.remaining()) {
            n = byteBuffer.remaining();
        }
        byteBuffer.position(byteBuffer.position() + (int) n);
        return n;
    }

    @Override
    public int available() {
        return byteBuffer.remaining();
    }

    @Override
    public void mark(int readlimit) {
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

    @Override
    public int read() {
        return byteBuffer.hasRemaining() ? byteBuffer.get() : -1;
    }

    @Override
    public int read(ByteBuffer dst) {
        if(byteBuffer.hasRemaining()) {
            int n = dst.remaining();
            if (n < byteBuffer.remaining()) {
                dst.put(byteBuffer.duplicate().limit(byteBuffer.position() + n));
                byteBuffer.position(byteBuffer.position() + n);
            } else {
                n = byteBuffer.remaining();
                dst.put(byteBuffer);
            }
            return n;
        }
        return -1;
    }

    @Override
    public boolean isOpen() {
        return true;
    }
}
