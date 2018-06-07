package cc.whohow.redis.io;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
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
        return byteBuffer.get();
    }
}
