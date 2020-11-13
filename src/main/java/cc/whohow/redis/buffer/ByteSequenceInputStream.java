package cc.whohow.redis.buffer;

import java.io.IOException;
import java.io.InputStream;

public class ByteSequenceInputStream extends InputStream {
    protected final ByteSequence byteSequence;
    protected final int length;
    protected int readIndex;

    public ByteSequenceInputStream(ByteSequence byteSequence) {
        this.byteSequence = byteSequence;
        this.length = byteSequence.length();
        this.readIndex = 0;
    }

    @Override
    public int read() throws IOException {
        return (readIndex < length) ? (byteSequence.get(readIndex) & 0xff) : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (readIndex >= length) {
            return -1;
        }
        int n = Integer.min(len, available());
        byteSequence.get(readIndex, b, off, n);
        readIndex += n;
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        int skipped = Integer.min((int) n, available());
        readIndex += skipped;
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return length - readIndex;
    }
}
