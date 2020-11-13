package cc.whohow.redis.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ByteBufferOutputStream extends ByteArrayOutputStream implements WritableByteChannel {
    public ByteBufferOutputStream() {
        super();
    }

    public ByteBufferOutputStream(int size) {
        super(size);
    }

    public ByteBuffer getByteBuffer() {
        return ByteBuffer.wrap(buf, 0, count);
    }

    public synchronized int position() {
        return count;
    }

    public synchronized int write(ByteBuffer b) throws IOException {
        return IO.write(this, b);
    }

    @Override
    public boolean isOpen() {
        return true;
    }
}
