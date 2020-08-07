package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;

public class IO {
    public static final int BUFFER_SIZE = 8 * 1024;
    public static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    /**
     * @see Files#read(java.io.InputStream, int)
     * @see Files#readAllBytes(java.nio.file.Path)
     */
    public static ByteBuffer read(InputStream input, int bufferSize) throws IOException {
        int capacity = bufferSize;
        byte[] buf = new byte[capacity];
        int nread = 0;
        int n;
        for (; ; ) {
            // read to EOF which may read more or less than initialSize (eg: file
            // is truncated while we are reading)
            while ((n = input.read(buf, nread, capacity - nread)) > 0)
                nread += n;

            // if last call to source.read() returned -1, we are done
            // otherwise, try to read one more byte; if that failed we're done too
            if (n < 0 || (n = input.read()) < 0)
                break;

            // one more byte was read; need to allocate a larger buffer
            if (capacity <= MAX_BUFFER_SIZE - capacity) {
                capacity = Math.max(capacity << 1, BUFFER_SIZE);
            } else {
                if (capacity == MAX_BUFFER_SIZE)
                    throw new OutOfMemoryError("Required array size too large");
                capacity = MAX_BUFFER_SIZE;
            }
            buf = Arrays.copyOf(buf, capacity);
            buf[nread++] = (byte) n;
        }
        return ByteBuffer.wrap(buf, 0, nread);
    }

    public static int write(OutputStream stream, ByteBuffer buffer) throws IOException {
        if (!buffer.hasRemaining()) {
            return 0;
        }
        int n = buffer.remaining();
        if (buffer.hasArray()) {
            stream.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            buffer.position(buffer.limit());
        } else {
            while (buffer.hasRemaining()) {
                stream.write(buffer.get());
            }
        }
        return n;
    }
}
