package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public abstract class AbstractCodec<T> implements Codec<T> {
    private volatile long count = 0;
    private volatile double avgSize = 0.0;
    private volatile int minSize = 0;
    private volatile int maxSize = 0;

    protected int newByteBufferSize() {
        int size = (int) (avgSize + maxSize) / 2;
        return size == 0 ? 32 : size;
    }

    protected synchronized void record(ByteBuffer byteBuffer) {
        int size = byteBuffer.remaining();

        count++;
        if (minSize > size) {
            minSize = size;
        }
        if (maxSize < size) {
            maxSize = size;
        }
        //   (avgSize *  n +       size           ) / (n + 1)
        // = (avgSize * (n + 1) + (size - avgSize)) / (n + 1)
        // =  avgSize +           (size - avgSize ) / (n + 1)
        avgSize = avgSize + (size - avgSize) / count;
    }

    @Override
    public ByteBuffer encode(T value) {
        try (ByteBufferOutputStream stream = new ByteBufferOutputStream(newByteBufferSize())) {
            encode(value, stream);
            ByteBuffer byteBuffer = stream.getByteBuffer();
            byteBuffer.flip();
            record(byteBuffer);
            return byteBuffer;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public T decode(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            return decode(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void encode(T value, OutputStream stream) throws IOException {
        ByteBuffer buffer = encode(value);
        if (buffer.hasArray()) {
            if (buffer.hasRemaining()) {
                stream.write(buffer.array(), buffer.arrayOffset(), buffer.remaining());
            }
        } else {
            while (buffer.hasRemaining()) {
                stream.write(buffer.get());
            }
        }
    }

    public T decode(InputStream stream) throws IOException {
        return decode(new Java9InputStream(stream).readAllBytes(newByteBufferSize()));
    }
}
