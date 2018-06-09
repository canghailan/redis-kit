package cc.whohow.redis.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public abstract class AbstractStreamCodec<T> implements Codec<T> {
    private volatile long count = 0;
    private volatile double avgSize = 0.0;
    private volatile int minSize = 0;
    private volatile int maxSize = 0;

    protected int newByteBufferSize() {
        int size = (int) (avgSize + maxSize) / 2;
        return size == 0 ? 32 : size;
    }

    protected synchronized void updateStatistic(ByteBuffer byteBuffer) {
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
            updateStatistic(byteBuffer);
            return byteBuffer;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public T decode(ByteBuffer buffer) {
        try {
            return buffer == null ? null : decode(new ByteBufferInputStream(buffer));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public abstract void encode(T value, ByteBufferOutputStream stream) throws IOException;

    public abstract T decode(ByteBufferInputStream stream) throws IOException;
}
