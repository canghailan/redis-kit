package cc.whohow.redis.codec;

import cc.whohow.redis.io.ByteBufferInputStream;
import cc.whohow.redis.io.ByteBufferOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public abstract class AbstractStreamCodec<T> implements Codec<T> {
    private volatile int count = 0;
    private volatile int maxSize = 0;
    private volatile int averageSize = 0;

    protected int newByteBufferSize() {
        int size =  (averageSize + maxSize) / 2;
        return size == 0 ? 32 : size;
    }

    protected synchronized void updateStatistic(ByteBuffer byteBuffer) {
        int size = byteBuffer.remaining();

        count++;
        if (maxSize < size) {
            maxSize = size;
        }
        //   (averageSize *  n +       size)                / (n + 1)
        // = (averageSize * (n + 1) + (size - averageSize)) / (n + 1)
        // =  averageSize +           (size - averageSize)  / (n + 1)
        averageSize = averageSize + (int) (((double) (size - averageSize)) / count);
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
    public T decode(ByteBuffer bytes) {
        try {
            return bytes == null ? null : decode(new ByteBufferInputStream(bytes));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public abstract void encode(T value, OutputStream stream) throws IOException ;

    public abstract T decode(InputStream stream) throws IOException;
}
