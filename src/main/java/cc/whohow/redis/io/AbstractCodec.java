package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public abstract class AbstractCodec<T> implements Codec<T> {
    /**
     * 获取新缓冲区大小
     */
    protected abstract int getBufferSize();

    @Override
    public ByteBuffer encode(T value) {
        try (ByteBufferOutputStream stream = new ByteBufferOutputStream(getBufferSize())) {
            encode(value, stream);
            ByteBuffer byteBuffer = stream.getByteBuffer();
            byteBuffer.flip();
            return byteBuffer;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
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

    @Override
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

    @Override
    public T decode(InputStream stream) throws IOException {
        return decode(new Java9InputStream(stream).readAllBytes(getBufferSize()));
    }
}
