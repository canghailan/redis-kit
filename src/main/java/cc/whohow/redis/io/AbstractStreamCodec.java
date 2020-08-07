package cc.whohow.redis.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/**
 * 自适应缓冲区编码器
 */
public abstract class AbstractStreamCodec<T> implements Codec<T>, StreamCodec<T> {
    protected final ByteBufferAllocator byteBufferAllocator;

    protected AbstractStreamCodec(ByteBufferAllocator byteBufferAllocator) {
        this.byteBufferAllocator = byteBufferAllocator;
    }

    @Override
    public ByteBuffer encode(T value) {
        try (ByteBufferOutputStream stream = new ByteBufferOutputStream(byteBufferAllocator.allocate())) {
            encode(value, stream);
            ByteBuffer byteBuffer = stream.getByteBuffer();
            byteBuffer.flip();
            byteBufferAllocator.record(byteBuffer.remaining());
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
}
