package cc.whohow.redis.io;

import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.buffer.ByteSequenceInputStream;
import cc.whohow.redis.buffer.ByteSequenceOutputStream;

import java.io.ByteArrayInputStream;
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
    public ByteSequence encode(T value) {
        try (ByteSequenceOutputStream stream = new ByteSequenceOutputStream(byteBufferAllocator.guess())) {
            encode(value, stream);
            ByteSequence buffer = stream.getByteSequence();
            byteBufferAllocator.record(buffer.length());
            return buffer;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public T decode(ByteSequence buffer) {
        if (buffer == null) {
            return null;
        }
        try (ByteSequenceInputStream stream = new ByteSequenceInputStream(buffer)) {
            return decode(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public T decode(byte... buffer) {
        if (buffer == null) {
            return null;
        }
        try (ByteArrayInputStream stream = new ByteArrayInputStream(buffer)) {
            return decode(stream);
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
