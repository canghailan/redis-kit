package cc.whohow.redis.codec;

import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.bytes.ByteSequenceInputStream;
import cc.whohow.redis.bytes.ByteSequenceOutputStream;
import cc.whohow.redis.bytes.ByteSummaryStatistics;
import cc.whohow.redis.io.ByteBufferInputStream;
import cc.whohow.redis.io.IO;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/**
 * 自适应缓冲区编码器
 */
public abstract class AbstractStreamCodec<T> implements Codec<T>, StreamCodec<T> {
    protected final ByteSummaryStatistics byteSummaryStatistics = new ByteSummaryStatistics();

    @Override
    public ByteSequence encode(T value) {
        int bufferSize = (int) byteSummaryStatistics.getAverage();
        if (bufferSize > 0) {
            bufferSize = ByteSummaryStatistics.ceilingNextPowerOfTwo(bufferSize);
        } else {
            bufferSize = IO.BUFFER_SIZE;
        }
        try (ByteSequenceOutputStream stream = new ByteSequenceOutputStream(bufferSize)) {
            encode(value, stream);
            ByteSequence buffer = stream.getByteSequence();
            byteSummaryStatistics.accept(buffer.length());
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
