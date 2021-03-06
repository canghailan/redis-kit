package cc.whohow.redis.codec;

import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.bytes.ByteSummaryStatistics;
import cc.whohow.redis.io.IO;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class StreamCodecAdapter<T> implements Codec<T>, StreamCodec<T> {
    protected final Codec<T> codec;
    protected final ByteSummaryStatistics byteSummaryStatistics = new ByteSummaryStatistics();

    public StreamCodecAdapter(Codec<T> codec) {
        this.codec = codec;
    }

    @Override
    public ByteSequence encode(T value) {
        return codec.encode(value);
    }

    @Override
    public T decode(ByteSequence buffer) {
        return codec.decode(buffer);
    }

    @Override
    public T decode(byte... buffer) {
        return codec.decode(buffer);
    }

    @Override
    public T decode(ByteBuffer buffer) {
        return codec.decode(buffer);
    }

    @Override
    public void encode(T value, OutputStream stream) throws IOException {
        ByteSequence buffer = encode(value);
        if (buffer == null) {
            return;
        }
        for (ByteBuffer byteBuffer : buffer) {
            IO.write(stream, byteBuffer);
        }
    }

    @Override
    public T decode(InputStream stream) throws IOException {
        int bufferSize = (int) byteSummaryStatistics.getAverage();
        if (bufferSize > 0) {
            bufferSize = ByteSummaryStatistics.ceilingNextPowerOfTwo(bufferSize);
        } else {
            bufferSize = stream.available();
            if (bufferSize > 0) {
                bufferSize = ByteSummaryStatistics.ceilingNextPowerOfTwo(bufferSize);
            } else {
                bufferSize = IO.BUFFER_SIZE;
            }
        }
        ByteBuffer buffer = IO.read(stream, bufferSize);
        byteSummaryStatistics.accept(buffer.remaining());
        return decode(buffer);
    }
}
