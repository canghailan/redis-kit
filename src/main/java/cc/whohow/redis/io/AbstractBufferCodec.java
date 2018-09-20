package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * 自适应缓冲区编码器
 */
public abstract class AbstractBufferCodec<T> implements Codec<T> {
    protected final SummaryStatistics stat;

    protected AbstractBufferCodec(SummaryStatistics stat) {
        this.stat = stat;
    }

    @Override
    public void encode(T value, OutputStream stream) throws IOException {
        ByteBuffer buffer = encode(value);
        if (buffer == null) {
            return;
        }
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
        ByteBuffer buffer = new Java9InputStream(stream).readAllBytes(stat.getTypical());
        stat.accept(buffer.remaining());
        return decode(buffer);
    }
}
