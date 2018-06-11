package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * 自适应缓冲区编码器
 */
public abstract class AbstractAdaptiveCodec<T> extends AbstractCodec<T> {
    /**
     * 编/解码次数
     */
    private volatile long count = 0;
    /**
     * 缓冲区平均大小
     */
    private volatile double avgBufferSize = 0.0;
    /**
     * 缓冲区最小值
     */
    private volatile int minBufferSize = 0;
    /**
     * 缓冲区最大值
     */
    private volatile int maxBufferSize = 0;

    /**
     * 获取新缓冲区大小
     */
    protected int getBufferSize() {
        if (maxBufferSize == 0) {
            return 128;
        }
        if (maxBufferSize < 256) {
            return maxBufferSize;
        }
        if (maxBufferSize > avgBufferSize * 2) {
            return (int) avgBufferSize;
        }
        return (int) (avgBufferSize + maxBufferSize) / 2;
    }

    private synchronized void recordEncode(ByteBuffer byteBuffer) {
        record(byteBuffer);
    }

    private synchronized void recordDecode(ByteBuffer byteBuffer) {
//        record(byteBuffer);
    }

    private synchronized void record(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return;
        }
        int size = byteBuffer.remaining();

        count++;
        if (minBufferSize > size) {
            minBufferSize = size;
        }
        if (maxBufferSize < size) {
            maxBufferSize = size;
        }
        //   (avgBufferSize *  n +       size           ) / (n + 1)
        // = (avgBufferSize * (n + 1) + (size - avgBufferSize)) / (n + 1)
        // =  avgBufferSize +           (size - avgBufferSize ) / (n + 1)
        avgBufferSize = avgBufferSize + (size - avgBufferSize) / count;
    }

    @Override
    public final ByteBuffer encode(T value) {
        ByteBuffer buffer = encodeToByteBuffer(value);
        recordEncode(buffer);
        return buffer;
    }

    @Override
    public final T decode(ByteBuffer buffer) {
        recordDecode(buffer);
        return decodeByteBuffer(buffer);
    }

    @Override
    public final void encode(T value, OutputStream stream) throws IOException {
        encodeToStream(value, stream);
    }

    @Override
    public final T decode(InputStream stream) throws IOException {
        return decodeStream(stream);
    }

    protected ByteBuffer encodeToByteBuffer(T value) {
        return super.encode(value);
    }

    protected T decodeByteBuffer(ByteBuffer buffer) {
        return super.decode(buffer);
    }

    protected void encodeToStream(T value, OutputStream stream) throws IOException {
        super.encode(value, stream);
    }

    protected T decodeStream(InputStream stream) throws IOException {
        return super.decode(stream);
    }
}
