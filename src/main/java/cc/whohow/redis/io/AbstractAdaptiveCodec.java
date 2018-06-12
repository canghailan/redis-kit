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
        // 初始值
        if (maxBufferSize == 0) {
            return 128;
        }
        // 小型缓冲区
        if (maxBufferSize < 256) {
            return maxBufferSize;
        }
        // 波动较大缓冲区
        if (maxBufferSize > avgBufferSize * 2) {
            return (int) avgBufferSize;
        }
        return (int) (avgBufferSize + maxBufferSize) / 2;
    }

    private synchronized void recordEncode(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return;
        }
        int bufferSize = byteBuffer.remaining();

        // 记录最大、最小值，计算平均值
        count++;
        if (minBufferSize > bufferSize) {
            minBufferSize = bufferSize;
        }
        if (maxBufferSize < bufferSize) {
            maxBufferSize = bufferSize;
        }
        //   (avgBufferSize *  n +       bufferSize                 ) / (n + 1)
        // = (avgBufferSize * (n + 1) + (bufferSize - avgBufferSize)) / (n + 1)
        // =  avgBufferSize +           (bufferSize - avgBufferSize ) / (n + 1)
        avgBufferSize += (bufferSize - avgBufferSize) / count;
    }

    private synchronized void recordDecode(ByteBuffer byteBuffer) {
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
