package cc.whohow.redis.io;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 字符串编码器
 */
public class StringCodec implements Codec<String> {
    /**
     * NULL字符串占位符
     */
    private static final byte NULL_PLACEHOLDER = 127;
    private static final ByteBuffer NULL = ByteBuffer.wrap(new byte[]{NULL_PLACEHOLDER});
    private final Charset charset;
    private final BufferAllocationPredictor predictor = new BufferAllocationPredictor(16, 256);

    public StringCodec() {
        this(StandardCharsets.UTF_8);
    }

    public StringCodec(Charset charset) {
        this.charset = charset;
    }

    private static boolean isNull(ByteBuffer buffer) {
        return (buffer == null) || (buffer.remaining() == 1 && buffer.get(0) == NULL_PLACEHOLDER);
    }

    @Override
    public ByteBuffer encode(String value) {
        return (value == null) ? NULL.duplicate() : charset.encode(value);
    }

    @Override
    public String decode(ByteBuffer buffer) {
        return isNull(buffer) ? null : charset.decode(buffer).toString();
    }

    @Override
    public void encode(String value, OutputStream stream) throws IOException {
        Writer writer = new OutputStreamWriter(stream, charset);
        writer.write(value);
        writer.flush();
    }

    @Override
    public String decode(InputStream stream) throws IOException {
        char[] buffer = new char[predictor.getPredicted()];
        Reader reader = new InputStreamReader(stream, charset);
        int offset = 0;
        int length = buffer.length;
        while (true) {
            int n = reader.read(buffer, offset, length);
            if (n < 0) {
                predictor.accept(offset);
                return new String(buffer, 0, offset);
            } else if (n > 0) {
                offset += n;
                length -= n;
                if (length == 0) {
                    buffer = Arrays.copyOf(buffer, buffer.length * 2);
                    length = buffer.length - offset;
                }
            }
        }
    }
}
