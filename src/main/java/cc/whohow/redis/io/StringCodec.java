package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringCodec extends AbstractAdaptiveCodec<String> {
    private static final ByteBuffer NULL = ByteBuffer.wrap(new byte[]{0});
    private final Charset charset;

    public StringCodec() {
        this(StandardCharsets.UTF_8);
    }

    public StringCodec(Charset charset) {
        this.charset = charset;
    }

    private static boolean isNull(ByteBuffer buffer) {
        return (buffer == null) || (buffer.remaining() == 1 && buffer.get(0) == 0);
    }

    @Override
    public ByteBuffer encodeToByteBuffer(String value) {
        return value == null ? NULL.duplicate() : charset.encode(value);
    }

    @Override
    public String decodeByteBuffer(ByteBuffer buffer) {
        return isNull(buffer) ? null : charset.decode(buffer).toString();
    }
}
