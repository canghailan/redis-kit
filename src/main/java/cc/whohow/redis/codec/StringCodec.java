package cc.whohow.redis.codec;

import cc.whohow.redis.bytes.ByteSequence;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * 字符串编码器
 */
public class StringCodec implements Codec<String> {
    /**
     * NULL字符串占位符
     */
    private static final byte NULL_PLACEHOLDER = 127;
    private static final ByteSequence NULL = ByteSequence.of(NULL_PLACEHOLDER);
    private final Charset charset;

    public StringCodec(Charset charset) {
        this.charset = charset;
    }

    public static ByteSequence encodeNull() {
        return NULL;
    }

    public static boolean isEncodedNull(ByteSequence bytes) {
        return bytes.length() == 1 && bytes.get(0) == NULL_PLACEHOLDER;
    }

    public static boolean isEncodedNull(ByteBuffer bytes) {
        return bytes.remaining() == 1 && bytes.get(0) == NULL_PLACEHOLDER;
    }

    public static boolean isEncodedNull(byte... bytes) {
        return bytes.length == 1 && bytes[0] == NULL_PLACEHOLDER;
    }

    @Override
    public ByteSequence encode(String value) {
        if (value == null) {
            return encodeNull();
        }
        return ByteSequence.of(value, charset);
    }

    @Override
    public String decode(ByteSequence buffer) {
        if (buffer == null || isEncodedNull(buffer)) {
            return null;
        }
        return buffer.toString(charset);
    }

    @Override
    public String decode(ByteBuffer buffer) {
        if (buffer == null || isEncodedNull(buffer)) {
            return null;
        }
        if (buffer.hasArray()) {
            return new String(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining(), charset);
        }
        return charset.decode(buffer).toString();
    }

    @Override
    public String decode(byte... buffer) {
        if (buffer == null || isEncodedNull(buffer)) {
            return null;
        }
        return new String(buffer, charset);
    }

    public static class UTF8 implements Codec<String> {
        private static final UTF8 INSTANCE = new UTF8();

        public static UTF8 get() {
            return INSTANCE;
        }

        @Override
        public ByteSequence encode(String value) {
            if (value == null) {
                return encodeNull();
            }
            return ByteSequence.utf8(value);
        }

        @Override
        public String decode(ByteSequence buffer) {
            if (buffer == null || isEncodedNull(buffer)) {
                return null;
            }
            return buffer.toString(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte... buffer) {
            if (buffer == null || isEncodedNull(buffer)) {
                return null;
            }
            return new String(buffer, StandardCharsets.UTF_8);
        }

        @Override
        public String decode(ByteBuffer buffer) {
            if (buffer == null || isEncodedNull(buffer)) {
                return null;
            }
            if (buffer.hasArray()) {
                return new String(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining(), StandardCharsets.UTF_8);
            }
            return StandardCharsets.UTF_8.decode(buffer).toString();
        }
    }
}
