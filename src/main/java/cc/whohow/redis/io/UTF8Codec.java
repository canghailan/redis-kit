package cc.whohow.redis.io;

import cc.whohow.redis.buffer.ByteSequence;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class UTF8Codec implements Codec<String> {
    private static final UTF8Codec INSTANCE = new UTF8Codec();

    public static UTF8Codec get() {
        return INSTANCE;
    }

    @Override
    public ByteSequence encode(String value) {
        if (value == null) {
            return StringCodec.encodeNull();
        }
        return ByteSequence.utf8(value);
    }

    @Override
    public String decode(ByteSequence buffer) {
        if (buffer == null || StringCodec.isEncodedNull(buffer)) {
            return null;
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    @Override
    public String decode(byte... buffer) {
        if (buffer == null || StringCodec.isEncodedNull(buffer)) {
            return null;
        }
        return new String(buffer, StandardCharsets.UTF_8);
    }

    @Override
    public String decode(ByteBuffer buffer) {
        if (buffer == null || StringCodec.isEncodedNull(buffer)) {
            return null;
        }
        if (buffer.hasArray()) {
            return new String(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining(), StandardCharsets.UTF_8);
        }
        return StandardCharsets.UTF_8.decode(buffer).toString();
    }
}
