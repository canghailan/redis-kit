package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringCodec implements Codec<String> {
    public static final StringCodec ASCII = new StringCodec(StandardCharsets.US_ASCII);
    public static final StringCodec UTF_8 = new StringCodec(StandardCharsets.UTF_8);

    private final Charset charset;

    public StringCodec(Charset charset) {
        this.charset = charset;
    }

    @Override
    public ByteBuffer encode(String value) {
        return charset.encode(value);
    }

    @Override
    public String decode(ByteBuffer buffer) {
        return charset.decode(buffer).toString();
    }
}
