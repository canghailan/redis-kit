package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringCodec extends AbstractCodec<String> {
    private final Charset charset;

    public StringCodec() {
        this(StandardCharsets.UTF_8);
    }

    public StringCodec(Charset charset) {
        this.charset = charset;
    }

    @Override
    public ByteBuffer encode(String value) {
        ByteBuffer buffer = charset.encode(value);
        record(buffer);
        return buffer;
    }

    @Override
    public String decode(ByteBuffer buffer) {
        return charset.decode(buffer).toString();
    }
}
