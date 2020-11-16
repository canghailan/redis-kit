package cc.whohow.redis.lettuce;

import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringOutput extends CommandOutput<ByteSequence, ByteSequence, String> {
    protected final Charset charset;

    public StringOutput() {
        this(StandardCharsets.UTF_8);
    }

    public StringOutput(Charset charset) {
        super(ByteSequenceRedisCodec.get(), null);
        this.charset = charset;
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (bytes != null) {
            output = charset.decode(bytes).toString();
        }
    }
}
