package cc.whohow.redis.lettuce;

import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class DecodeOutput<T> extends CommandOutput<ByteSequence, ByteSequence, T> {
    protected final Function<ByteBuffer, T> decoder;

    public DecodeOutput(Function<ByteBuffer, T> decoder) {
        this(decoder, null);
    }

    public DecodeOutput(Function<ByteBuffer, T> decoder, T defaultValue) {
        super(ByteSequenceRedisCodec.get(), defaultValue);
        this.decoder = decoder;
    }

    @Override
    public void set(ByteBuffer bytes) {
        output = decoder.apply(bytes);
    }
}
