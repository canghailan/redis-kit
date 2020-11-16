package cc.whohow.redis.lettuce;

import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

public class SetOutput<T> extends CommandOutput<ByteSequence, ByteSequence, Set<T>> {
    protected final Function<ByteBuffer, T> decoder;

    public SetOutput(Function<ByteBuffer, T> decoder) {
        this(decoder, new LinkedHashSet<>());
    }

    public SetOutput(Function<ByteBuffer, T> decoder, Set<T> output) {
        super(ByteSequenceRedisCodec.get(), output);
        this.decoder = decoder;
    }

    @Override
    public void set(ByteBuffer bytes) {
        output.add(decoder.apply(bytes));
    }
}
