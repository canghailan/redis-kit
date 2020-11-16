package cc.whohow.redis.lettuce;

import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ListOutput<T> extends CommandOutput<ByteSequence, ByteSequence, List<T>> {
    protected final Function<ByteBuffer, T> decoder;

    public ListOutput(Function<ByteBuffer, T> decoder) {
        this(decoder, new ArrayList<>());
    }

    public ListOutput(Function<ByteBuffer, T> decoder, List<T> output) {
        super(ByteSequenceRedisCodec.get(), output);
        this.decoder = decoder;
    }

    @Override
    public void set(ByteBuffer bytes) {
        output.add(decoder.apply(bytes));
    }
}
