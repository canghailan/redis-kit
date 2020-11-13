package cc.whohow.redis.lettuce;

import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.util.RedisScanIteration;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class ScanOutput<T> extends CommandOutput<ByteSequence, ByteSequence, RedisScanIteration<T>> {
    protected final Function<ByteBuffer, T> decoder;

    public ScanOutput(Function<ByteBuffer, T> decoder) {
        this(decoder, new RedisScanIteration<>());
    }

    public ScanOutput(Function<ByteBuffer, T> decoder, RedisScanIteration<T> output) {
        super(ByteSequenceRedisCodec.get(), output);
        this.decoder = decoder;
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (output.getCursor() == null) {
            output.setCursor(decodeAscii(bytes));
        } else {
            output.getArray().add(decoder.apply(bytes));
        }
    }
}
