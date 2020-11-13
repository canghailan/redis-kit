package cc.whohow.redis.lettuce;

import cc.whohow.redis.RESP;
import cc.whohow.redis.buffer.ByteSequence;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class ScoredValueOutput<T> extends CommandOutput<ByteSequence, ByteSequence, ScoredValue<T>> {
    protected final Function<ByteBuffer, T> decoder;
    protected int index = 0;
    protected T value;

    public ScoredValueOutput(Function<ByteBuffer, T> decoder) {
        super(ByteSequenceRedisCodec.get(), ScoredValue.empty());
        this.decoder = decoder;
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (index++ % 2 == 0) {
            value = decoder.apply(bytes);
        } else {
            output = ScoredValue.fromNullable(RESP.f64(bytes), value);
        }
    }
}
