package cc.whohow.redis.lettuce;

import cc.whohow.redis.RESP;
import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ScoredValueListOutput<T> extends CommandOutput<ByteSequence, ByteSequence, List<ScoredValue<T>>> {
    protected final Function<ByteBuffer, T> decoder;
    protected int index = 0;
    protected T value;

    public ScoredValueListOutput(Function<ByteBuffer, T> decoder) {
        super(ByteSequenceRedisCodec.get(), new ArrayList<>());
        this.decoder = decoder;
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (index++ % 2 == 0) {
            value = decoder.apply(bytes);
        } else {
            output.add(ScoredValue.fromNullable(RESP.f64(bytes), value));
            value = null;
        }
    }
}
