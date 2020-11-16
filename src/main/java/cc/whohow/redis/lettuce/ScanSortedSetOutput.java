package cc.whohow.redis.lettuce;

import cc.whohow.redis.RESP;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.util.RedisScanIteration;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class ScanSortedSetOutput<T> extends CommandOutput<ByteSequence, ByteSequence, RedisScanIteration<ScoredValue<T>>> {
    protected final Function<ByteBuffer, T> decoder;
    protected int index = 0;
    protected T value;

    public ScanSortedSetOutput(Function<ByteBuffer, T> decoder) {
        this(decoder, new RedisScanIteration<>());
    }

    public ScanSortedSetOutput(Function<ByteBuffer, T> decoder, RedisScanIteration<ScoredValue<T>> output) {
        super(ByteSequenceRedisCodec.get(), output);
        this.decoder = decoder;
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (output.getCursor() == null) {
            output.setCursor(decodeAscii(bytes));
        } else {
            if (index++ % 2 == 0) {
                value = decoder.apply(bytes);
            } else {
                output.getArray().add(ScoredValue.fromNullable(RESP.f64(decodeAscii(bytes)), value));
            }
        }
    }
}
