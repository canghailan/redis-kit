package cc.whohow.redis.lettuce;

import cc.whohow.redis.buffer.ByteSequence;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

public class TimeOutput extends CommandOutput<ByteSequence, ByteSequence, Long> {
    public TimeOutput() {
        super(ByteSequenceRedisCodec.get(), 0L);
    }

    @Override
    public void set(ByteBuffer bytes) {
        output = output * 1000_000 + Long.parseLong(decodeAscii(bytes));
    }
}
