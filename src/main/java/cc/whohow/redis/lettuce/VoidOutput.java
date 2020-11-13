package cc.whohow.redis.lettuce;

import cc.whohow.redis.buffer.ByteSequence;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

public class VoidOutput extends CommandOutput<ByteSequence, ByteSequence, Void> {
    public VoidOutput() {
        super(ByteSequenceRedisCodec.get(), null);
    }

    @Override
    public void set(ByteBuffer bytes) {
    }
}
