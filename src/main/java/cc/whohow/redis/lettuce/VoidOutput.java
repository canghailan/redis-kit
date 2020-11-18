package cc.whohow.redis.lettuce;

import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

public class VoidOutput extends CommandOutput<ByteSequence, ByteSequence, Void> {
    public VoidOutput() {
        super(ByteSequenceRedisCodec.get(), null);
    }

    @Override
    public void set(ByteBuffer bytes) {
    }

    @Override
    public void set(long integer) {
    }
}
