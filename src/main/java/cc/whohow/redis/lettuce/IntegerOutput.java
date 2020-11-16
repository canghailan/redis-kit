package cc.whohow.redis.lettuce;

import cc.whohow.redis.RESP;
import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

public class IntegerOutput extends CommandOutput<ByteSequence, ByteSequence, Long> {
    public IntegerOutput() {
        this(null);
    }

    public IntegerOutput(Long value) {
        super(ByteSequenceRedisCodec.get(), value);
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (bytes != null) {
            output = RESP.i64(bytes);
        }
    }

    @Override
    public void set(long integer) {
        output = integer;
    }
}
