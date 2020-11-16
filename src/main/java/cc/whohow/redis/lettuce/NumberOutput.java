package cc.whohow.redis.lettuce;

import cc.whohow.redis.RESP;
import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

public class NumberOutput extends CommandOutput<ByteSequence, ByteSequence, Number> {
    public NumberOutput() {
        super(ByteSequenceRedisCodec.get(), null);
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (bytes != null) {
            output = RESP.f64(bytes);
        }
    }

    @Override
    public void set(long integer) {
        output = integer;
    }
}
