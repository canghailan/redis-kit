package cc.whohow.redis.lettuce;

import cc.whohow.redis.bytes.ByteSequence;

public class StatusOutput extends io.lettuce.core.output.StatusOutput<ByteSequence, ByteSequence> {
    public StatusOutput() {
        super(ByteSequenceRedisCodec.get());
    }
}
