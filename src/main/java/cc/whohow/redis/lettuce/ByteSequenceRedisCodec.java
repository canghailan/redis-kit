package cc.whohow.redis.lettuce;

import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.ToByteBufEncoder;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class ByteSequenceRedisCodec implements
        RedisCodec<ByteSequence, ByteSequence>,
        ToByteBufEncoder<ByteSequence, ByteSequence> {
    private static final ByteSequenceRedisCodec INSTANCE = new ByteSequenceRedisCodec();

    public static ByteSequenceRedisCodec get() {
        return INSTANCE;
    }

    @Override
    public ByteSequence decodeKey(ByteBuffer bytes) {
        return ByteSequence.of(bytes).copy();
    }

    @Override
    public ByteSequence decodeValue(ByteBuffer bytes) {
        return ByteSequence.of(bytes).copy();
    }

    @Override
    public ByteBuffer encodeKey(ByteSequence key) {
        return key.toByteBuffer();
    }

    @Override
    public ByteBuffer encodeValue(ByteSequence value) {
        return value.toByteBuffer();
    }

    @Override
    public void encodeKey(ByteSequence key, ByteBuf target) {
        encode(key, target);
    }

    @Override
    public void encodeValue(ByteSequence value, ByteBuf target) {
        encode(value, target);
    }

    protected void encode(ByteSequence byteSequence, ByteBuf target) {
        if (byteSequence != null) {
            if (byteSequence.hasArray()) {
                target.writeBytes(byteSequence.array(), byteSequence.arrayOffset(), byteSequence.length());
            } else {
                byteSequence.forEach(target::writeBytes);
            }
        }
    }

    @Override
    public int estimateSize(Object keyOrValue) {
        if (keyOrValue == null) {
            return 0;
        }
        ByteSequence byteSequence = (ByteSequence) keyOrValue;
        return byteSequence.length();
    }
}
