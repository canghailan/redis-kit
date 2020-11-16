package cc.whohow.redis.codec;

import cc.whohow.redis.bytes.ByteSequence;

import java.nio.ByteBuffer;

public interface Codec<T> {
    ByteSequence encode(T value);

    T decode(ByteSequence buffer);

    default T decode(byte... buffer) {
        return decode(ByteSequence.of(buffer));
    }

    default T decode(ByteBuffer buffer) {
        return decode(ByteSequence.of(buffer));
    }
}
