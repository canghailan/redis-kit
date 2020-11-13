package cc.whohow.redis.io;

import cc.whohow.redis.buffer.ByteSequence;

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
