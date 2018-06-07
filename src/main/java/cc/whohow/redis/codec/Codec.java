package cc.whohow.redis.codec;

import java.nio.ByteBuffer;

public interface Codec<T> {
    ByteBuffer encode(T value);

    T decode(ByteBuffer bytes);
}
