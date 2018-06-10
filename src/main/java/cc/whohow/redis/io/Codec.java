package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface Codec<T> {
    ByteBuffer encode(T value);

    T decode(ByteBuffer buffer);

    void encode(T value, OutputStream stream) throws IOException;

    T decode(InputStream stream) throws IOException;
}
