package cc.whohow.redis.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface StreamCodec<T> {
    void encode(T value, OutputStream stream) throws IOException;

    T decode(InputStream stream) throws IOException;
}
