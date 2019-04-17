package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface StreamCodec<T> {
    void encode(T value, OutputStream stream) throws IOException;

    T decode(InputStream stream) throws IOException;
}
