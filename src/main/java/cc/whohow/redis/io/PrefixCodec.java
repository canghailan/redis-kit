package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * 前缀编码器
 */
public class PrefixCodec<T> implements Codec<T> {
    private final Codec<T> codec;
    private final ByteBuffer prefix;

    public PrefixCodec(Codec<T> codec, ByteBuffer prefix) {
        this.codec = codec;
        this.prefix = prefix;
    }

    public PrefixCodec(Codec<T> codec, String prefix) {
        this(codec, ByteBuffers.fromUtf8(prefix));
    }

    @Override
    public ByteBuffer encode(T value) {
        return ByteBuffers.concat(prefix, codec.encode(value));
    }

    @Override
    public T decode(ByteBuffer buffer) {
//        if (ByteBuffers.startsWith(buffer, prefix)) {
        return codec.decode(ByteBuffers.slice(buffer, prefix.remaining()));
//        }
//        throw new IllegalStateException();
    }

    @Override
    public void encode(T value, OutputStream stream) throws IOException {
        stream.write(prefix.array(), prefix.arrayOffset(), prefix.remaining());
        codec.encode(value, stream);
    }

    @Override
    public T decode(InputStream stream) throws IOException {
        for (int i = 0; i < prefix.remaining(); i++) {
            if (stream.read() != prefix.get(i)) {
                throw new IllegalStateException();
            }
        }
        return codec.decode(stream);
    }
}
