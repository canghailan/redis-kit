package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * GZIP压缩编码器
 */
public class GzipCodec<T> extends AbstractStreamCodec<T> {
    private final StreamCodec<T> codec;

    public GzipCodec(StreamCodec<T> codec) {
        super(new ByteBufferAllocator(1024, 8 * 1024));
        this.codec = codec;
    }

    public GzipCodec(Codec<T> codec) {
        this(codec, new ByteBufferAllocator(1024, 8 * 1024));
    }

    public GzipCodec(Codec<T> codec, ByteBufferAllocator byteBufferAllocator) {
        super(byteBufferAllocator);
        this.codec = new StreamCodecAdapter<>(codec, byteBufferAllocator);
    }

    @Override
    public void encode(T value, OutputStream stream) throws IOException {
        GZIPOutputStream gzip = new GZIPOutputStream(stream);
        codec.encode(value, gzip);
        gzip.finish();
        gzip.flush();
    }

    @Override
    public T decode(InputStream stream) throws IOException {
        return codec.decode(new GZIPInputStream(stream));
    }
}
