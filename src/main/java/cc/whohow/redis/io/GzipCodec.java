package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCodec<T> extends AbstractAdaptiveCodec<T> {
    private final Codec<T> codec;

    public GzipCodec(Codec<T> codec) {
        this.codec = codec;
    }

    @Override
    protected void encodeToStream(T value, OutputStream stream) throws IOException {
        GZIPOutputStream gzip = new GZIPOutputStream(stream);
        codec.encode(value, gzip);
        gzip.finish();
        gzip.flush();
    }

    @Override
    protected T decodeStream(InputStream stream) throws IOException {
        return codec.decode(new GZIPInputStream(stream));
    }
}
