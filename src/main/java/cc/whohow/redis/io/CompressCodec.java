package cc.whohow.redis.io;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CompressCodec<T> extends AbstractStreamCodec<T> {
    private final String name;
    private final Codec<T> codec;
    private CompressorStreamFactory factory = CompressorStreamFactory.getSingleton();

    public CompressCodec(String name, Codec<T> codec) {
        super(new SummaryStatistics(1024, 8 * 1024));
        this.name = name;
        this.codec = codec;
    }

    @Override
    public void encode(T value, OutputStream stream) throws IOException {
        try (CompressorOutputStream compressor = factory.createCompressorOutputStream(
                name, new DelegateOutputStream(stream, false))) {
            codec.encode(value, compressor);
        } catch (CompressorException e) {
            throw new IOException(e);
        }
    }

    @Override
    public T decode(InputStream stream) throws IOException {
        try {
            return codec.decode(factory.createCompressorInputStream(name, stream));
        } catch (CompressorException e) {
            throw new IOException(e);
        }
    }
}
