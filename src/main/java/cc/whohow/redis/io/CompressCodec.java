package cc.whohow.redis.io;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CompressCodec<T> extends AbstractStreamCodec<T> {
    private final String name;
    private final StreamCodec<T> codec;
    private final CompressorStreamFactory factory = CompressorStreamFactory.getSingleton();

    public CompressCodec(String name, StreamCodec<T> codec) {
        super(new ByteBufferAllocator(1024, 8 * 1024));
        this.name = name;
        this.codec = codec;
    }

    public CompressCodec(String name, Codec<T> codec) {
        this(name, codec, new ByteBufferAllocator(1024, 8 * 1024));
    }

    public CompressCodec(String name, Codec<T> codec, ByteBufferAllocator byteBufferAllocator) {
        super(byteBufferAllocator);
        this.name = name;
        this.codec = new StreamCodecAdapter<>(codec, byteBufferAllocator);
    }

    @Override
    public void encode(T value, OutputStream stream) throws IOException {
        try (CompressorOutputStream compressor = newCompressorOutputStream(stream)) {
            codec.encode(value, compressor);
        } catch (CompressorException e) {
            throw new IOException(e);
        }
    }

    @Override
    public T decode(InputStream stream) throws IOException {
        try {
            return codec.decode(newCompressorInputStream(stream));
        } catch (CompressorException e) {
            throw new IOException(e);
        }
    }

    protected CompressorInputStream newCompressorInputStream(InputStream stream) throws CompressorException {
        return factory.createCompressorInputStream(name, stream);
    }

    protected CompressorOutputStream newCompressorOutputStream(OutputStream stream) throws CompressorException {
        return factory.createCompressorOutputStream(name, new FilterOutputStream(stream) {
            @Override
            public void close() throws IOException {
                // skip
            }
        });
    }
}
