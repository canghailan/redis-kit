package cc.whohow.redis.io;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;

public class Lz4Codec<T> extends AbstractAdaptiveCodec<T> {
    private static final LZ4Factory FACTORY = LZ4Factory.fastestInstance();
    private static final LZ4Compressor COMPRESSOR = FACTORY.fastCompressor();
    private static final LZ4FastDecompressor DECOMPRESSOR = FACTORY.fastDecompressor();

    private final Codec<T> codec;

    public Lz4Codec(Codec<T> codec) {
        this.codec = codec;
    }

    @Override
    public ByteBuffer encodeToByteBuffer(T value) {
        ByteBuffer uncompressed = codec.encode(value);
        ByteBuffer compressed = ByteBuffer.allocate(uncompressed.remaining() + 4);
        compressed.putInt(uncompressed.remaining());
        COMPRESSOR.compress(uncompressed, compressed);
        compressed.flip();
        return compressed;
    }

    @Override
    public T decodeByteBuffer(ByteBuffer buffer) {
        ByteBuffer uncompressed = ByteBuffer.allocate(buffer.getInt());
        DECOMPRESSOR.decompress(buffer, uncompressed);
        uncompressed.flip();
        return codec.decode(uncompressed);
    }
}