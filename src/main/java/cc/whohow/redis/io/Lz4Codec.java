package cc.whohow.redis.io;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;

public class Lz4Codec<T> implements Codec<T> {
    private LZ4Factory factory = LZ4Factory.fastestInstance();
    private LZ4Compressor compressor = factory.fastCompressor();
    private LZ4FastDecompressor decompressor = factory.fastDecompressor();

    private Codec<T> codec;

    @Override
    public ByteBuffer encode(T value) {
        ByteBuffer uncompressed = codec.encode(value);
        ByteBuffer compressed = ByteBuffer.allocate(uncompressed.remaining() + 4);
        compressed.putInt(uncompressed.remaining());
        compressor.compress(uncompressed, compressed);
        compressed.flip();
        return compressed;
    }

    @Override
    public T decode(ByteBuffer bytes) {
        ByteBuffer uncompressed = ByteBuffer.allocate(bytes.getInt());
        decompressor.decompress(bytes, uncompressed);
        uncompressed.flip();
        return codec.decode(uncompressed);
    }
}
