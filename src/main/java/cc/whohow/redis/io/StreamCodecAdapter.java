package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class StreamCodecAdapter<T> implements Codec<T>, StreamCodec<T> {
    protected final Codec<T> codec;
    protected final ByteBufferAllocator byteBufferAllocator;

    public StreamCodecAdapter(Codec<T> codec) {
        this(codec, new ByteBufferAllocator());
    }

    public StreamCodecAdapter(Codec<T> codec, ByteBufferAllocator byteBufferAllocator) {
        this.codec = codec;
        this.byteBufferAllocator = byteBufferAllocator;
    }

    @Override
    public ByteBuffer encode(T value) {
        return codec.encode(value);
    }

    @Override
    public T decode(ByteBuffer buffer) {
        return codec.decode(buffer);
    }

    @Override
    public void encode(T value, OutputStream stream) throws IOException {
        ByteBuffer buffer = encode(value);
        if (buffer == null) {
            return;
        }
        IO.write(stream, buffer);
    }

    @Override
    public T decode(InputStream stream) throws IOException {
        ByteBuffer buffer = IO.read(stream, byteBufferAllocator.guess());
        byteBufferAllocator.record(buffer.remaining());
        return decode(buffer);
    }
}
