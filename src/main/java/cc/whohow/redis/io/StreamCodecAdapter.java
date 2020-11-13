package cc.whohow.redis.io;

import cc.whohow.redis.buffer.ByteSequence;

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
    public ByteSequence encode(T value) {
        return codec.encode(value);
    }

    @Override
    public T decode(ByteSequence buffer) {
        return codec.decode(buffer);
    }

    @Override
    public T decode(byte... buffer) {
        return codec.decode(buffer);
    }

    @Override
    public T decode(ByteBuffer buffer) {
        return codec.decode(buffer);
    }

    @Override
    public void encode(T value, OutputStream stream) throws IOException {
        ByteSequence buffer = encode(value);
        if (buffer == null) {
            return;
        }
        for (ByteBuffer byteBuffer : buffer) {
            IO.write(stream, byteBuffer);
        }
    }

    @Override
    public T decode(InputStream stream) throws IOException {
        ByteBuffer buffer = IO.read(stream, byteBufferAllocator.guess());
        byteBufferAllocator.record(buffer.remaining());
        return decode(buffer);
    }
}
