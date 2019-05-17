package cc.whohow.redis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class StreamCodecAdapter<T> implements Codec<T>, StreamCodec<T> {
    protected final Codec<T> codec;
    protected final BufferAllocationPredictor predictor;

    public StreamCodecAdapter(Codec<T> codec) {
        this(codec, new BufferAllocationPredictor());
    }

    public StreamCodecAdapter(Codec<T> codec, BufferAllocationPredictor predictor) {
        this.codec = codec;
        this.predictor = predictor;
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
        if (buffer.hasArray()) {
            if (buffer.hasRemaining()) {
                stream.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            }
        } else {
            while (buffer.hasRemaining()) {
                stream.write(buffer.get());
            }
        }
    }

    @Override
    public T decode(InputStream stream) throws IOException {
        ByteBuffer buffer = new Java9InputStream(stream).readAllBytes(predictor.getPredicted());
        predictor.accept(buffer.remaining());
        return decode(buffer);
    }
}
