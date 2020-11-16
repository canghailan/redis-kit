package cc.whohow.redis.codec;

import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.bytes.ConcatByteSequence;

import java.nio.ByteBuffer;

/**
 * 前缀编码器
 */
public class PrefixCodec<T> implements Codec<T> {
    private final Codec<T> codec;
    private final ByteSequence prefix;

    public PrefixCodec(Codec<T> codec, ByteSequence prefix) {
        this.codec = codec;
        this.prefix = prefix;
    }

    public PrefixCodec(Codec<T> codec, String prefix) {
        this(codec, ByteSequence.utf8(prefix));
    }

    public Codec<T> getCodec() {
        return codec;
    }

    public ByteSequence getPrefix() {
        return prefix;
    }

    @Override
    public ByteSequence encode(T value) {
        return new ConcatByteSequence(prefix, codec.encode(value));
    }

    @Override
    public T decode(ByteSequence buffer) {
        if (buffer == null) {
            return null;
        }
        // skip check prefix
        return codec.decode(buffer.subSequence(prefix.length(), buffer.length()));
    }

    @Override
    public T decode(byte... buffer) {
        if (buffer == null) {
            return null;
        }
        // skip check prefix
        return codec.decode(ByteSequence.of(buffer).subSequence(prefix.length(), buffer.length));
    }

    @Override
    public T decode(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        // skip check prefix
        buffer.position(buffer.position() + prefix.length());
        return codec.decode(buffer);
    }
}
