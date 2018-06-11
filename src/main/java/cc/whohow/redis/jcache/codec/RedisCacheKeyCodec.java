package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.AbstractAdaptiveCodec;
import cc.whohow.redis.io.Codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RedisCacheKeyCodec<K> extends AbstractAdaptiveCodec<K> {
    private final String cacheName;
    private final String separator;
    private final Codec<K> keyCodec;
    private final ByteBuffer keyPrefix;

    public RedisCacheKeyCodec(String cacheName, String separator, Codec<K> keyCodec) {
        this.cacheName = cacheName;
        this.separator = separator;
        this.keyCodec = keyCodec;
        this.keyPrefix = StandardCharsets.UTF_8.encode(cacheName + separator);
    }

    public String getCacheName() {
        return cacheName;
    }

    public String getSeparator() {
        return separator;
    }

    public Codec<K> getKeyCodec() {
        return keyCodec;
    }

    @Override
    public K decodeByteBuffer(ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() == 0) {
            return null;
        }
        ByteBuffer keyPrefix = this.keyPrefix.duplicate();
        while (keyPrefix.hasRemaining()) {
            if (buffer.hasRemaining()) {
                if (buffer.get() != keyPrefix.get()) {
                    throw new IllegalStateException();
                }
            } else {
                throw new IllegalStateException();
            }
        }
        return keyCodec.decode(buffer);
    }

    @Override
    public void encodeToStream(K value, OutputStream stream) throws IOException {
        stream.write(keyPrefix.array(), keyPrefix.arrayOffset(), keyPrefix.remaining());
        keyCodec.encode(value, stream);
    }

    @Override
    public K decodeStream(InputStream stream) throws IOException {
        ByteBuffer keyPrefix = this.keyPrefix.duplicate();
        byte[] buffer = new byte[keyPrefix.remaining()];
        int offset = 0;
        while (keyPrefix.hasRemaining()) {
            int n = stream.read(buffer, offset, buffer.length - offset);
            if (n < 0) {
                throw new IllegalStateException();
            } else if (n > 0) {
                for (int i = 0; i < n; i++) {
                    if (buffer[offset++] != keyPrefix.get()) {
                        throw new IllegalStateException();
                    }
                }
            }
        }
        return keyCodec.decode(stream);
    }
}
