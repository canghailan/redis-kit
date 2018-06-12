package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.AbstractAdaptiveCodec;
import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.Java9InputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Redis缓存key编码器，处理cacheName前缀
 */
public class RedisCacheKeyCodec<K> extends AbstractAdaptiveCodec<K> {
    /**
     * 缓存名
     */
    private final String cacheName;
    /**
     * 分隔符
     */
    private final String separator;
    /**
     * RedisKey前缀（已编码）
     */
    private final ByteBuffer keyPrefix;
    /**
     * 缓存Key编码器
     */
    private final Codec<K> keyCodec;

    public RedisCacheKeyCodec(String cacheName, String separator, Codec<K> keyCodec) {
        this.cacheName = cacheName;
        this.separator = separator;
        this.keyPrefix = ByteBuffers.fromUtf8(cacheName + separator);
        this.keyCodec = keyCodec;
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
        if (buffer == null) {
            return null;
        }
        if (ByteBuffers.startsWith(buffer, keyPrefix)) {
            buffer.position(buffer.position() + keyPrefix.remaining());
            return keyCodec.decode(buffer);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public void encodeToStream(K value, OutputStream stream) throws IOException {
        stream.write(keyPrefix.array(), keyPrefix.arrayOffset(), keyPrefix.remaining());
        keyCodec.encode(value, stream);
    }

    @Override
    public K decodeStream(InputStream stream) throws IOException {
        ByteBuffer buffer = new Java9InputStream(stream).readNBytes(keyPrefix.remaining());
        if (ByteBuffers.contentEquals(buffer, keyPrefix)) {
            return keyCodec.decode(stream);
        }
        throw new IllegalArgumentException();
    }
}
