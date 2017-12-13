package cc.whohow.redis.jcache.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import java.io.IOException;
import java.lang.reflect.Type;

public class JCacheKeyJacksonCodec implements Codec {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ObjectMapper objectMapper;
    private final Type[] cacheKeyTypes;

    public JCacheKeyJacksonCodec(Type[] cacheKeyTypes) {
        this(OBJECT_MAPPER, cacheKeyTypes);
    }

    public JCacheKeyJacksonCodec(ObjectMapper objectMapper, Type[] cacheKeyTypes) {
        this.objectMapper = objectMapper;
        this.cacheKeyTypes = cacheKeyTypes;
    }

    public Type[] getCacheKeyTypes() {
        return cacheKeyTypes;
    }

    @Override
    public Decoder<Object> getMapValueDecoder() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Encoder getMapValueEncoder() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Decoder<Object> getMapKeyDecoder() {
        return null;
    }

    @Override
    public Encoder getMapKeyEncoder() {
        return null;
    }

    @Override
    public Decoder<Object> getValueDecoder() {
        return null;
    }

    @Override
    public Encoder getValueEncoder() {
        return null;
    }
}
