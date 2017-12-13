package cc.whohow.redis.jcache.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import java.lang.reflect.Type;

public class JacksonCodec implements Codec {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected final ObjectMapper objectMapper;
    protected final Type type;

    public JacksonCodec(Type type) {
        this(OBJECT_MAPPER, type);
    }

    public JacksonCodec(ObjectMapper objectMapper, Type type) {
        this.objectMapper = objectMapper;
        this.type = type;
    }

    @Override
    public Decoder<Object> getMapValueDecoder() {
        return null;
    }

    @Override
    public Encoder getMapValueEncoder() {
        return null;
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
