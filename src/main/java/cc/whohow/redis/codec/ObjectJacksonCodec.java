package cc.whohow.redis.codec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import java.io.IOException;

public class ObjectJacksonCodec implements Codec {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    protected final ObjectMapper objectMapper;
    protected final JavaType type;

    private final Encoder encoder = new Encoder() {
        @Override
        @SuppressWarnings("Duplicates")
        public ByteBuf encode(Object in) throws IOException {
            ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
            try {
                ByteBufOutputStream os = new ByteBufOutputStream(out);
                objectMapper.writeValue(os, in);
                return os.buffer();
            } catch (IOException e) {
                out.release();
                throw e;
            }
        }
    };

    private final Decoder<Object> decoder = new Decoder<Object>() {
        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            return objectMapper.readValue(new ByteBufInputStream(buf), type);
        }
    };

    public ObjectJacksonCodec(String typeCanonicalName) {
        this(OBJECT_MAPPER, typeCanonicalName);
    }

    public ObjectJacksonCodec(ObjectMapper objectMapper, String typeCanonicalName) {
        this(OBJECT_MAPPER, objectMapper.getTypeFactory().constructFromCanonical(typeCanonicalName));
    }

    public ObjectJacksonCodec(Class<?> type) {
        this(OBJECT_MAPPER, type);
    }

    public ObjectJacksonCodec(ObjectMapper objectMapper, Class<?> type) {
        this(objectMapper, objectMapper.getTypeFactory().constructType(type));
    }

    public ObjectJacksonCodec(JavaType type) {
        this(OBJECT_MAPPER, type);
    }

    public ObjectJacksonCodec(ObjectMapper objectMapper, JavaType type) {
        this.objectMapper = objectMapper;
        this.type = type;
    }

    @Override
    public Decoder<Object> getMapValueDecoder() {
        return getValueDecoder();
    }

    @Override
    public Encoder getMapValueEncoder() {
        return getValueEncoder();
    }

    @Override
    public Decoder<Object> getMapKeyDecoder() {
        return getValueDecoder();
    }

    @Override
    public Encoder getMapKeyEncoder() {
        return getValueEncoder();
    }

    @Override
    public Decoder<Object> getValueDecoder() {
        return decoder;
    }

    @Override
    public Encoder getValueEncoder() {
        return encoder;
    }

    @Override
    public String toString() {
        return "ObjectJacksonCodec{" +
                "type=" + type +
                '}';
    }
}
