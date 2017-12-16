package cc.whohow.redis.codec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import java.io.IOException;
import java.util.Arrays;

public class ObjectArrayJacksonCodec implements Codec {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    protected final ObjectMapper objectMapper;
    protected final JavaType[] types;

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
            ArrayNode arrayNode = objectMapper.readValue(new ByteBufInputStream(buf), ArrayNode.class);
            Object[] objectArray = new Object[arrayNode.size()];
            for (int i = 0; i < arrayNode.size(); i++) {
                objectArray[i] = objectMapper.convertValue(arrayNode.get(i), types[i]);
            }
            return objectArray;
        }
    };

    public ObjectArrayJacksonCodec(String... typeCanonicalNames) {
        this(OBJECT_MAPPER, typeCanonicalNames);
    }

    public ObjectArrayJacksonCodec(ObjectMapper objectMapper, String... typeCanonicalNames) {
        this(objectMapper, Arrays.stream(typeCanonicalNames)
                .map(objectMapper.getTypeFactory()::constructFromCanonical)
                .toArray(JavaType[]::new));
    }

    public ObjectArrayJacksonCodec(Class<?>... types) {
        this(OBJECT_MAPPER, types);
    }

    public ObjectArrayJacksonCodec(ObjectMapper objectMapper, Class<?>... types) {
        this(OBJECT_MAPPER, Arrays.stream(types)
                .map(objectMapper.getTypeFactory()::constructType)
                .toArray(JavaType[]::new));
    }

    public ObjectArrayJacksonCodec(JavaType... types) {
        this(OBJECT_MAPPER, types);
    }

    public ObjectArrayJacksonCodec(ObjectMapper objectMapper, JavaType... types) {
        this.objectMapper = objectMapper;
        this.types = types;
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
        return "ObjectArrayJacksonCodec{" +
                "types=" + Arrays.toString(types) +
                '}';
    }
}
