package cc.whohow.redis.codec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * JSON编码器
 */
public class JacksonCodec<T> extends AbstractStreamCodec<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        OBJECT_MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    protected final ObjectMapper objectMapper;
    protected final JavaType type;

    public JacksonCodec(Class<T> type) {
        this(OBJECT_MAPPER, type);
    }

    public JacksonCodec(String canonicalName) {
        this(OBJECT_MAPPER, canonicalName);
    }

    public JacksonCodec(TypeReference<T> type) {
        this(OBJECT_MAPPER, type);
    }

    public JacksonCodec(JavaType type) {
        this(OBJECT_MAPPER, type);
    }

    public JacksonCodec(ObjectMapper objectMapper, Class<T> type) {
        this(objectMapper, objectMapper.getTypeFactory().constructType(type));
    }

    public JacksonCodec(ObjectMapper objectMapper, String canonicalName) {
        this(objectMapper, objectMapper.getTypeFactory().constructFromCanonical(canonicalName));
    }

    public JacksonCodec(ObjectMapper objectMapper, TypeReference<T> type) {
        this(objectMapper, objectMapper.getTypeFactory().constructType(type));
    }

    public JacksonCodec(ObjectMapper objectMapper, JavaType type) {
        this.objectMapper = objectMapper;
        this.type = type;
    }

    @Override
    public void encode(T value, OutputStream stream) throws IOException {
        objectMapper.writeValue(stream, value);
    }

    @Override
    public T decode(InputStream stream) throws IOException {
        return objectMapper.readValue(stream, type);
    }
}
