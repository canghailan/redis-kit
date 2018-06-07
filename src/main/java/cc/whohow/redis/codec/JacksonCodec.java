package cc.whohow.redis.codec;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JacksonCodec<T> extends AbstractStreamCodec<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected final ObjectMapper objectMapper;
    protected final JavaType type;

    public JacksonCodec(Class<T> type) {
        this(OBJECT_MAPPER, type);
    }

    public JacksonCodec(String canonicalName) {
        this(OBJECT_MAPPER, canonicalName);
    }

    public JacksonCodec(ObjectMapper objectMapper, Class<T> type) {
        this.objectMapper = objectMapper;
        this.type = objectMapper.getTypeFactory().constructType(type);
    }

    public JacksonCodec(ObjectMapper objectMapper, String canonicalName) {
        this.objectMapper = objectMapper;
        this.type = objectMapper.getTypeFactory().constructFromCanonical(canonicalName);
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
