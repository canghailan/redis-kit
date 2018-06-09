package cc.whohow.redis.io;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public class JacksonCodec<T> implements Codec<T> {
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
    public ByteBuffer encode(T value) {
        try {
            return ByteBuffer.wrap(objectMapper.writeValueAsBytes(value));
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public T decode(ByteBuffer bytes) {
        try {
            if (bytes == null || !bytes.hasRemaining()) {
                return null;
            }
            if (bytes.hasArray()) {
                return objectMapper.readValue(bytes.array(), bytes.arrayOffset(), bytes.remaining(), type);
            } else {
                return objectMapper.readValue(new ByteBufferInputStream(bytes), type);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
