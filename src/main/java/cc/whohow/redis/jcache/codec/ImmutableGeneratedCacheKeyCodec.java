package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.AbstractStreamCodec;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.ByteBufferInputStream;
import cc.whohow.redis.io.ByteBufferOutputStream;
import cc.whohow.redis.jcache.ImmutableGeneratedCacheKey;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

public class ImmutableGeneratedCacheKeyCodec implements Codec<ImmutableGeneratedCacheKey> {
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected Codec<ImmutableGeneratedCacheKey> delegate;

    public ImmutableGeneratedCacheKeyCodec(String... cacheKeyTypeCanonicalNames) {
        Objects.requireNonNull(cacheKeyTypeCanonicalNames);
        if (cacheKeyTypeCanonicalNames.length == 0) {
            delegate = NoKeyCodec.INSTANCE;
        } else if (cacheKeyTypeCanonicalNames.length == 1) {
            switch (cacheKeyTypeCanonicalNames[0]) {
                case "java.lang.String": {
                    delegate = StringKeyCodec.INSTANCE;
                }
                case "java.lang.Integer": {
                    delegate = PlainKeyCodec.INTEGER;
                }
                case "java.lang.Long": {
                    delegate = PlainKeyCodec.LONG;
                }
                default: {
                    delegate = new ObjectKeyCodec(cacheKeyTypeCanonicalNames[0]);
                }
            }
        } else {
            delegate = new ArrayKeyCodec(cacheKeyTypeCanonicalNames);
        }
    }

    @Override
    public ByteBuffer encode(ImmutableGeneratedCacheKey value) {
        return delegate.encode(value);
    }

    @Override
    public ImmutableGeneratedCacheKey decode(ByteBuffer bytes) {
        return delegate.decode(bytes);
    }

    private static class NoKeyCodec implements Codec<ImmutableGeneratedCacheKey> {
        static final NoKeyCodec INSTANCE = new NoKeyCodec();

        @Override
        public ByteBuffer encode(ImmutableGeneratedCacheKey value) {
            return EMPTY;
        }

        @Override
        public ImmutableGeneratedCacheKey decode(ByteBuffer bytes) {
            return ImmutableGeneratedCacheKey.empty();
        }
    }

    private static class StringKeyCodec implements Codec<ImmutableGeneratedCacheKey> {
        static final StringKeyCodec INSTANCE = new StringKeyCodec();

        @Override
        public ByteBuffer encode(ImmutableGeneratedCacheKey value) {
            return StandardCharsets.UTF_8.encode(value.getKey(0).toString());
        }

        @Override
        public ImmutableGeneratedCacheKey decode(ByteBuffer bytes) {
            return ImmutableGeneratedCacheKey.of(StandardCharsets.UTF_8.decode(bytes).toString());
        }
    }

    private static class PlainKeyCodec implements Codec<ImmutableGeneratedCacheKey> {
        static final PlainKeyCodec INTEGER = new PlainKeyCodec(Integer::parseInt);
        static final PlainKeyCodec LONG = new PlainKeyCodec(Long::parseLong);

        private final Function<String, ?> parse;

        private PlainKeyCodec(Function<String, ?> parse) {
            this.parse = parse;
        }

        @Override
        public ByteBuffer encode(ImmutableGeneratedCacheKey value) {
            Object key = value.getKey(0);
            return key == null ? EMPTY : StandardCharsets.UTF_8.encode(key.toString());
        }

        @Override
        public ImmutableGeneratedCacheKey decode(ByteBuffer bytes) {
            if (bytes.hasRemaining()) {
                return ImmutableGeneratedCacheKey.of(parse.apply(StandardCharsets.UTF_8.decode(bytes).toString()));
            }
            return ImmutableGeneratedCacheKey.of((Object) null);
        }
    }

    private static class ObjectKeyCodec extends AbstractStreamCodec<ImmutableGeneratedCacheKey> {
        private final JavaType type;

        private ObjectKeyCodec(String canonicalName) {
            this.type = OBJECT_MAPPER.getTypeFactory().constructFromCanonical(canonicalName);
        }

        @Override
        public void encode(ImmutableGeneratedCacheKey value, ByteBufferOutputStream stream) throws IOException {
            OBJECT_MAPPER.writeValue(stream, value.getKey(0));
        }

        @Override
        public ImmutableGeneratedCacheKey decode(ByteBufferInputStream stream) throws IOException {
            return ImmutableGeneratedCacheKey.of(OBJECT_MAPPER.readValue(stream, type));
        }
    }

    private static class ArrayKeyCodec extends AbstractStreamCodec<ImmutableGeneratedCacheKey> {
        private final JavaType[] types;

        public ArrayKeyCodec(String... canonicalName) {
            this.types = Arrays.stream(canonicalName)
                    .map(OBJECT_MAPPER.getTypeFactory()::constructFromCanonical)
                    .toArray(JavaType[]::new);
        }

        @Override
        public void encode(ImmutableGeneratedCacheKey value, ByteBufferOutputStream stream) throws IOException {
            OBJECT_MAPPER.writeValue(stream, value.getKeys());
        }

        @Override
        public ImmutableGeneratedCacheKey decode(ByteBufferInputStream stream) throws IOException {
            ArrayNode arrayNode = OBJECT_MAPPER.readValue(stream, ArrayNode.class);
            Object[] objectArray = new Object[arrayNode.size()];
            for (int i = 0; i < arrayNode.size(); i++) {
                objectArray[i] = OBJECT_MAPPER.convertValue(arrayNode.get(i), types[i]);
            }
            return ImmutableGeneratedCacheKey.of(objectArray);
        }
    }
}
