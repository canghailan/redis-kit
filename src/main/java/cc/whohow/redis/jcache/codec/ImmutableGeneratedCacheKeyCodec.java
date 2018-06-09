package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.*;
import cc.whohow.redis.jcache.ImmutableGeneratedCacheKey;
import cc.whohow.redis.lettuce.Lettuce;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class ImmutableGeneratedCacheKeyCodec implements Codec<ImmutableGeneratedCacheKey> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected Codec<ImmutableGeneratedCacheKey> delegate;

    public ImmutableGeneratedCacheKeyCodec(String... cacheKeyTypeCanonicalNames) {
        Objects.requireNonNull(cacheKeyTypeCanonicalNames);
        if (cacheKeyTypeCanonicalNames.length == 1) {
            switch (cacheKeyTypeCanonicalNames[0]) {
                case "java.lang.String": {
                    delegate = SingletonKeyCodec.STRING_KEY_CODEC;
                }
                case "java.lang.Integer": {
                    delegate = SingletonKeyCodec.INTEGER_KEY_CODEC;
                }
                case "java.lang.Long": {
                    delegate = SingletonKeyCodec.LONG_KEY_CODEC;
                }
                default: {
                    delegate = new SingletonKeyCodec(new JacksonCodec(cacheKeyTypeCanonicalNames[0]));
                }
            }
        } else if (cacheKeyTypeCanonicalNames.length == 0) {
            delegate = NoKeyCodec.INSTANCE;
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
            return Lettuce.NIL;
        }

        @Override
        public ImmutableGeneratedCacheKey decode(ByteBuffer bytes) {
            return ImmutableGeneratedCacheKey.empty();
        }
    }

    @SuppressWarnings("unchecked")
    private static class SingletonKeyCodec implements Codec<ImmutableGeneratedCacheKey> {
        static final SingletonKeyCodec STRING_KEY_CODEC = new SingletonKeyCodec(StringCodec.UTF_8);
        static final SingletonKeyCodec INTEGER_KEY_CODEC = new SingletonKeyCodec(PrimitiveCodec.INTEGER);
        static final SingletonKeyCodec LONG_KEY_CODEC = new SingletonKeyCodec(PrimitiveCodec.LONG);

        private final Codec keyCodec;

        private SingletonKeyCodec(Codec keyCodec) {
            this.keyCodec = keyCodec;
        }

        @Override
        public ByteBuffer encode(ImmutableGeneratedCacheKey value) {
            return keyCodec.encode(value.getKey(0));
        }

        @Override
        public ImmutableGeneratedCacheKey decode(ByteBuffer bytes) {
            return ImmutableGeneratedCacheKey.of(keyCodec.decode(bytes));
        }
    }

    private static class ArrayKeyCodec implements Codec<ImmutableGeneratedCacheKey> {
        private final JavaType[] types;

        private ArrayKeyCodec(String... canonicalName) {
            this.types = Arrays.stream(canonicalName)
                    .map(OBJECT_MAPPER.getTypeFactory()::constructFromCanonical)
                    .toArray(JavaType[]::new);
        }

        @Override
        public ByteBuffer encode(ImmutableGeneratedCacheKey value) {
            try {
                return ByteBuffer.wrap(OBJECT_MAPPER.writeValueAsBytes(value.getKeys()));
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public ImmutableGeneratedCacheKey decode(ByteBuffer bytes) {
            ArrayNode arrayNode = parse(bytes);
            Object[] objectArray = new Object[arrayNode.size()];
            for (int i = 0; i < arrayNode.size(); i++) {
                objectArray[i] = OBJECT_MAPPER.convertValue(arrayNode.get(i), types[i]);
            }
            return ImmutableGeneratedCacheKey.of(objectArray);
        }

        private ArrayNode parse(ByteBuffer bytes) {
            try {
                if (bytes.hasArray()) {
                    return OBJECT_MAPPER.readValue(bytes.array(), bytes.arrayOffset(), bytes.remaining(), ArrayNode.class);
                } else {
                    return OBJECT_MAPPER.readValue(new ByteBufferInputStream(bytes), ArrayNode.class);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
