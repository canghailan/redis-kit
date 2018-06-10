package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.*;
import cc.whohow.redis.jcache.ImmutableGeneratedCacheKey;
import cc.whohow.redis.lettuce.Lettuce;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
                    delegate = new SingletonKeyCodec(new StringCodec());
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
    public ImmutableGeneratedCacheKey decode(ByteBuffer buffer) {
        return delegate.decode(buffer);
    }

    @Override
    public void encode(ImmutableGeneratedCacheKey value, OutputStream stream) throws IOException {
        delegate.encode(value, stream);
    }

    @Override
    public ImmutableGeneratedCacheKey decode(InputStream stream) throws IOException {
        return delegate.decode(stream);
    }

    private static class NoKeyCodec implements Codec<ImmutableGeneratedCacheKey> {
        static final NoKeyCodec INSTANCE = new NoKeyCodec();

        @Override
        public ByteBuffer encode(ImmutableGeneratedCacheKey value) {
            return Lettuce.NIL;
        }

        @Override
        public ImmutableGeneratedCacheKey decode(ByteBuffer buffer) {
            return ImmutableGeneratedCacheKey.empty();
        }

        @Override
        public void encode(ImmutableGeneratedCacheKey value, OutputStream stream) throws IOException {
        }

        @Override
        public ImmutableGeneratedCacheKey decode(InputStream stream) throws IOException {
            return ImmutableGeneratedCacheKey.empty();
        }
    }

    @SuppressWarnings("unchecked")
    private static class SingletonKeyCodec implements Codec<ImmutableGeneratedCacheKey> {
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
        public ImmutableGeneratedCacheKey decode(ByteBuffer buffer) {
            return ImmutableGeneratedCacheKey.of(keyCodec.decode(buffer));
        }

        @Override
        public void encode(ImmutableGeneratedCacheKey value, OutputStream stream) throws IOException {
            keyCodec.encode(value.getKey(0), stream);
        }

        @Override
        public ImmutableGeneratedCacheKey decode(InputStream stream) throws IOException {
            return ImmutableGeneratedCacheKey.of(keyCodec.decode(stream));
        }
    }

    private static class ArrayKeyCodec extends AbstractCodec<ImmutableGeneratedCacheKey> {
        private final JavaType[] types;

        private ArrayKeyCodec(String... canonicalName) {
            this.types = Arrays.stream(canonicalName)
                    .map(OBJECT_MAPPER.getTypeFactory()::constructFromCanonical)
                    .toArray(JavaType[]::new);
        }

        @Override
        public void encode(ImmutableGeneratedCacheKey value, OutputStream stream) throws IOException {
            OBJECT_MAPPER.writeValue(stream, value.getKeys());
        }

        @Override
        public ImmutableGeneratedCacheKey decode(InputStream stream) throws IOException {
            ArrayNode arrayNode = OBJECT_MAPPER.readValue(stream, ArrayNode.class);
            Object[] objectArray = new Object[arrayNode.size()];
            for (int i = 0; i < arrayNode.size(); i++) {
                objectArray[i] = OBJECT_MAPPER.convertValue(arrayNode.get(i), types[i]);
            }
            return ImmutableGeneratedCacheKey.of(objectArray);
        }
    }
}
