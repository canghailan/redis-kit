package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.io.*;
import cc.whohow.redis.jcache.ImmutableGeneratedCacheKey;
import cc.whohow.redis.jcache.configuration.RedisCacheConfiguration;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/**
 * ImmutableGeneratedCacheKey编码器默认实现，简单类型特殊处理，复杂类型基于json
 */
public class ImmutableGeneratedCacheKeyCodecFactory
        implements Function<RedisCacheConfiguration, Codec> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public Codec apply(RedisCacheConfiguration configuration) {
        return create(configuration.getKeyTypeCanonicalName());
    }

    public Codec<ImmutableGeneratedCacheKey> create(String... cacheKeyTypeCanonicalNames) {
        Objects.requireNonNull(cacheKeyTypeCanonicalNames);
        switch (cacheKeyTypeCanonicalNames.length) {
            case 1: {
                switch (cacheKeyTypeCanonicalNames[0]) {
                    case "java.lang.String": {
                        return new SingletonKeyCodec(new StringCodec());
                    }
                    case "java.lang.Integer": {
                        return SingletonKeyCodec.INTEGER_KEY_CODEC;
                    }
                    case "java.lang.Long": {
                        return SingletonKeyCodec.LONG_KEY_CODEC;
                    }
                    default: {
                        return new SingletonKeyCodec(new JacksonCodec(OBJECT_MAPPER, cacheKeyTypeCanonicalNames[0]));
                    }
                }
            }
            case 0: {
                return NoKeyCodec.INSTANCE;
            }
            default: {
                return new ArrayKeyCodec(cacheKeyTypeCanonicalNames);
            }
        }
    }

    private static class NoKeyCodec implements Codec<ImmutableGeneratedCacheKey> {
        static final NoKeyCodec INSTANCE = new NoKeyCodec();

        @Override
        public ByteBuffer encode(ImmutableGeneratedCacheKey value) {
            return ByteBuffers.empty();
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

    private static class ArrayKeyCodec extends AbstractAdaptiveCodec<ImmutableGeneratedCacheKey> {
        private final JavaType[] types;

        private ArrayKeyCodec(String... canonicalName) {
            this.types = Arrays.stream(canonicalName)
                    .map(OBJECT_MAPPER.getTypeFactory()::constructFromCanonical)
                    .toArray(JavaType[]::new);
        }

        @Override
        public void encodeToStream(ImmutableGeneratedCacheKey value, OutputStream stream) throws IOException {
            OBJECT_MAPPER.writeValue(stream, value.getKeys());
        }

        @Override
        public ImmutableGeneratedCacheKey decodeStream(InputStream stream) throws IOException {
            ArrayNode arrayNode = OBJECT_MAPPER.readValue(stream, ArrayNode.class);
            Object[] objectArray = new Object[arrayNode.size()];
            for (int i = 0; i < arrayNode.size(); i++) {
                objectArray[i] = OBJECT_MAPPER.convertValue(arrayNode.get(i), types[i]);
            }
            return ImmutableGeneratedCacheKey.of(objectArray);
        }
    }
}