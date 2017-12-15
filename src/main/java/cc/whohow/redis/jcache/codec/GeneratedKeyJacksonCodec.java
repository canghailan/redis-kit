package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.jcache.annotation.GeneratedKey;
import cc.whohow.redis.jcache.annotation.GeneratedSimpleKey;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.netty.buffer.ByteBuf;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

public class GeneratedKeyJacksonCodec implements Codec {
    private final Codec codec;
    private final Function<Object, GeneratedKey> wrap;
    private final Function<Object, Object> unwrap;

    private final Encoder encoder = new Encoder() {
        @Override
        public ByteBuf encode(Object in) throws IOException {
            return codec.getValueEncoder().encode(unwrap.apply(in));
        }
    };

    private final Decoder<Object> decoder = new Decoder<Object>() {
        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            return wrap.apply(codec.getValueDecoder().decode(buf, state));
        }
    };

    public GeneratedKeyJacksonCodec(String... typeCanonicalNames) {
        this(Arrays.stream(typeCanonicalNames)
                .map(TypeFactory.defaultInstance()::constructFromCanonical)
                .toArray(JavaType[]::new));
    }

    public GeneratedKeyJacksonCodec(Class<?>... types) {
        this(Arrays.stream(types)
                .map(TypeFactory.defaultInstance()::constructType)
                .toArray(JavaType[]::new));
    }

    public GeneratedKeyJacksonCodec(JavaType... types) {
        if (types.length == 1) {
            codec = new ObjectJacksonCodec(types[0]);
            wrap = this::wrapObject;
            unwrap = this::unwrapObject;
        } else {
            codec = new ObjectArrayJacksonCodec(types);
            wrap = this::wrapObjectArray;
            unwrap = this::unwrapObjectArray;
        }
    }

    private GeneratedKey wrapObject(Object object) {
        return GeneratedKey.of(object);
    }

    private Object unwrapObject(Object generatedKey) {
        return ((GeneratedSimpleKey) generatedKey).getKey();
    }

    private GeneratedKey wrapObjectArray(Object object) {
        return GeneratedKey.of((Object[]) object);
    }

    private Object unwrapObjectArray(Object generatedKey) {
        return ((GeneratedKey) generatedKey).getKeys();
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
}
