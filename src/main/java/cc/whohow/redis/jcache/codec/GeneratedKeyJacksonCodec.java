package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.codec.ObjectArrayJacksonCodec;
import cc.whohow.redis.codec.ObjectJacksonCodec;
import cc.whohow.redis.codec.WrapCodec;
import cc.whohow.redis.jcache.annotation.GeneratedKey;
import cc.whohow.redis.jcache.annotation.GeneratedSimpleKey;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.IntegerCodec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;

import java.util.Arrays;
import java.util.function.Function;

public class GeneratedKeyJacksonCodec extends WrapCodec {
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
        this(getDefaultCodec(types), getDefaultWrap(types), getDefaultUnwrap(types));
    }

    public GeneratedKeyJacksonCodec(Codec codec, Function<Object, GeneratedKey> wrap, Function<GeneratedKey, Object> unwrap) {
        super(codec, wrap, unwrap);
    }

    public static Codec getDefaultCodec(JavaType... types) {
        if (types.length == 1) {
            JavaType type = types[0];
            switch (type.toCanonical()) {
                case "java.lang.String": {
                    return StringCodec.INSTANCE;
                }
                case "java.lang.Integer": {
                    return IntegerCodec.INSTANCE;
                }
                case "java.lang.Long": {
                    return LongCodec.INSTANCE;
                }
                default: {
                    return new ObjectJacksonCodec(type);
                }
            }
        } else {
            return new ObjectArrayJacksonCodec(types);
        }
    }

    public static Codec getDefaultNullableCodec(JavaType... types) {
        if (types.length == 1) {
            return new ObjectJacksonCodec(types[0]);
        } else {
            return new ObjectArrayJacksonCodec(types);
        }
    }

    public static Function<Object, GeneratedKey> getDefaultWrap(JavaType... types) {
        return (types.length == 1) ? GeneratedKeyJacksonCodec::wrapObject : GeneratedKeyJacksonCodec::wrapObjectArray;
    }

    public static Function<GeneratedKey, Object> getDefaultUnwrap(JavaType... types) {
        return (types.length == 1) ? GeneratedKeyJacksonCodec::unwrapObject : GeneratedKeyJacksonCodec::unwrapObjectArray;
    }

    public static GeneratedKey wrapObject(Object object) {
        return GeneratedKey.of(object);
    }

    public static Object unwrapObject(GeneratedKey generatedKey) {
        return ((GeneratedSimpleKey) generatedKey).getKey();
    }

    public static GeneratedKey wrapObjectArray(Object object) {
        return GeneratedKey.of((Object[]) object);
    }

    public static Object unwrapObjectArray(GeneratedKey generatedKey) {
        return generatedKey.getKeys();
    }
}
