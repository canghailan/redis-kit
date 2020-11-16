package cc.whohow.redis.codec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Function;

public class DefaultCodecFactory implements Function<Class<?>, Codec<?>> {
    private static final Map<Class<?>, Codec<?>> DEFAULTS = new IdentityHashMap<>();

    static {
        DEFAULTS.put(boolean.class, PrimitiveCodec.BOOLEAN);
        DEFAULTS.put(Boolean.class, PrimitiveCodec.BOOLEAN);
        DEFAULTS.put(byte.class, PrimitiveCodec.BYTE);
        DEFAULTS.put(Byte.class, PrimitiveCodec.BYTE);
        DEFAULTS.put(short.class, PrimitiveCodec.SHORT);
        DEFAULTS.put(Short.class, PrimitiveCodec.SHORT);
        DEFAULTS.put(int.class, PrimitiveCodec.INTEGER);
        DEFAULTS.put(Integer.class, PrimitiveCodec.INTEGER);
        DEFAULTS.put(long.class, PrimitiveCodec.LONG);
        DEFAULTS.put(Long.class, PrimitiveCodec.LONG);
        DEFAULTS.put(float.class, PrimitiveCodec.FLOAT);
        DEFAULTS.put(Float.class, PrimitiveCodec.FLOAT);
        DEFAULTS.put(double.class, PrimitiveCodec.DOUBLE);
        DEFAULTS.put(Double.class, PrimitiveCodec.DOUBLE);
        DEFAULTS.put(BigDecimal.class, PrimitiveCodec.NUMBER);
        DEFAULTS.put(BigInteger.class, PrimitiveCodec.BIGINT);
        DEFAULTS.put(String.class, StringCodec.UTF8.get());
        DEFAULTS.put(ByteBuffer.class, ByteBufferCodec.get());
    }

    @Override
    public Codec<?> apply(Class<?> type) {
        return DEFAULTS.computeIfAbsent(type, JacksonCodec::new);
    }
}
