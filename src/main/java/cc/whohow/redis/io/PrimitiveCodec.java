package cc.whohow.redis.io;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * 原始类型编码器
 */
public class PrimitiveCodec<T> implements Codec<T> {
    public static final PrimitiveCodec<Boolean> BOOLEAN = new PrimitiveCodec<>(Boolean::parseBoolean);
    public static final PrimitiveCodec<Byte> BYTE = new PrimitiveCodec<>(Byte::parseByte);
    public static final PrimitiveCodec<Short> SHORT = new PrimitiveCodec<>(Short::parseShort);
    public static final PrimitiveCodec<Integer> INTEGER = new PrimitiveCodec<>(Integer::parseInt);
    public static final PrimitiveCodec<Long> LONG = new PrimitiveCodec<>(Long::parseLong);
    public static final PrimitiveCodec<Float> FLOAT = new PrimitiveCodec<>(Float::parseFloat);
    public static final PrimitiveCodec<Double> DOUBLE = new PrimitiveCodec<>(Double::parseDouble);
    public static final PrimitiveCodec<Number> NUMBER = new PrimitiveCodec<>(BigDecimal::new);
    public static final PrimitiveCodec<BigInteger> BIGINT = new PrimitiveCodec<>(BigInteger::new);

    private final Function<String, T> parse;

    public PrimitiveCodec(Function<String, T> parse) {
        this.parse = parse;
    }

    @Override
    public ByteBuffer encode(T value) {
        return (value == null) ? ByteBuffers.empty() : ByteBuffers.from(value.toString(), StandardCharsets.US_ASCII);
    }

    @Override
    public T decode(ByteBuffer buffer) {
        return decode(buffer, null);
    }

    public T decode(ByteBuffer buffer, T defaultValue) {
        return ByteBuffers.isEmpty(buffer) ? defaultValue : parse.apply(ByteBuffers.toString(buffer, StandardCharsets.US_ASCII));
    }
}
