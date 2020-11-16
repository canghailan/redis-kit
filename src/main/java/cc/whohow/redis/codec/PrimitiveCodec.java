package cc.whohow.redis.codec;

import cc.whohow.redis.bytes.ByteSequence;

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
    public ByteSequence encode(T value) {
        return (value == null) ? ByteSequence.empty() : ByteSequence.ascii(value.toString());
    }

    public T decode(String buffer) {
        return decode(buffer, null);
    }

    public T decode(String buffer, T defaultValue) {
        if (buffer == null || buffer.isEmpty()) {
            return defaultValue;
        }
        return parse.apply(buffer);
    }

    @Override
    public T decode(ByteSequence buffer) {
        return decode(buffer, null);
    }

    public T decode(ByteSequence buffer, T defaultValue) {
        if (buffer == null || buffer.isEmpty()) {
            return defaultValue;
        }
        return parse.apply(buffer.toString(StandardCharsets.US_ASCII));
    }

    @Override
    public T decode(byte... buffer) {
        return decode(buffer);
    }

    public T decode(byte[] buffer, T defaultValue) {
        if (buffer == null || buffer.length == 0) {
            return defaultValue;
        }
        return parse.apply(new String(buffer, StandardCharsets.US_ASCII));
    }

    @Override
    public T decode(ByteBuffer buffer) {
        return decode(buffer, null);
    }

    public T decode(ByteBuffer buffer, T defaultValue) {
        if (buffer == null || !buffer.hasRemaining()) {
            return defaultValue;
        }
        return parse.apply(StandardCharsets.US_ASCII.decode(buffer).toString());
    }
}
