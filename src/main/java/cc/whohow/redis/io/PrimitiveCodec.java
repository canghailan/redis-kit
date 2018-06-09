package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

public class PrimitiveCodec<T> implements Codec<T> {
    public static final PrimitiveCodec<Boolean> BOOLEAN = new PrimitiveCodec<>(Boolean::parseBoolean);
    public static final PrimitiveCodec<Byte> BYTE = new PrimitiveCodec<>(Byte::parseByte);
    public static final PrimitiveCodec<Short> SHORT = new PrimitiveCodec<>(Short::parseShort);
    public static final PrimitiveCodec<Integer> INTEGER = new PrimitiveCodec<>(Integer::parseInt);
    public static final PrimitiveCodec<Long> LONG = new PrimitiveCodec<>(Long::parseLong);
    public static final PrimitiveCodec<Float> FLOAT = new PrimitiveCodec<>(Float::parseFloat);
    public static final PrimitiveCodec<Double> DOUBLE = new PrimitiveCodec<>(Double::parseDouble);

    private static final ByteBuffer NULL = ByteBuffer.allocate(0);

    private final Function<String, T> parse;

    public PrimitiveCodec(Function<String, T> parse) {
        this.parse = parse;
    }

    @Override
    public ByteBuffer encode(T value) {
        return value == null ? NULL : StandardCharsets.US_ASCII.encode(value.toString());
    }

    @Override
    public T decode(ByteBuffer bytes) {
        return (bytes != null && bytes.hasRemaining()) ?
                parse.apply(StandardCharsets.US_ASCII.decode(bytes).toString()) : null;
    }
}
