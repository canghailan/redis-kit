package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * 原始类型编码器
 */
public class PrimitiveCodec<T> extends AbstractCodec<T> {
    public static final PrimitiveCodec<Boolean> BOOLEAN = new PrimitiveCodec<>(Boolean::parseBoolean, 5);
    public static final PrimitiveCodec<Byte> BYTE = new PrimitiveCodec<>(Byte::parseByte, 4);
    public static final PrimitiveCodec<Short> SHORT = new PrimitiveCodec<>(Short::parseShort, 6);
    public static final PrimitiveCodec<Integer> INTEGER = new PrimitiveCodec<>(Integer::parseInt, 11);
    public static final PrimitiveCodec<Long> LONG = new PrimitiveCodec<>(Long::parseLong, 20);
    public static final PrimitiveCodec<Float> FLOAT = new PrimitiveCodec<>(Float::parseFloat, 32);
    public static final PrimitiveCodec<Double> DOUBLE = new PrimitiveCodec<>(Double::parseDouble, 32);

    private final int bufferSize;
    private final Function<String, T> parse;

    public PrimitiveCodec(Function<String, T> parse) {
        this(parse, 32);
    }

    public PrimitiveCodec(Function<String, T> parse, int bufferSize) {
        this.parse = parse;
        this.bufferSize = bufferSize;
    }

    @Override
    protected int getBufferSize() {
        return bufferSize;
    }

    @Override
    public ByteBuffer encode(T value) {
        return (value == null) ? ByteBuffers.empty() : StandardCharsets.US_ASCII.encode(value.toString());
    }

    @Override
    public T decode(ByteBuffer buffer) {
        return ByteBuffers.isEmpty(buffer) ? null : parse.apply(StandardCharsets.US_ASCII.decode(buffer).toString());
    }
}
