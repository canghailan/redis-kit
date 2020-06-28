package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.function.Function;

public class DateTimeCodec<T> implements Codec<T> {
    private static final DateTimeCodec<Date> DATE = date(Duration.ofMillis(1));
    private static final DateTimeCodec<Long> MILLIS = millis(Duration.ofMillis(1));

    protected final Function<T, Long> toEpochMilli;
    protected final Function<Long, T> ofEpochMilli;
    protected final long accuracy;

    public DateTimeCodec(Function<T, Long> toEpochMilli, Function<Long, T> ofEpochMilli, Duration accuracy) {
        this.toEpochMilli = toEpochMilli;
        this.ofEpochMilli = ofEpochMilli;
        this.accuracy = accuracy.toMillis();
    }

    public static DateTimeCodec<Date> date() {
        return DATE;
    }

    public static DateTimeCodec<Date> date(Duration accuracy) {
        return new DateTimeCodec<>(Date::getTime, Date::new, accuracy);
    }

    public static DateTimeCodec<Long> millis() {
        return MILLIS;
    }

    public static DateTimeCodec<Long> millis(Duration accuracy) {
        return new DateTimeCodec<>(Function.identity(), Function.identity(), accuracy);
    }

    @Override
    public ByteBuffer encode(T value) {
        if (value == null) {
            return ByteBuffers.empty();
        }
        long millis = toEpochMilli.apply(value) / accuracy;
        return ByteBuffers.from(Long.toString(millis), StandardCharsets.US_ASCII);
    }

    @Override
    public T decode(ByteBuffer buffer) {
        return decode(buffer, null);
    }

    public T decode(ByteBuffer buffer, T defaultValue) {
        if (ByteBuffers.isEmpty(buffer)) {
            return defaultValue;
        }
        long millis = Long.parseLong(ByteBuffers.toString(buffer, StandardCharsets.US_ASCII)) * accuracy;
        return ofEpochMilli.apply(millis);
    }
}
