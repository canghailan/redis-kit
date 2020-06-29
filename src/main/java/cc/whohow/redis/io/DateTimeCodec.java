package cc.whohow.redis.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.function.Function;

public class DateTimeCodec<T> implements Codec<T> {
    private static final DateTimeCodec<Date> DATE = new DateTimeCodec<>(Date::getTime, Date::new);
    private static final DateTimeCodec<Long> MILLIS = new DateTimeCodec<>(Function.identity(), Function.identity());

    protected final Function<T, Long> toEpochMilli;
    protected final Function<Long, T> ofEpochMilli;

    public DateTimeCodec(Function<T, Long> toEpochMilli, Function<Long, T> ofEpochMilli) {
        this.toEpochMilli = toEpochMilli;
        this.ofEpochMilli = ofEpochMilli;
    }

    public static DateTimeCodec<Date> date() {
        return DATE;
    }

    public static DateTimeCodec<Long> millis() {
        return MILLIS;
    }

    @Override
    public ByteBuffer encode(T value) {
        if (value == null) {
            return ByteBuffers.empty();
        }
        return ByteBuffers.from(Long.toString(toEpochMilli.apply(value)), StandardCharsets.US_ASCII);
    }

    @Override
    public T decode(ByteBuffer buffer) {
        return decode(buffer, null);
    }

    public T decode(ByteBuffer buffer, T defaultValue) {
        if (ByteBuffers.isEmpty(buffer)) {
            return defaultValue;
        }
        return ofEpochMilli.apply(Long.parseLong(ByteBuffers.toString(buffer, StandardCharsets.US_ASCII)));
    }

    public static class Accuracy<T> extends DateTimeCodec<T> {
        protected final long accuracy;

        public Accuracy(Function<T, Long> toEpochMilli, Function<Long, T> ofEpochMilli, Duration accuracy) {
            super(toEpochMilli, ofEpochMilli);
            this.accuracy = accuracy.toMillis();
        }

        public static DateTimeCodec<Date> date(Duration accuracy) {
            return new Accuracy<>(Date::getTime, Date::new, accuracy);
        }

        public static DateTimeCodec<Long> millis(Duration accuracy) {
            return new Accuracy<>(Function.identity(), Function.identity(), accuracy);
        }

        @Override
        public ByteBuffer encode(T value) {
            if (value == null) {
                return ByteBuffers.empty();
            }
            long t = toEpochMilli.apply(value);
            long ta = t - t % accuracy;
            return ByteBuffers.from(Long.toString(ta), StandardCharsets.US_ASCII);
        }
    }
}
