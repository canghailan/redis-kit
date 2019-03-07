package cc.whohow.redis.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class TimestampedValue<T> implements DelayedValue<T> {
    private final T value;
    private final long timestamp;

    public TimestampedValue(T value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public TimestampedValue(T value, Instant instant) {
        this(value, instant.toEpochMilli());
    }

    public TimestampedValue(T value, Date date) {
        this(value, date.getTime());
    }

    public TimestampedValue(T value, Duration delay) {
        this(value, delay, Clock.systemDefaultZone());
    }

    public TimestampedValue(T value, Duration delay, Clock clock) {
        this(value, clock.millis() + delay.toMillis());
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(timestamp, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public T get() {
        return value;
    }

    @Override
    public String toString() {
        return Objects.toString(value) + ":" + timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DelayedValue) {
            DelayedValue delayedValue = (DelayedValue) o;
            return getDelay(TimeUnit.MILLISECONDS) == delayedValue.getDelay(TimeUnit.MILLISECONDS) &&
                    Objects.equals(get(), delayedValue.get());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(get(), getDelay(TimeUnit.MILLISECONDS));
    }
}
