package cc.whohow.redis.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class RedisDelayed<T> implements Delayed, Supplier<T> {
    private final T value;
    private final long timestamp;

    public RedisDelayed(T value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public RedisDelayed(T value, Instant instant) {
        this(value, instant.toEpochMilli());
    }

    public RedisDelayed(T value, Date date) {
        this(value, date.getTime());
    }

    public RedisDelayed(T value, Duration delay) {
        this(value, delay, Clock.systemDefaultZone());
    }

    public RedisDelayed(T value, Duration delay, Clock clock) {
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
        return value + ":" + timestamp;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object o) {
        if (o instanceof RedisDelayed) {
            RedisDelayed RedisDelayed = (RedisDelayed) o;
            return getDelay(TimeUnit.MILLISECONDS) == RedisDelayed.getDelay(TimeUnit.MILLISECONDS) &&
                    Objects.equals(get(), RedisDelayed.get());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(get(), getDelay(TimeUnit.MILLISECONDS));
    }
}
