package cc.whohow.redis.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class RedisDelayed<E> implements Delayed, Map.Entry<E, Number> {
    private final E element;
    private final long timestamp;

    public RedisDelayed(E element, long timestamp) {
        this.element = element;
        this.timestamp = timestamp;
    }

    public RedisDelayed(E element, Instant instant) {
        this(element, instant.toEpochMilli());
    }

    public RedisDelayed(E element, Date date) {
        this(element, date.getTime());
    }

    public RedisDelayed(E element, Duration delay) {
        this(element, delay, Clock.systemDefaultZone());
    }

    public RedisDelayed(E element, Duration delay, Clock clock) {
        this(element, clock.millis() + delay.toMillis());
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
    public E getKey() {
        return element;
    }

    @Override
    public Number getValue() {
        return timestamp;
    }

    @Override
    public Number setValue(Number value) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof RedisDelayed) {
            RedisDelayed that = (RedisDelayed) o;
            return getDelay(TimeUnit.MILLISECONDS) == that.getDelay(TimeUnit.MILLISECONDS) &&
                    Objects.equals(getKey(), that.getKey());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKey(), getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public String toString() {
        return timestamp + " " + element;
    }
}
