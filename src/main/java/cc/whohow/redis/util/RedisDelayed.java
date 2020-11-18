package cc.whohow.redis.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Date;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class RedisDelayed<E> extends AbstractMap.SimpleImmutableEntry<E, Number> implements Delayed {
    public RedisDelayed(E element, long timestamp) {
        super(element, timestamp);
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
        return unit.convert(getValue().longValue(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }
}
