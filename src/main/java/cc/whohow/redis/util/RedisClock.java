package cc.whohow.redis.util;

import io.lettuce.core.api.sync.RedisCommands;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Redis时钟
 */
public class RedisClock extends Clock {
    protected final RedisCommands<?, Long> redis;
    protected final ZoneId zone;

    public RedisClock(RedisCommands<?, Long> redis) {
        this(redis, ZoneId.systemDefault());
    }

    public RedisClock(RedisCommands<?, Long> redis, ZoneId zone) {
        this.redis = redis;
        this.zone = zone;
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new RedisClock(redis, zone);
    }

    @Override
    public Instant instant() {
        List<Long> time = redis.time();
        return Instant.ofEpochSecond(time.get(0), TimeUnit.MICROSECONDS.toNanos(time.get(1)));
    }

    @Override
    public long millis() {
        List<Long> time = redis.time();
        return TimeUnit.SECONDS.toMillis(time.get(0)) + TimeUnit.MICROSECONDS.toMillis(time.get(1));
    }
}
