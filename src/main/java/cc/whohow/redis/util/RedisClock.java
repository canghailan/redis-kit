package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.lettuce.TimeOutput;
import io.lettuce.core.protocol.CommandType;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * Redis时钟
 */
public class RedisClock extends Clock {
    protected final Redis redis;
    protected final ZoneId zone;

    public RedisClock(Redis redis) {
        this(redis, ZoneId.systemDefault());
    }

    public RedisClock(Redis redis, ZoneId zone) {
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
        return Instant.ofEpochMilli(millis());
    }

    @Override
    public long millis() {
        return TimeUnit.MICROSECONDS.toMillis(redis.send(new TimeOutput(), CommandType.TIME));
    }

    @Override
    public String toString() {
        return "time";
    }
}
