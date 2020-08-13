package cc.whohow.redis.util;

import cc.whohow.redis.io.PrimitiveCodec;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Redis时钟
 */
public class RedisClock extends Clock {
    private static final Logger log = LogManager.getLogger();
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final ZoneId zone;

    public RedisClock(RedisCommands<ByteBuffer, ByteBuffer> redis) {
        this(redis, ZoneId.systemDefault());
    }

    public RedisClock(RedisCommands<ByteBuffer, ByteBuffer> redis, ZoneId zone) {
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
        log.trace("TIME");
        List<ByteBuffer> time = redis.time();
        long s = PrimitiveCodec.LONG.decode(time.get(0));
        long ss = PrimitiveCodec.LONG.decode(time.get(1));
        return TimeUnit.SECONDS.toMillis(s) + TimeUnit.MICROSECONDS.toMillis(ss);
    }

    @Override
    public String toString() {
        return "time";
    }
}
