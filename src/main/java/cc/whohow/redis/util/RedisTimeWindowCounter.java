package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.DateTimeCodec;
import cc.whohow.redis.util.impl.DateRange;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * 时间窗口计数器（通过accuracy指定时间精度）
 */
public class RedisTimeWindowCounter extends RedisWindowCounter<Date> {
    protected final Duration accuracy;

    public RedisTimeWindowCounter(RedisCommands<ByteBuffer, ByteBuffer> redis, String key, Duration accuracy) {
        this(redis, ByteBuffers.fromUtf8(key), accuracy);
    }

    public RedisTimeWindowCounter(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, Duration accuracy) {
        super(redis, DateTimeCodec.date(accuracy), key);
        this.accuracy = accuracy;
    }

    /**
     * 指定时间段内计数总数
     */
    public long sum(Date startInclusive, Date endExclusive) {
        return sum(new DateRange(startInclusive, endExclusive, accuracy));
    }

    /**
     * 指定时间段内计数总数
     */
    public long sum(ZonedDateTime startInclusive, ZonedDateTime endExclusive) {
        return sum(new DateRange(startInclusive, endExclusive, accuracy));
    }

    /**
     * 最近时间段内计数总数
     */
    public long sumLast(Duration duration) {
        Date now = new Date();
        return sum(new Date(now.getTime() - duration.toMillis()), now);
    }

    /**
     * 仅保留最近时间段内计数，移除其他计数窗口
     */
    public void retainLast(Duration duration) {
        long timestamp = (System.currentTimeMillis() - duration.toMillis())
                / accuracy.toMillis() * accuracy.toMillis();
        removeIf(new Date(timestamp)::after);
    }
}
