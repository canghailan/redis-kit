package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.DateTimeCodec;
import cc.whohow.redis.util.impl.DateRange;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * 时间窗口计数器（通过accuracy指定时间精度）
 */
public class RedisTimeWindowCounter extends RedisWindowCounter<Date> {
    private static final Logger LOG = LogManager.getLogger(RedisTimeWindowCounter.class);
    protected final Duration accuracy;

    public RedisTimeWindowCounter(RedisCommands<ByteBuffer, ByteBuffer> redis, String key, Duration accuracy) {
        this(redis, ByteBuffers.fromUtf8(key), accuracy);
    }

    public RedisTimeWindowCounter(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, Duration accuracy) {
        super(redis, DateTimeCodec.Accuracy.date(accuracy), key);
        this.accuracy = accuracy;
    }

    /**
     * 指定时间段内计数总数
     */
    public long sum(Date startInclusive, Date endExclusive) {
        LOG.trace("sum({}, {})", startInclusive, endExclusive);
        return sum(new DateRange(startInclusive, endExclusive, accuracy));
    }

    /**
     * 指定时间段内计数总数
     */
    public long sum(ZonedDateTime startInclusive, ZonedDateTime endExclusive) {
        LOG.trace("sum({}, {})", startInclusive, endExclusive);
        return sum(new DateRange(startInclusive, endExclusive, accuracy));
    }

    /**
     * 最近时间段内计数总数
     */
    public long sumLast(Duration duration) {
        LOG.trace("sumLast({})", duration);
        return sumLast((int) (duration.toMillis() / accuracy.toMillis()));
    }

    /**
     * 最近N个计数总数
     */
    public long sumLast(int n) {
        LOG.trace("sumLast({})", n);
        long t = System.currentTimeMillis();
        Date[] window = new Date[n];
        for (int i = 0; i < window.length; i++) {
            window[i] = new Date(t - accuracy.toMillis() * i);
        }
        return sum(window);
    }

    /**
     * 仅保留最近时间段内计数，移除其他计数窗口
     */
    public void retainLast(Duration duration) {
        LOG.trace("retainLast({})", duration);
        long t = System.currentTimeMillis() - duration.toMillis();
        long ta = t - t % accuracy.toMillis();
        retainAfter(new Date(ta));
    }

    /**
     * 仅保留指定时间后计数，移除其他计数窗口
     */
    public void retainAfter(Date retain) {
        LOG.trace("retainLast({})", retain);
        removeIf(retain::after);
    }
}
