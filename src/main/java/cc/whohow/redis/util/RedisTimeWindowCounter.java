package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.PrimitiveCodec;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 时间窗口计数器（通过accuracy指定时间精度）
 */
public class RedisTimeWindowCounter extends RedisWindowCounter<Date> {
    private static final Logger log = LogManager.getLogger(RedisTimeWindowCounter.class);
    protected final Duration accuracy;

    public RedisTimeWindowCounter(RedisCommands<ByteBuffer, ByteBuffer> redis, String key, Duration accuracy) {
        this(redis, ByteBuffers.fromUtf8(key), accuracy);
    }

    public RedisTimeWindowCounter(RedisCommands<ByteBuffer, ByteBuffer> redis, ByteBuffer key, Duration accuracy) {
        super(redis, new DateCodec(accuracy.toMillis()), key);
        this.accuracy = accuracy;
    }

    protected Date reduceAccuracy(Date date) {
        return new Date(reduceAccuracy(date.getTime()));
    }

    protected long reduceAccuracy(long timestamp) {
        return timestamp - timestamp % accuracy.toMillis();
    }

    protected long count(Duration duration) {
        return duration.toMillis() / accuracy.toMillis();
    }

    protected long count(Date start, Date end) {
        return (end.getTime() - start.getTime()) / accuracy.toMillis();
    }

    /**
     * 指定时间段内计数总数
     */
    public long sum(Date startInclusive, Date endExclusive) {
        Date start = reduceAccuracy(startInclusive);
        Date end = reduceAccuracy(endExclusive);
        log.trace("sum({}, {})", start, end);
        List<Date> list = new ArrayList<>((int) count(start, end));
        for (Date date = start; date.before(end); date = new Date(date.getTime() + accuracy.toMillis())) {
            list.add(date);
        }
        return sum(list);
    }

    /**
     * 指定时间段内计数总数
     */
    public long sum(ZonedDateTime startInclusive, ZonedDateTime endExclusive) {
        return sum(Date.from(startInclusive.toInstant()), Date.from(endExclusive.toInstant()));
    }

    /**
     * 最近时间段内计数总数
     */
    public long sumLast(Duration duration) {
        log.trace("sumLast({})", duration);
        return sumLast((int) count(duration));
    }

    /**
     * 最近N个计数总数
     */
    public long sumLast(int n) {
        log.trace("sumLast({})", n);
        long timestamp = reduceAccuracy(System.currentTimeMillis());
        Date[] window = new Date[n];
        for (int i = 0; i < window.length; i++) {
            window[i] = new Date(timestamp - accuracy.toMillis() * i);
        }
        return sum(window);
    }

    /**
     * 仅保留最近时间段内计数，移除其他计数窗口
     */
    public void retainLast(Duration duration) {
        log.trace("retainLast({})", duration);
        retainAfter(new Date(reduceAccuracy(System.currentTimeMillis() - duration.toMillis())));
    }

    /**
     * 仅保留指定时间后计数，移除其他计数窗口
     */
    public void retainAfter(Date retain) {
        log.trace("retainLast({})", retain);
        removeIf(retain::after);
    }

    public static class DateCodec implements Codec<Date> {
        protected final long accuracy;

        public DateCodec(long accuracy) {
            this.accuracy = accuracy;
        }

        @Override
        public ByteBuffer encode(Date value) {
            if (value == null) {
                return ByteBuffers.empty();
            }
            return PrimitiveCodec.LONG.encode(reduceAccuracy(value.getTime()));
        }

        @Override
        public Date decode(ByteBuffer buffer) {
            Long timestamp = PrimitiveCodec.LONG.decode(buffer);
            if (timestamp == null) {
                return null;
            }
            return new Date(timestamp);
        }

        protected long reduceAccuracy(long timestamp) {
            return timestamp - timestamp % accuracy;
        }
    }
}
