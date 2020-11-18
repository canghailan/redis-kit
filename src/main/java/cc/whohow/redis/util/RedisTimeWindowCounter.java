package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.codec.Codec;
import cc.whohow.redis.codec.PrimitiveCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.function.Predicate;

/**
 * 时间窗口计数器（通过accuracy指定时间精度）
 */
public class RedisTimeWindowCounter extends RedisWindowCounter<Date> {
    private static final Logger log = LogManager.getLogger();

    protected final Duration accuracy;

    public RedisTimeWindowCounter(Redis redis, String key, Duration accuracy) {
        super(redis, new DateAccuracyCodec(accuracy.toMillis()), key);
        this.accuracy = accuracy;
    }

    protected Date reduceAccuracy(Date date) {
        return new Date(reduceAccuracy(date.getTime()));
    }

    protected long reduceAccuracy(long timestamp) {
        return timestamp - timestamp % accuracy.toMillis();
    }

    protected long count(long millis) {
        return millis / accuracy.toMillis();
    }

    protected long count(Duration duration) {
        return count(duration.toMillis());
    }

    protected long count(Date start, Date end) {
        return count(end.getTime() - start.getTime());
    }

    protected Date[] window(Date start, Duration duration) {
        int n = (int) count(duration);
        long timestamp = reduceAccuracy(start.getTime());
        Date[] window = new Date[n];
        for (int i = 0; i < n; i++) {
            window[i] = new Date(timestamp + accuracy.toMillis() * i);
        }
        return window;
    }

    protected Date[] last(Duration duration) {
        return last((int) count(duration));
    }

    protected Date[] last(int n) {
        long timestamp = reduceAccuracy(System.currentTimeMillis());
        Date[] window = new Date[n];
        for (int i = 0; i < n; i++) {
            window[n - 1 - i] = new Date(timestamp - accuracy.toMillis() * i);
        }
        return window;
    }

    /**
     * 指定时间段内计数总数
     */
    public long sum(Date start, Duration duration) {
        log.trace("sum({}, {})", start, duration);
        return sum(window(start, duration));
    }

    /**
     * 最近时间段内计数总数
     */
    public long sumLast(Duration duration) {
        log.trace("sumLast({})", duration);
        return sum(last(duration));
    }

    /**
     * 仅保留最近时间段内计数，移除其他计数窗口
     */
    public void retainLast(Duration duration) {
        log.trace("retainLast({})", duration);
        Predicate<Date> isLast = new HashSet<>(Arrays.asList(last(duration)))::contains;
        removeIf(isLast.negate());
    }

    public static class DateAccuracyCodec implements Codec<Date> {
        protected final long accuracy;

        public DateAccuracyCodec(long accuracy) {
            this.accuracy = accuracy;
        }

        @Override
        public ByteSequence encode(Date value) {
            if (value == null) {
                return ByteSequence.empty();
            }
            return PrimitiveCodec.LONG.encode(reduceAccuracy(value.getTime()));
        }

        @Override
        public Date decode(ByteSequence buffer) {
            Long timestamp = PrimitiveCodec.LONG.decode(buffer);
            if (timestamp == null) {
                return null;
            }
            return new Date(timestamp);
        }

        @Override
        public Date decode(byte... buffer) {
            Long timestamp = PrimitiveCodec.LONG.decode(buffer);
            if (timestamp == null) {
                return null;
            }
            return new Date(timestamp);
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
