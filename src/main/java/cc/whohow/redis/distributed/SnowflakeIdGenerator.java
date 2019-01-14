package cc.whohow.redis.distributed;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Snowflake算法
 */
public class SnowflakeIdGenerator implements Supplier<Number>, LongSupplier {
    protected static final long EPOCH_2000 = 946684800000L; // UTC 2000-01-01 00:00:00

    protected final TimeUnit timeUnit;
    protected final long timestampMask;
    protected final long workerIdMask;
    protected final long sequenceMask;
    protected final long timestampShift;
    protected final long workerIdShift;
    protected final long epoch;

    protected volatile long timestamp;
    protected volatile long workerId;
    protected volatile long sequence;

    public SnowflakeIdGenerator() {
        this(EPOCH_2000);
    }

    public SnowflakeIdGenerator(long epoch) {
        // snowflake-64bit: [0][41bit timestamp][10bit worker id][12bit sequence]
        this(TimeUnit.MILLISECONDS, 41L, 10L, 12L, epoch);
    }

    public SnowflakeIdGenerator(TimeUnit timeUnit,
                                long timestampBits, long workerIdBits, long sequenceBits,
                                long epoch) {
        this.timestamp = -1;
        this.workerId = -1;
        this.sequence = 0;
        this.timestampMask = -1L ^ (-1L << timestampBits);
        this.workerIdMask = -1L ^ (-1L << workerIdBits);
        this.sequenceMask = -1L ^ (-1L << sequenceBits);
        this.timestampShift = workerIdBits + sequenceBits;
        this.workerIdShift = sequenceBits;
        this.epoch = epoch;
        this.timeUnit = timeUnit;
    }

    /**
     * 1970-01-01 00:00:00+00
     */
    public long snowflake(long timestamp, long workerId, long sequence, long epoch) {
        long t = timeUnit.convert(timestamp - epoch, TimeUnit.MILLISECONDS);
        if (t < 0 || t > timestampMask) {
            throw new IllegalArgumentException();
        }
        if (workerId < 0 || workerId > workerIdMask) {
            throw new IllegalArgumentException();
        }
        if (sequence < 0 || sequence > sequenceMask) {
            throw new IllegalArgumentException();
        }
        return (t << timestampShift) | (workerId << workerIdShift) | sequence;
    }

    @Override
    public Number get() {
        return getAsLong();
    }

    @Override
    public long getAsLong() {
        long i = getWorkerId();
        long t = getTime();
        synchronized (this) {
            if (t == timestamp && i == workerId) {
                sequence = (sequence + 1) & sequenceMask;
                if (sequence == 0) {
                    timestamp = await(timestamp);
                }
            } else {
                timestamp = t;
                workerId = i;
                sequence = 0;
            }
            return snowflake(timestamp, workerId, sequence, epoch);
        }
    }

    protected long getTime() {
        return System.currentTimeMillis();
    }

    protected long getWorkerId() {
        return 0;
    }

    protected long await(long timestamp) {
        long t = getTime();
        while (t < timestamp) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(t - timestamp));
            t = getTime();
        }
        return t;
    }
}
