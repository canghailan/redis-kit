package cc.whohow.redis.distributed;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Snowflake算法
 */
public class SnowflakeId implements Supplier<Number>, LongSupplier {
    public static final Instant Y2K = Instant.ofEpochMilli(946684800000L); // UTC 2000-01-01 00:00:00

    protected final TimeUnit timeUnit;
    protected final long timestampMask;
    protected final long workerIdMask;
    protected final long sequenceMask;
    protected final long timestampShift;
    protected final long workerIdShift;

    protected final Clock clock;
    protected final LongSupplier worker;
    protected final long epoch;
    protected volatile long workerId;
    protected volatile long timestamp;
    protected volatile long sequence;

    public SnowflakeId() {
        this(Clock.systemDefaultZone(), Worker.ZERO);
    }

    public SnowflakeId(Clock clock, LongSupplier worker) {
        this(clock, worker, Y2K);
    }

    public SnowflakeId(Clock clock, LongSupplier worker, Instant epoch) {
        this(clock, worker, epoch, TimeUnit.MILLISECONDS, 41L, 10L, 12L);
    }

    public SnowflakeId(Clock clock, LongSupplier worker, Instant epoch,
                       TimeUnit timeUnit, long timestampBits, long workerIdBits, long sequenceBits) {
        this.clock = clock;
        this.worker = worker;
        this.epoch = timeUnit.convert(epoch.toEpochMilli(), TimeUnit.MILLISECONDS);
        this.timeUnit = timeUnit;
        this.timestampMask = ~(-1L << timestampBits);
        this.workerIdMask = ~(-1L << workerIdBits);
        this.sequenceMask = ~(-1L << sequenceBits);
        this.timestampShift = workerIdBits + sequenceBits;
        this.workerIdShift = sequenceBits;
    }

    /**
     * 1970-01-01 00:00:00+00
     */
    public long snowflake(long timestamp, long workerId, long sequence) {
        if (timestamp < 0 || timestamp > timestampMask) {
            throw new IllegalArgumentException();
        }
        if (workerId < 0 || workerId > workerIdMask) {
            throw new IllegalArgumentException();
        }
        if (sequence < 0 || sequence > sequenceMask) {
            throw new IllegalArgumentException();
        }
        return (timestamp << timestampShift) | (workerId << workerIdShift) | sequence;
    }

    @Override
    public Number get() {
        return getAsLong();
    }

    public long getWorkerId() {
        return worker.getAsLong();
    }

    public long getTimestamp() {
        return getTimestamp(clock.millis());
    }

    public long getTimestamp(long millis) {
        return timeUnit.convert(millis, TimeUnit.MILLISECONDS);
    }

    @Override
    public long getAsLong() {
        long i = getWorkerId();
        long t = getTimestamp();
        synchronized (this) {
            if (t == timestamp && i == workerId) {
                sequence = (sequence + 1) & sequenceMask;
                if (sequence == 0) {
                    timestamp = await(timestamp + 1);
                }
            } else {
                workerId = i;
                timestamp = t;
                sequence = 0;
            }
            return snowflake(timestamp - epoch, workerId, sequence);
        }
    }

    public long random(long timestamp) {
        return snowflake(
                timeUnit.convert(timestamp, TimeUnit.MILLISECONDS) - epoch,
                workerId,
                ThreadLocalRandom.current().nextLong(sequenceMask >> 1, sequenceMask));
    }

    protected long await(long timestamp) {
        try {
            long t = getTimestamp();
            while (t < timestamp) {
                Thread.sleep(timeUnit.toMillis(timestamp - t));
                t = getTimestamp();
            }
            return t;
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public static final class Worker implements IntSupplier, LongSupplier {
        public static final Worker ZERO = new Worker(0);

        private final int id;

        public Worker(int id) {
            this.id = id;
        }

        @Override
        public int getAsInt() {
            return id;
        }

        @Override
        public long getAsLong() {
            return id;
        }
    }
}
