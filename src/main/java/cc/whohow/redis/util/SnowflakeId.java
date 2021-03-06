package cc.whohow.redis.util;

import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Snowflake算法
 */
public class SnowflakeId implements LongSupplier {
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
        this(Clock.systemDefaultZone(), new AtomicLong(0)::get);
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

    public long random(long timestamp) {
        return snowflake(
                timeUnit.convert(timestamp, TimeUnit.MILLISECONDS) - epoch,
                workerId,
                ThreadLocalRandom.current().nextLong(sequenceMask >> 1, sequenceMask));
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

    public Date extractDate(long id) {
        return new Date(timeUnit.toMillis(extractTimestamp(id)));
    }

    public long extractTimestamp(long id) {
        return ((id >> timestampShift) & timestampMask) + epoch;
    }

    public long extractWorkerId(long id) {
        return (id >> workerIdShift) & workerIdMask;
    }

    public long extractSequence(long id) {
        return id & sequenceMask;
    }

    public static class I52 extends SnowflakeId {
        public I52() {
            this(Clock.systemDefaultZone(), new AtomicLong(0)::get);
        }

        public I52(Clock clock, LongSupplier worker) {
            this(clock, worker, Y2K);
        }

        public I52(Clock clock, LongSupplier worker, Instant epoch) {
            super(clock, worker, epoch, TimeUnit.SECONDS, 36L, 6L, 10L);
        }
    }
}
