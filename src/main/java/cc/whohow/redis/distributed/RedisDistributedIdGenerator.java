package cc.whohow.redis.distributed;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

public class RedisDistributedIdGenerator implements LongSupplier {
    private static final long MAX_TIME_DELTA = TimeUnit.SECONDS.toMillis(1);
    private final RedisDistributed redisDistributed;
    private volatile long timestamp;
    private volatile long shardId;
    private volatile long sequence;

    public RedisDistributedIdGenerator(RedisDistributed redisDistributed) {
        this.redisDistributed = redisDistributed;
        this.timestamp = getTime();
        this.shardId = -1;
        this.sequence = 0;
    }

    /**
     * 2015-05-01 08:30:00+08
     */
    public static long snowflakeEpoch(long timestamp, long shardId, long sequence) {
        return snowflake(timestamp - 1430440200000L, shardId, sequence);
    }

    /**
     * 2000-01-01 08:00:00+08
     */
    public static long snowflake2000(long timestamp, long shardId, long sequence) {
        return snowflake(timestamp - 946684800000L, shardId, sequence);
    }

    /**
     * 1970-01-01 08:00:00+08
     */
    public static long snowflake(long timestamp, long shardId, long sequence) {
        if (timestamp < 0 || timestamp >= (2L << 41)) {
            throw new IllegalArgumentException();
        }
        if (shardId < 0 || shardId >= (2L << 10)) {
            throw new IllegalArgumentException();
        }
        if (sequence < 0 || sequence >= (2L << 12)) {
            throw new IllegalArgumentException();
        }
        // snowflake-64bit: [0][41bit timestamp][10bit machine id][12bit sequence]
        return (timestamp << 10 << 12) | (shardId << 12) | sequence;
    }

    protected long generate(long timestamp, long shardId, long sequence) {
        return snowflakeEpoch(timestamp, shardId, sequence);
    }

    @Override
    public long getAsLong() {
        long currentShardId = getShardId();
        long currentTimestamp = getTime();
        synchronized (this) {
            while (currentTimestamp < timestamp) {
                long delta = timestamp - currentTimestamp;
                if (delta > MAX_TIME_DELTA) {
                    throw new IllegalStateException();
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(delta));
                currentTimestamp = getTime();
            }
            if (currentTimestamp == timestamp && currentShardId == shardId) {
                sequence++;
                return generate(currentTimestamp, currentShardId, sequence);
            } else {
                timestamp = currentTimestamp;
                shardId = currentShardId;
                sequence = 0;
                return generate(currentTimestamp, currentShardId, sequence);
            }
        }
    }

    private long getTime() {
        return TimeUnit.MICROSECONDS.toMillis(redisDistributed.time());
    }

    private long getShardId() {
        return redisDistributed.getId();
    }
}
