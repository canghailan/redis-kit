package cc.whohow.redis.distributed;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Snowflake算法，使用Redis提供时间及Worker ID
 */
public class RedisSnowflakeIdGenerator extends SnowflakeIdGenerator {
    protected final RedisDistributed redisDistributed;

    public RedisSnowflakeIdGenerator(RedisDistributed redisDistributed) {
        super();
        this.redisDistributed = redisDistributed;
    }

    public RedisSnowflakeIdGenerator(RedisDistributed redisDistributed, long epoch) {
        super(epoch);
        this.redisDistributed = redisDistributed;
    }

    public RedisSnowflakeIdGenerator(RedisDistributed redisDistributed,
                                     TimeUnit timeUnit,
                                     long timestampBits, long workerIdBits, long sequenceBits,
                                     long epoch) {
        super(timeUnit, timestampBits, workerIdBits, sequenceBits, epoch);
        this.redisDistributed = redisDistributed;
    }

    protected long getTime() {
        return redisDistributed.clock().millis();
    }

    protected long getWorkerId() {
        return redisDistributed.getId();
    }
}
