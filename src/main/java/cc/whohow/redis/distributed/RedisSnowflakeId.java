package cc.whohow.redis.distributed;

/**
 * Snowflake算法，使用Redis提供时间及Worker ID
 */
public class RedisSnowflakeId extends SnowflakeId {
    public RedisSnowflakeId(RedisDistributed redisDistributed) {
        super(redisDistributed.clock(), redisDistributed::getId);
    }
}
