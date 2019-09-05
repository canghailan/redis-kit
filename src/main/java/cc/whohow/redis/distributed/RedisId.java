package cc.whohow.redis.distributed;

/**
 * Snowflake算法，使用Redis提供时间及Worker ID
 */
public class RedisId {
    public static SnowflakeId i64(RedisDistributed redisDistributed) {
        return new SnowflakeId(redisDistributed.clock(), redisDistributed::getId);
    }

    public static SnowflakeId i52(RedisDistributed redisDistributed) {
        return new SnowflakeId52(redisDistributed.clock(), redisDistributed::getId);
    }
}
