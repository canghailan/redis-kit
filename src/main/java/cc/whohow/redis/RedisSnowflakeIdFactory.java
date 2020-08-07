package cc.whohow.redis;

import cc.whohow.redis.util.RedisLocal;
import cc.whohow.redis.util.SnowflakeId;

public class RedisSnowflakeIdFactory implements RedisIdGeneratorFactory {
    private final RedisLocal redisLocal;

    public RedisSnowflakeIdFactory(RedisLocal redisLocal) {
        this.redisLocal = redisLocal;

    }

    @Override
    public SnowflakeId newI64Generator() {
        return new SnowflakeId(redisLocal.getClock(), redisLocal::getId);
    }

    @Override
    public SnowflakeId newI52Generator() {
        return new SnowflakeId.I52(redisLocal.getClock(), redisLocal::getId);
    }
}
