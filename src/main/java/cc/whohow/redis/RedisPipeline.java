package cc.whohow.redis;

import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommand;

public interface RedisPipeline {
    Redis getRedis();

    void pipe(RedisCommand<?> command, Object... params);

    void pipe(Codec codec, RedisCommand<?> command, Object... params);

    void execute();
}
