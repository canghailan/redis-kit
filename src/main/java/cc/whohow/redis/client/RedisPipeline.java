package cc.whohow.redis.client;

import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.misc.RPromise;

public interface RedisPipeline {
    <T> RPromise<T> execute(RedisCommand<T> command, Object... params);

    <T, R> RPromise<R> execute(Codec codec, RedisCommand<T> command, Object... params);

    void sync();
}
