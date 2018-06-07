package cc.whohow.redis;

import cc.whohow.redis.codec.Codec;
import org.redisson.api.RFuture;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommand;

import java.util.List;

public interface RedisPipeline {
    <T> RFuture<T> execute(RedisCommand<T> command, Object... params);

    <T, R> RFuture<R> execute(Codec codec, RedisCommand<T> command, Object... params);

    List<?> flush();
}
