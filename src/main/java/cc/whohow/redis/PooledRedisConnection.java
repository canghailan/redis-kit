package cc.whohow.redis;

import org.redisson.client.RedisConnection;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommand;

import java.io.Closeable;

public interface PooledRedisConnection extends Closeable {
    RedisConnection get();

    <T> T execute(RedisCommand<T> command, Object... params);

    <T, R> R execute(Codec codec, RedisCommand<T> command, Object... params);

    @Override
    void close();
}
