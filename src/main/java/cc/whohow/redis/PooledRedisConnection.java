package cc.whohow.redis;

import org.redisson.client.RedisConnection;

import java.io.Closeable;

public interface PooledRedisConnection extends Closeable {
    RedisConnection get();

    @Override
    void close();
}
