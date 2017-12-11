package cc.whohow.redis;

import org.redisson.client.RedisConnection;

import java.io.Closeable;

public interface Redis extends Closeable {
    RedisConnection getConnection();

    PooledRedisConnection getPooledConnection();
}
