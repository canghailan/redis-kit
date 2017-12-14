package cc.whohow.redis.client;

import org.redisson.client.RedisConnection;

import java.io.Closeable;

public interface RedisPooledConnection extends Closeable {
    RedisConnection getConnection();

    @Override
    void close();
}
