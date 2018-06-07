package cc.whohow.redis;

import cc.whohow.redis.codec.Codec;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.connection.PubSubConnectionEntry;

import java.io.Closeable;
import java.net.URI;

public interface Redis extends Closeable {
    URI getUri();

    RedisPipeline pipeline();

    <T> T execute(RedisCommand<T> command, Object... params);

    <T, R> R execute(Codec codec, RedisCommand<T> command, Object... params);

    PubSubConnectionEntry subscribe(String name, Codec codec, RedisPubSubListener<?>... listeners);

    PubSubConnectionEntry psubscribe(String pattern, Codec codec, RedisPubSubListener<?>... listeners);

    void unsubscribe(String name);

    void punsubscribe(String pattern);
}
