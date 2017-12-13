package cc.whohow.redis;

import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommand;

import java.io.Closeable;
import java.net.URI;

public interface Redis extends Closeable {
    URI getUri();

    <T> T execute(RedisCommand<T> command, Object... params);

    <T, R> R execute(Codec codec, RedisCommand<T> command, Object... params);
}
