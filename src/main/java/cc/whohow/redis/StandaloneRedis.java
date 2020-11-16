package cc.whohow.redis;

import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.lettuce.ByteSequenceRedisCodec;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.RedisCommand;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

public class StandaloneRedis implements Redis {
    protected final RedisClient client;
    protected final RedisURI uri;
    protected volatile StatefulConnection<ByteSequence, ByteSequence> connection;

    public StandaloneRedis(RedisClient client, RedisURI uri) {
        this.client = client;
        this.uri = uri;
        this.connection = client.connect(ByteSequenceRedisCodec.get(), uri);
    }

    @Override
    public <T> CompletableFuture<T> sendAsync(RedisCommand<ByteSequence, ByteSequence, T> command) {
        return (AsyncCommand<ByteSequence, ByteSequence, T>) connection.dispatch(new AsyncCommand<>(command));
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    @Override
    public void addListener(RedisConnectionStateListener listener) {
        client.addListener(listener);
    }

    @Override
    public void removeListener(RedisConnectionStateListener listener) {
        client.removeListener(listener);
    }

    @Override
    public URI getURI() {
        return uri.toURI();
    }
}
