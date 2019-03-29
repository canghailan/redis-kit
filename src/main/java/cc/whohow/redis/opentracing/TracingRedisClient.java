package cc.whohow.redis.opentracing;

import io.lettuce.core.ConnectionFuture;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.resource.ClientResources;
import io.opentracing.Tracer;
import io.opentracing.contrib.redis.lettuce.TracingStatefulRedisConnection;

public class TracingRedisClient extends RedisClient {
    private final Tracer tracer;
    private boolean traceWithActiveSpanOnly = true;

    public TracingRedisClient(RedisURI redisURI, Tracer tracer) {
        this(null, redisURI, tracer);
    }

    public TracingRedisClient(ClientResources clientResources, RedisURI redisURI, Tracer tracer) {
        super(clientResources, redisURI);
        this.tracer = tracer;
    }

    @Override
    public StatefulRedisConnection<String, String> connect() {
        return new TracingStatefulRedisConnection<>(super.connect(), tracer, traceWithActiveSpanOnly);
    }

    @Override
    public <K, V> StatefulRedisConnection<K, V> connect(RedisCodec<K, V> codec) {
        return new TracingStatefulRedisConnection<>(super.connect(codec), tracer, traceWithActiveSpanOnly);
    }

    @Override
    public StatefulRedisConnection<String, String> connect(RedisURI redisURI) {
        return new TracingStatefulRedisConnection<>(super.connect(redisURI), tracer, traceWithActiveSpanOnly);
    }

    @Override
    public <K, V> StatefulRedisConnection<K, V> connect(RedisCodec<K, V> codec, RedisURI redisURI) {
        return new TracingStatefulRedisConnection<>(super.connect(codec, redisURI), tracer, traceWithActiveSpanOnly);
    }

    @Override
    public <K, V> ConnectionFuture<StatefulRedisConnection<K, V>> connectAsync(RedisCodec<K, V> codec, RedisURI redisURI) {
        return super.connectAsync(codec, redisURI).thenApply(c -> new TracingStatefulRedisConnection<>(c, tracer, traceWithActiveSpanOnly));
    }
}
