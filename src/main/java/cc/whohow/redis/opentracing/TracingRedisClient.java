package cc.whohow.redis.opentracing;

import io.lettuce.core.ConnectionFuture;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.lettuce.TracingStatefulRedisConnection;
import io.opentracing.contrib.redis.lettuce.TracingStatefulRedisPubSubConnection;

/**
 * OpenTracing支持
 */
public class TracingRedisClient extends RedisClient {
    protected final TracingConfiguration tracingConfiguration;

    public TracingRedisClient(RedisURI redisURI, TracingConfiguration tracingConfiguration) {
        this(null, redisURI, tracingConfiguration);
    }

    public TracingRedisClient(ClientResources clientResources, RedisURI redisURI, TracingConfiguration tracingConfiguration) {
        super(clientResources, redisURI);
        this.tracingConfiguration = tracingConfiguration;
    }

    @Override
    public StatefulRedisConnection<String, String> connect() {
        return new TracingStatefulRedisConnection<>(super.connect(), tracingConfiguration);
    }

    @Override
    public <K, V> StatefulRedisConnection<K, V> connect(RedisCodec<K, V> codec) {
        return new TracingStatefulRedisConnection<>(super.connect(codec), tracingConfiguration);
    }

    @Override
    public StatefulRedisConnection<String, String> connect(RedisURI redisURI) {
        return new TracingStatefulRedisConnection<>(super.connect(redisURI), tracingConfiguration);
    }

    @Override
    public <K, V> StatefulRedisConnection<K, V> connect(RedisCodec<K, V> codec, RedisURI redisURI) {
        return new TracingStatefulRedisConnection<>(super.connect(codec, redisURI), tracingConfiguration);
    }

    @Override
    public <K, V> ConnectionFuture<StatefulRedisConnection<K, V>> connectAsync(RedisCodec<K, V> codec, RedisURI redisURI) {
        return super.connectAsync(codec, redisURI).thenApply(c -> new TracingStatefulRedisConnection<>(c, tracingConfiguration));
    }

    @Override
    public StatefulRedisPubSubConnection<String, String> connectPubSub() {
        return new TracingStatefulRedisPubSubConnection<>(super.connectPubSub(), tracingConfiguration);
    }

    @Override
    public StatefulRedisPubSubConnection<String, String> connectPubSub(RedisURI redisURI) {
        return new TracingStatefulRedisPubSubConnection<>(super.connectPubSub(redisURI), tracingConfiguration);
    }

    @Override
    public <K, V> StatefulRedisPubSubConnection<K, V> connectPubSub(RedisCodec<K, V> codec) {
        return new TracingStatefulRedisPubSubConnection<>(super.connectPubSub(codec), tracingConfiguration);
    }

    @Override
    public <K, V> StatefulRedisPubSubConnection<K, V> connectPubSub(RedisCodec<K, V> codec, RedisURI redisURI) {
        return new TracingStatefulRedisPubSubConnection<>(super.connectPubSub(codec, redisURI), tracingConfiguration);
    }

    @Override
    public <K, V> ConnectionFuture<StatefulRedisPubSubConnection<K, V>> connectPubSubAsync(RedisCodec<K, V> codec, RedisURI redisURI) {
        return super.connectPubSubAsync(codec, redisURI).thenApply(c -> new TracingStatefulRedisPubSubConnection<>(c, tracingConfiguration));
    }
}
