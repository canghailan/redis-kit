package cc.whohow.redis.distributed;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RedisDistributed implements
        Closeable,
        RedisConnectionStateListener,
        RedisPubSubListener<ByteBuffer, ByteBuffer> {
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected static final ByteBuffer KEY = ByteBuffers.fromUtf8("RedisDistributed");
    protected static final ByteBuffer KEY_PREFIX = ByteBuffers.fromUtf8("RedisDistributed:");
    protected static final ByteBuffer KEY_PATTERN = ByteBuffers.fromUtf8("RedisDistributed:*");
    protected static final ByteBuffer STAR = ByteBuffers.fromUtf8("*");
    protected final RedisClient redisClient;
    protected final RedisURI redisURI;
    protected final StatefulRedisConnection<ByteBuffer, ByteBuffer> redisConnection;
    protected final StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> redisPubSubConnection;
    protected final ByteBuffer keyspace;
    protected volatile CompletableFuture<String> uuid;

    public RedisDistributed(RedisClient redisClient, RedisURI redisURI) {
        this.redisClient = redisClient;
        this.redisURI = redisURI;
        this.redisConnection = redisClient.connect(ByteBufferCodec.INSTANCE, redisURI);
        this.redisPubSubConnection = redisClient.connectPubSub(ByteBufferCodec.INSTANCE, redisURI);
        this.redisPubSubConnection.addListener(this);
        this.keyspace = ByteBuffers.fromUtf8("__keyspace@" + redisURI.getDatabase() + "__:");
        this.connect();
    }

    public String getUuid() {
        return uuid.join();
    }

    public Set<String> getNodeUuidSet() {
        return redisConnection.sync().pubsubChannels(KEY_PATTERN.duplicate()).stream()
            .map(self -> ByteBuffers.slice(self, KEY_PREFIX.remaining()))
            .map(ByteBuffers::toUtf8String)
            .collect(Collectors.toSet());
    }

    @Override
    public void close() throws IOException {
        redisConnection.close();
        redisPubSubConnection.close();
    }

    @Override
    public void onRedisConnected(RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
        System.out.println(socketAddress);
    }

    @Override
    public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
        disconnect();
    }

    @Override
    public void onRedisExceptionCaught(RedisChannelHandler<?, ?> connection, Throwable cause) {
        disconnect();
    }

    @Override
    public void message(ByteBuffer channel, ByteBuffer message) {
        System.out.println(ByteBuffers.toUtf8String(channel));
        System.out.println(ByteBuffers.toUtf8String(message));
    }

    @Override
    public void message(ByteBuffer pattern, ByteBuffer channel, ByteBuffer message) {
        System.out.println(ByteBuffers.toUtf8String(channel));
        System.out.println(ByteBuffers.toUtf8String(message));
    }

    @Override
    public void subscribed(ByteBuffer channel, long count) {

    }

    @Override
    public void psubscribed(ByteBuffer pattern, long count) {

    }

    @Override
    public void unsubscribed(ByteBuffer channel, long count) {

    }

    @Override
    public void punsubscribed(ByteBuffer pattern, long count) {

    }

    protected synchronized void connect() {
        cancel();
        uuid = new CompletableFuture<>();
        while (true) {
            String newUuid = UUID.randomUUID().toString();
            RedisDistributedNode node = new RedisDistributedNode(newUuid);
            if (redisConnection.sync().hsetnx(KEY.duplicate(),
                    ByteBuffers.fromUtf8(newUuid),
                    serialize(node))) {
                redisPubSubConnection.sync().psubscribe(
                        ByteBuffers.concat(keyspace.duplicate(), STAR.duplicate()));
                redisPubSubConnection.sync().subscribe(
                        KEY.duplicate(),
                        ByteBuffers.concat(KEY_PREFIX.duplicate(), ByteBuffers.fromUtf8(newUuid)));
                uuid.complete(newUuid);
                break;
            }
        }
    }

    protected synchronized void disconnect() {
        cancel();
        uuid = new CompletableFuture<>();
    }

    protected synchronized void cancel() {
        if (uuid == null || uuid.isDone()) {
            return;
        }
        uuid.cancel(true);
    }

    private <T> ByteBuffer serialize(T value) {
        try {
            return ByteBuffer.wrap(OBJECT_MAPPER.writeValueAsBytes(value));
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
