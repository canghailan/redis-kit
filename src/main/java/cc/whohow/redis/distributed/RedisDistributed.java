package cc.whohow.redis.distributed;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.lettuce.Lettuce;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Redis分布式注册中心
 */
public class RedisDistributed implements
        Runnable,
        Closeable,
        RedisConnectionStateListener {
    protected static final ByteBuffer KEY = ByteBuffers.fromUtf8("RedisDistributed");
    protected static final ByteBuffer KEY_PREFIX = ByteBuffers.fromUtf8("RedisDistributed:");
    protected static final ByteBuffer KEY_PATTERN = ByteBuffers.fromUtf8("RedisDistributed:*");
    protected static final long MAX_DELAY = TimeUnit.MINUTES.toMicros(3L);
    protected final RedisClient redisClient;
    protected final RedisURI redisURI;
    protected final StatefulRedisConnection<ByteBuffer, ByteBuffer> redisConnection;
    protected final StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> redisPubSubConnection;
    protected final long size;
    protected volatile CompletableFuture<Long> id;

    public RedisDistributed(RedisClient redisClient, RedisURI redisURI, long size) {
        this.redisClient = redisClient;
        this.redisURI = redisURI;
        this.size = size;
        this.redisConnection = redisClient.connect(ByteBufferCodec.INSTANCE, redisURI);
        this.redisPubSubConnection = redisClient.connectPubSub(ByteBufferCodec.INSTANCE, redisURI);
    }

    public StatefulRedisConnection<ByteBuffer, ByteBuffer> getRedisConnection() {
        return redisConnection;
    }

    public StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> getRedisPubSubConnection() {
        return redisPubSubConnection;
    }

    /**
     * Redis服务器时间（Micros）
     */
    public long time() {
        List<ByteBuffer> time = redisConnection.sync().time();
        long seconds = PrimitiveCodec.LONG.decode(time.get(0));
        long microseconds = PrimitiveCodec.LONG.decode(time.get(1));
        return TimeUnit.SECONDS.toMicros(seconds) + microseconds;
    }

    /**
     * 获取当前节点ID
     */
    public long getId() {
        return id.join();
    }

    /**
     * 异步获取当前节点ID
     */
    public CompletableFuture<Long> getIdAsync() {
        return id;
    }

    /**
     * 获取在线节点集合
     */
    public Set<Long> getNodeIdSet() {
        return redisConnection.sync()
                .pubsubChannels(KEY_PATTERN.duplicate()).stream()
                .map(self -> ByteBuffers.slice(self, KEY_PREFIX.remaining()))
                .map(this::decodeId)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * 获取已注册节点集合
     */
    public List<Long> getRegisteredNodeIdList() {
        return redisConnection.sync()
                .zrange(KEY.duplicate(), 0, -1).stream()
                .map(this::decodeId)
                .collect(Collectors.toList());
    }

    /**
     * 获取领导者ID
     */
    public Long getLeaderId() {
        List<Long> registeredIdList = getRegisteredNodeIdList();
        Set<Long> idSet = getNodeIdSet();
        return registeredIdList.stream()
                .filter(idSet::contains)
                .findFirst()
                .orElse(null);
    }

    /**
     * 启动
     */
    @Override
    @SuppressWarnings("unchecked")
    public synchronized void run() {
        resetId();
        while (true) {
            long time = time();
            long newId = nextId();
            if (redisConnection.sync().zadd(
                    KEY.duplicate(),
                    Lettuce.Z_ADD_NX,
                    ScoredValue.fromNullable(time, encodeId(newId))) > 0) {
                redisPubSubConnection.sync().subscribe(getNodeKey(newId));
                id.complete(newId);
                break;
            }
        }
    }

    /**
     * 关闭
     */
    @Override
    public void close() {
        try {
            if (id != null) {
                id.cancel(true);
                id = null;
            }
        } finally {
            close(redisConnection);
            close(redisPubSubConnection);
        }
    }

    /**
     * 回收垃圾数据
     */
    public long gc() {
        long time = time();
        Set<Long> garbage = redisConnection.sync()
                .zrangebyscore(KEY.duplicate(), Range.create(0L, time - MAX_DELAY))
                .stream()
                .map(this::decodeId)
                .collect(Collectors.toSet());
        garbage.removeAll(getNodeIdSet());
        if (garbage.isEmpty()) {
            return 0;
        }
        return redisConnection.sync()
                .zrem(KEY.duplicate(), garbage.stream().map(this::encodeId).toArray(ByteBuffer[]::new));
    }

    @Override
    public void onRedisConnected(RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
        run();
    }

    @Override
    public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
        resetId();
    }

    @Override
    public void onRedisExceptionCaught(RedisChannelHandler<?, ?> connection, Throwable cause) {
    }

    /**
     * 重置ID
     */
    protected synchronized void resetId() {
        if (id != null) {
            id.cancel(true);
        }
        id = new CompletableFuture<>();
    }

    protected long nextId() {
        List<Long> idSet = getRegisteredNodeIdList();
        for (long i = 0; i < size; i++) {
            if (!idSet.contains(i)) {
                return i;
            }
        }
        throw new IllegalStateException();
    }

    /**
     * 获取节点Redis Key
     */
    public ByteBuffer getNodeKey(Long id) {
        return ByteBuffers.concat(KEY_PREFIX.duplicate(), encodeId(id));
    }

    private ByteBuffer encodeId(Long id) {
        return PrimitiveCodec.LONG.encode(id);
    }

    private Long decodeId(ByteBuffer byteBuffer) {
        return PrimitiveCodec.LONG.decode(byteBuffer);
    }

    private void close(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception ignore) {
        }
    }
}
