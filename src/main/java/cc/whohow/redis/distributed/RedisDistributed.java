package cc.whohow.redis.distributed;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.util.RedisClock;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.List;
import java.util.Map;
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
    protected static final ByteBuffer KEY = ByteBuffers.fromUtf8("_rd_");
    protected static final ByteBuffer KEY_PREFIX = ByteBuffers.fromUtf8("_rd_:");
    protected static final ByteBuffer KEY_PATTERN = ByteBuffers.fromUtf8("_rd_:*");
    protected static final int MAX_ID = 65535;
    protected static final long MAX_DELAY_MS = TimeUnit.MINUTES.toMillis(3L);
    protected final RedisClient redisClient;
    protected final RedisURI redisURI;
    protected final StatefulRedisConnection<ByteBuffer, ByteBuffer> redisConnection;
    protected final StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> redisPubSubConnection;
    protected final RedisClock clock;
    protected volatile CompletableFuture<Long> id;

    public RedisDistributed(RedisClient redisClient, RedisURI redisURI) {
        this.redisClient = redisClient;
        this.redisURI = redisURI;
        this.redisConnection = redisClient.connect(ByteBufferCodec.INSTANCE, redisURI);
        this.redisPubSubConnection = redisClient.connectPubSub(ByteBufferCodec.INSTANCE, redisURI);
        this.clock = RedisClock.create(redisConnection);
        this.id = new CompletableFuture<>();
    }

    public StatefulRedisConnection<ByteBuffer, ByteBuffer> getRedisConnection() {
        return redisConnection;
    }

    public StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> getRedisPubSubConnection() {
        return redisPubSubConnection;
    }

    /**
     * 分布式时钟
     */
    public Clock clock() {
        return clock;
    }

    /**
     * 获取当前节点ID
     */
    public long getId() {
        return id.join();
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
     * 关闭
     */
    @Override
    public void close() {
        try {
            id = new CompletableFuture<>();
        } finally {
            close(redisConnection);
            close(redisConnection);
        }
    }

    /**
     * 回收垃圾数据
     */
    public long gc() {
        long time = clock.millis();
        Set<Long> garbage = redisConnection.sync()
                .zrangebyscore(KEY.duplicate(), Range.create(0L, time - MAX_DELAY_MS))
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
        this.id = new CompletableFuture<>();
    }

    @Override
    public void onRedisExceptionCaught(RedisChannelHandler<?, ?> connection, Throwable cause) {
    }

    protected long nextId() {
        List<Long> idSet = getRegisteredNodeIdList();
        for (long i = 0; i < MAX_ID; i++) {
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

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        while (!id.isDone()) {
            try {
                long time = clock.millis();
                long newId = nextId();
                if (redisConnection.sync().zadd(
                        KEY.duplicate(),
                        Lettuce.Z_ADD_NX,
                        ScoredValue.just(time, encodeId(newId))) > 0) {
                    redisPubSubConnection.sync().subscribe(getNodeKey(newId));
                    if (!id.isDone()) {
                        id.complete(newId);
                        onIdComplete();
                    }
                    break;
                }
            } catch (Throwable ignore) {
            }
        }
    }

    protected void onIdComplete() {
        ByteBuffer nodeKey = getNodeKey(getId());
        Map<ByteBuffer, ByteBuffer> systemInfo = new SystemInfo().get().entrySet().stream()
                .collect(Collectors.toMap(
                        e -> ByteBuffers.fromUtf8(e.getKey()),
                        e -> ByteBuffers.fromUtf8(e.getValue())));

        RedisCommands<ByteBuffer, ByteBuffer> redis = redisConnection.sync();
        redis.multi();
        redis.clientSetname(nodeKey.duplicate());
        redis.del(nodeKey.duplicate());
        redis.hmset(nodeKey.duplicate(), systemInfo);
        redis.exec();
    }
}
