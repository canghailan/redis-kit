package cc.whohow.redis.distributed;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.script.RedisScriptCommands;
import cc.whohow.redis.util.RedisClock;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
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
    protected static final ByteBuffer GC_LOCK = ByteBuffers.fromUtf8("_rd_gc_");
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    protected final int maxId = 65535;
    protected final Duration timeout = Duration.ofSeconds(30);
    protected final RedisClient redisClient;
    protected final RedisURI redisURI;
    protected final StatefulRedisConnection<ByteBuffer, ByteBuffer> redisConnection;
    protected final StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> redisPubSubConnection;
    protected final RedisClock clock;
    protected final ByteBuffer random;
    protected volatile long time;
    protected volatile CompletableFuture<Long> id;

    public RedisDistributed(RedisClient redisClient, RedisURI redisURI) {
        this.redisClient = redisClient;
        this.redisURI = redisURI;
        this.redisConnection = redisClient.connect(ByteBufferCodec.INSTANCE, redisURI);
        this.redisPubSubConnection = redisClient.connectPubSub(ByteBufferCodec.INSTANCE, redisURI);
        this.clock = new RedisClock(redisConnection.sync());
        this.random = ByteBuffers.fromUtf8(UUID.randomUUID().toString());
        this.id = new CompletableFuture<>();
        this.executor.scheduleAtFixedRate(this, 0, timeout.toMillis() / 3, TimeUnit.MILLISECONDS);
        this.executor.scheduleWithFixedDelay(this::gc, 0, timeout.toMillis() / 2, TimeUnit.MILLISECONDS);
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
    public Collection<Long> getNodeIdSet() {
        return redisConnection.sync().zrange(KEY.duplicate(), 0, -1).stream()
                .map(this::decodeLong)
                .collect(Collectors.toList());
    }

    /**
     * 获取领导者ID
     */
    public Long getLeaderId() {
        return redisConnection.sync().zrange(KEY.duplicate(), 0, 0).stream()
                .findFirst()
                .map(this::decodeLong)
                .orElse(null);
    }

    /**
     * 获取领导者ID
     */
    public boolean isLeader() {
        if (id.isDone()) {
            return Objects.equals(getId(), getLeaderId());
        }
        return false;
    }

    /**
     * 关闭
     */
    @Override
    public void close() {
        try {
            synchronized (this) {
                if (id.isDone()) {
                    id = new CompletableFuture<>();
                }
                id.cancel(true);
            }
        } finally {
            close(redisConnection);
            close(redisPubSubConnection);
            close(executor);
        }
    }

    @Override
    public void onRedisConnected(RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
        run();
    }

    @Override
    public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
        synchronized (this) {
            if (id.isDone()) {
                id = new CompletableFuture<>();
            }
            id.cancel(true);
        }
    }

    @Override
    public void onRedisExceptionCaught(RedisChannelHandler<?, ?> connection, Throwable cause) {
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void run() {
        redisConnection.sync().clientSetname(random.duplicate());
        RedisScriptCommands redisScript = new RedisScriptCommands(redisConnection.sync());
        while (true) {
            try {
                long t;
                long i;
                if (id.isDone()) {
                    // 如果已注册，心跳保活
                    t = time;
                    i = id.join();
                } else {
                    // 如果未注册，加入集群
                    t = clock.millis();
                    i = nextId();
                }
                ByteBuffer nodeKey = getNodeKey(i);
                boolean ok = redisScript.eval("rd", ScriptOutputType.BOOLEAN,
                        new ByteBuffer[]{KEY.duplicate(), nodeKey.duplicate()},
                        encode(i),
                        random.duplicate(),
                        encode(t),
                        encode(timeout.toMillis()));
                if (ok) {
                    if (!id.isDone()) {
                        // 加入集群，更新ID
                        time = t;
                        id.complete(i);
                        // 异步更新节点信息
                        id.thenRunAsync(this::onNodeChange, executor);
                    }
                    break;
                } else {
                    if (id.isDone()) {
                        // 更新失败，重置节点
                        id = new CompletableFuture<>();
                    }
                }
            } catch (Throwable ignore) {
            }
        }
    }

    protected long nextId() {
        Collection<Long> idSet = getNodeIdSet();
        for (long i = 0; i < maxId; i++) {
            if (!idSet.contains(i)) {
                return i;
            }
        }
        throw new IllegalStateException();
    }

    protected void onNodeChange() {
        set(new SystemInfo().get());
    }

    public void set(Map<String, String> data) {
        ByteBuffer nodeKey = getNodeKey(getId());
        Map<ByteBuffer, ByteBuffer> encoded = data.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> encode(e.getKey()),
                        e -> encode(e.getValue())));

        RedisCommands<ByteBuffer, ByteBuffer> redis = redisConnection.sync();
        redis.hmset(nodeKey.duplicate(), encoded);
        redis.pexpire(nodeKey.duplicate(), timeout.toMillis());
    }

    /**
     * 回收垃圾数据
     */
    public void gc() {
        RedisCommands<ByteBuffer, ByteBuffer> redis = redisConnection.sync();
        if (!Lettuce.ok(redis.set(GC_LOCK.duplicate(), random.duplicate(),
                new SetArgs().px(timeout.toMillis() / 2).nx()))) {
            return;
        }

        Collection<Long> idSet = getNodeIdSet();
        if (idSet.isEmpty()) {
            return;
        }
        List<ByteBuffer> keys = new ArrayList<>(idSet.size() + 1);
        List<ByteBuffer> argv = new ArrayList<>(idSet.size());
        for (Long id : idSet) {
            keys.add(getNodeKey(id));
            argv.add(encode(id));
        }
        keys.add(KEY.duplicate());

        new RedisScriptCommands(redis).eval("rd-gc", ScriptOutputType.VALUE,
                keys.toArray(new ByteBuffer[0]), argv.toArray(new ByteBuffer[0]));
    }

    /**
     * 获取节点Redis Key
     */
    public ByteBuffer getNodeKey(Long id) {
        return getNodeKey(encode(id));
    }

    private ByteBuffer getNodeKey(ByteBuffer id) {
        return ByteBuffers.concat(KEY_PREFIX, id);
    }

    private Long decodeLong(ByteBuffer byteBuffer) {
        return PrimitiveCodec.LONG.decode(byteBuffer.duplicate());
    }

    protected ByteBuffer encode(CharSequence text) {
        return ByteBuffers.fromUtf8(text);
    }

    protected ByteBuffer encode(long number) {
        return PrimitiveCodec.LONG.encode(number);
    }

    private void close(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception ignore) {
        }
    }

    private void close(ExecutorService executor) {
        try {
            if (executor != null) {
                executor.shutdown();
                executor.awaitTermination(3, TimeUnit.SECONDS);
            }
        } catch (Exception ignore) {
        }
    }
}
